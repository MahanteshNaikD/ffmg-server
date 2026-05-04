require('dotenv').config();

const express = require('express');
const axios = require('axios');
const path = require('path');
const fs = require('fs');
const { spawn, spawnSync } = require('child_process');
const {
  createSessionGcsState,
  enqueueGcsSync,
  flushGcsSync,
  gcsPayloadForWebhook,
  isGcsEnabled,
  gcsConfig,
} = require('./gcsSync');

const app = express();
app.use(express.json());

const PORT = Number(process.env.PORT || 8080);
const SERVER_A_INTERNAL_URL = process.env.SERVER_A_INTERNAL_URL || 'http://localhost:3000';
const SERVER_A_API_PREFIX = process.env.SERVER_A_API_PREFIX || '/api/v1';
const HEARTBEAT_INTERVAL_MS = Number(process.env.HEARTBEAT_INTERVAL_MS || 2000);
const RTMP_INPUT_BASE = process.env.RTMP_INPUT_BASE || 'rtmp://localhost/live';
const HLS_ROOT = process.env.HLS_ROOT || path.join(process.cwd(), 'hls-output');
const STREAM_RETENTION_MS = Number(process.env.STREAM_RETENTION_MS || 30 * 60 * 1000); // 30 minutes default
const ARCHIVE_ROOT = path.join(HLS_ROOT, '.archives');

fs.mkdirSync(HLS_ROOT, { recursive: true });
fs.mkdirSync(ARCHIVE_ROOT, { recursive: true });

/**
 * Active stream processing sessions keyed by stream id.
 * This simulates FFmpeg lifecycle and emits heartbeats to Server A.
 */
const sessions = new Map();
let ffmpegAvailableCache = null;

function log(...args) {
  console.log('[worker]', ...args);
}

function logError(...args) {
  console.error('[worker]', ...args);
}

function withBase(pathname) {
  return `${SERVER_A_INTERNAL_URL.replace(/\/$/, '')}${pathname}`;
}

function withApiPrefix(pathname) {
  const prefix = SERVER_A_API_PREFIX.startsWith('/') ? SERVER_A_API_PREFIX : `/${SERVER_A_API_PREFIX}`;
  return `${SERVER_A_INTERNAL_URL.replace(/\/$/, '')}${prefix}${pathname}`;
}

async function postServerA(pathname, payload) {
  try {
    return await axios.post(withBase(pathname), payload);
  } catch (error) {
    if (error?.response?.status === 404) {
      return axios.post(withApiPrefix(pathname), payload);
    }
    throw error;
  }
}

async function sendHeartbeat(session) {
  try {
    void enqueueGcsSync(session);
    session.segmentsWritten = countSegments(session.outputDir);
    await postServerA('/internal/worker/heartbeat', {
      stream_id: session.streamId,
      segments_written: session.segmentsWritten,
      current_bitrate: session.currentBitrate,
      status: 'ok',
      // ...gcsPayloadForWebhook(session.streamId, session.gcsState),
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    console.error(`Heartbeat failed for stream ${session.streamId}: ${message}`);
  }
}

async function notifyStreamEnded(streamId, exitCode = 0, sessionForGcs = null) {
  try {
    await postServerA('/internal/worker/stream-ended', {
      stream_id: streamId,
      exit_code: exitCode,
      // ...gcsPayloadForWebhook(streamId, sessionForGcs?.gcsState ?? null),
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    console.error(`stream-ended webhook failed for stream ${streamId}: ${message}`);
  }
}

function outputDirForStream(streamId) {
  return path.join(HLS_ROOT, String(streamId));
}

function countSegments(outputDir) {
  try {
    const files = fs.readdirSync(outputDir);
    return files.filter((name) => name.endsWith('.ts') || name.endsWith('.m4s')).length;
  } catch {
    return 0;
  }
}

function archiveDir(outputDir) {
  try {
    const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
    const streamId = path.basename(outputDir);
    const archivePath = path.join(ARCHIVE_ROOT, `${streamId}_${timestamp}`);
    fs.renameSync(outputDir, archivePath);
    log('Stream archived', { streamId, archivePath });
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    logError('Failed to archive stream', { message });
  }
}

function cleanupOldArchives() {
  try {
    const now = Date.now();
    const files = fs.readdirSync(ARCHIVE_ROOT);
    for (const file of files) {
      const archivePath = path.join(ARCHIVE_ROOT, file);
      const stat = fs.statSync(archivePath);
      const age = now - stat.mtimeMs;
      if (age > STREAM_RETENTION_MS) {
        fs.rmSync(archivePath, { recursive: true, force: true });
        log('Old archive deleted', { file, ageMs: age });
      }
    }
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    logError('Cleanup failed', { message });
  }
}

function removeDir(outputDir) {
  fs.rmSync(outputDir, { recursive: true, force: true });
}

function buildInputUrl(streamKey) {
  return `${RTMP_INPUT_BASE.replace(/\/$/, '')}/${streamKey}`;
}

function buildFfmpegArgs(inputUrl, outputDir) {
  return [
    '-y',
    '-i',
    inputUrl,
    '-preset',
    'veryfast',
    '-profile:v',
    'main',
    '-sc_threshold',
    '0',
    '-g',
    '48',
    '-keyint_min',
    '48',
    '-map',
    '0:v:0',
    '-map',
    '0:a?',
    '-map',
    '0:v:0',
    '-map',
    '0:a?',
    '-c:v',
    'libx264',
    '-c:a',
    'aac',
    '-ar',
    '48000',
    '-b:v:0',
    '3000k',
    '-maxrate:v:0',
    '3210k',
    '-bufsize:v:0',
    '4500k',
    '-s:v:0',
    '1280x720',
    '-b:a:0',
    '128k',
    '-b:v:1',
    '1200k',
    '-maxrate:v:1',
    '1284k',
    '-bufsize:v:1',
    '1800k',
    '-s:v:1',
    '854x480',
    '-b:a:1',
    '96k',
    '-f',
    'hls',
    '-hls_time',
    '2',
    '-hls_list_size',
    '6',
    '-hls_flags',
    'delete_segments+independent_segments+append_list',
    '-master_pl_name',
    'master.m3u8',
    '-hls_segment_filename',
    path.join(outputDir, 'v%v_seg_%06d.ts'),
    '-var_stream_map',
    'v:0,a:0,name:720p v:1,a:1,name:480p',
    path.join(outputDir, 'v%v.m3u8'),
  ];
}

function isFfmpegAvailable() {
  if (ffmpegAvailableCache !== null) return ffmpegAvailableCache;
  const probe = spawnSync('ffmpeg', ['-version'], { stdio: 'ignore' });
  ffmpegAvailableCache = probe.status === 0;
  return ffmpegAvailableCache;
}

function startFfmpegSession(streamId, streamKey, outputDir) {
  const inputUrl = buildInputUrl(streamKey);
  const ffmpegArgs = buildFfmpegArgs(inputUrl, outputDir);
  const processRef = spawn('ffmpeg', ffmpegArgs, {
    stdio: ['ignore', 'ignore', 'pipe'],
  });

  processRef.stderr.on('data', (chunk) => {
    const line = String(chunk || '').trim();
    if (line) {
      console.log(`[ffmpeg:${streamId}] ${line}`);
    }
  });

  return processRef;
}

app.get('/health', (_req, res) => {
  res.status(200).json({
    ok: true,
    service: 'server-b-worker',
    active_streams: sessions.size,
  });
});

app.use('/hls', express.static(HLS_ROOT, { fallthrough: true }));

app.post('/transcode/start', async (req, res) => {
  const streamId = Number(req.body?.stream_id);
  const streamKey = String(req.body?.stream_key || '');

  log('Received start request', { streamId, streamKey });

  if (!streamId || !streamKey) {
    logError('Invalid start request payload', { body: req.body });
    return res.status(400).json({ error: 'stream_id and stream_key are required' });
  }

  if (!isFfmpegAvailable()) {
    logError('FFmpeg not available, cannot start stream', { streamId });
    return res.status(500).json({
      error: 'ffmpeg binary not found. Install ffmpeg or run Server B via docker image.',
    });
  }

  if (sessions.has(streamId)) {
    log('Start request ignored; stream already running', { streamId });
    return res.status(200).json({ success: true, stream_id: streamId, already_running: true });
  }

  const outputDir = outputDirForStream(streamId);
  log('Preparing output directory', { streamId, outputDir });
  cleanupOldArchives();
  if (fs.existsSync(outputDir)) {
    removeDir(outputDir);
  }
  fs.mkdirSync(outputDir, { recursive: true });
  log('Output directory ready', { streamId, outputDir });

  const ffmpegProcess = startFfmpegSession(streamId, streamKey, outputDir);
  log('FFmpeg process spawned', { streamId, streamKey });

  const session = {
    streamId,
    streamKey,
    outputDir,
    startedAt: new Date().toISOString(),
    segmentsWritten: 0,
    currentBitrate: 3200,
    interval: null,
    ffmpegProcess,
    gcsState: createSessionGcsState(streamId),
  };

  session.interval = setInterval(() => {
    void sendHeartbeat(session);
  }, HEARTBEAT_INTERVAL_MS);
  log('Heartbeat interval established', { streamId, intervalMs: HEARTBEAT_INTERVAL_MS });

  ffmpegProcess.on('exit', (code) => {
    const current = sessions.get(streamId);
    if (!current) return;
    clearInterval(current.interval);
    sessions.delete(streamId);
    log('FFmpeg exited', { streamId, code });
    void (async () => {
      try {
        await flushGcsSync(current);
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        logError('GCS sync on ffmpeg exit failed', { streamId, message });
      }
      archiveDir(current.outputDir);
      await notifyStreamEnded(streamId, Number(code || 0), current);
    })();
  });

  ffmpegProcess.on('error', (error) => {
    const current = sessions.get(streamId);
    if (!current) return;
    clearInterval(current.interval);
    sessions.delete(streamId);
    const message = error instanceof Error ? error.message : String(error);
    logError('FFmpeg failed during startup', { streamId, message });
    void (async () => {
      try {
        if (current.outputDir && fs.existsSync(current.outputDir)) {
          await flushGcsSync(current);
        }
      } catch (syncError) {
        const syncMessage = syncError instanceof Error ? syncError.message : String(syncError);
        logError('GCS sync after ffmpeg error failed', { streamId, message: syncMessage });
      }
      if (current.outputDir && fs.existsSync(current.outputDir)) {
        archiveDir(current.outputDir);
      }
      await notifyStreamEnded(streamId, 127, current);
    })();
  });

  sessions.set(streamId, session);
  log('Transcode session registered', { streamId, startedAt: session.startedAt });

  log('Sending first heartbeat for new session', { streamId });
  await sendHeartbeat(session);

  return res.status(200).json({
    success: true,
    stream_id: streamId,
    status: 'started',
  });
});

app.post('/transcode/stop', async (req, res) => {
  const streamId = Number(req.body?.stream_id);
  log('Received stop request', { streamId });

  if (!streamId) {
    logError('Invalid stop request payload', { body: req.body });
    return res.status(400).json({ error: 'stream_id is required' });
  }

  const session = sessions.get(streamId);
  if (!session) {
    log('No active session found for stream; sending stream-ended notification', { streamId });
    await notifyStreamEnded(streamId, 0);
    return res.status(200).json({ success: true, stream_id: streamId, status: 'already_stopped' });
  }

  log('Stopping active session', { streamId });
  clearInterval(session.interval);
  log('Heartbeat interval cleared', { streamId });
  try {
    if (!session.ffmpegProcess.killed) {
      log('Sending SIGTERM to FFmpeg process', { streamId });
      session.ffmpegProcess.kill('SIGTERM');
      setTimeout(() => {
        if (!session.ffmpegProcess.killed) {
          log('SIGTERM did not stop FFmpeg; sending SIGKILL', { streamId });
          session.ffmpegProcess.kill('SIGKILL');
        }
      }, 2000);
    }
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    logError('Failed to stop ffmpeg process', { streamId, message });
  }

  try {
    await flushGcsSync(session);
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    logError('GCS sync on transcode stop failed', { streamId, message });
  }

  sessions.delete(streamId);
  log('Transcode session stopped and removed', { streamId });
  archiveDir(session.outputDir);
  await notifyStreamEnded(streamId, 0, session);
  log('Stream ended notification sent', { streamId });

  return res.status(200).json({
    success: true,
    stream_id: streamId,
    status: 'stopped',
  });
});

app.listen(PORT, '0.0.0.0', () => {
  console.log(`Server B worker running on port ${PORT}`);
  if (isGcsEnabled()) {
    log('GCS archive enabled', gcsConfig());
  }
});
