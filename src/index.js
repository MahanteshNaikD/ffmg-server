const express = require('express');
const axios = require('axios');
const dotenv = require('dotenv');
const path = require('path');
const fs = require('fs');
const { spawn, spawnSync } = require('child_process');

dotenv.config();

const app = express();
app.use(express.json());

const PORT = Number(process.env.PORT || 8080);
const SERVER_A_INTERNAL_URL = process.env.SERVER_A_INTERNAL_URL || 'http://localhost:3000';
const SERVER_A_API_PREFIX = process.env.SERVER_A_API_PREFIX || '/api/v1';
const HEARTBEAT_INTERVAL_MS = Number(process.env.HEARTBEAT_INTERVAL_MS || 2000);
const RTMP_INPUT_BASE = process.env.RTMP_INPUT_BASE || 'rtmp://localhost/live';
const HLS_ROOT = process.env.HLS_ROOT || path.join(process.cwd(), 'hls-output');

fs.mkdirSync(HLS_ROOT, { recursive: true });

/**
 * Active stream processing sessions keyed by stream id.
 * This simulates FFmpeg lifecycle and emits heartbeats to Server A.
 */
const sessions = new Map();
let ffmpegAvailableCache = null;

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
    session.segmentsWritten = countSegments(session.outputDir);
    await postServerA('/internal/worker/heartbeat', {
      stream_id: session.streamId,
      segments_written: session.segmentsWritten,
      current_bitrate: session.currentBitrate,
      status: 'ok',
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    console.error(`Heartbeat failed for stream ${session.streamId}: ${message}`);
  }
}

async function notifyStreamEnded(streamId, exitCode = 0) {
  try {
    await postServerA('/internal/worker/stream-ended', {
      stream_id: streamId,
      exit_code: exitCode,
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

  if (!streamId || !streamKey) {
    return res.status(400).json({ error: 'stream_id and stream_key are required' });
  }

  if (!isFfmpegAvailable()) {
    return res.status(500).json({
      error: 'ffmpeg binary not found. Install ffmpeg or run Server B via docker image.',
    });
  }

  if (sessions.has(streamId)) {
    return res.status(200).json({ success: true, stream_id: streamId, already_running: true });
  }

  const outputDir = outputDirForStream(streamId);
  removeDir(outputDir);
  fs.mkdirSync(outputDir, { recursive: true });

  const ffmpegProcess = startFfmpegSession(streamId, streamKey, outputDir);

  const session = {
    streamId,
    streamKey,
    outputDir,
    startedAt: new Date().toISOString(),
    segmentsWritten: 0,
    currentBitrate: 3200,
    interval: null,
    ffmpegProcess,
  };

  session.interval = setInterval(() => {
    void sendHeartbeat(session);
  }, HEARTBEAT_INTERVAL_MS);

  ffmpegProcess.on('exit', (code) => {
    const current = sessions.get(streamId);
    if (!current) return;
    clearInterval(current.interval);
    sessions.delete(streamId);
    void notifyStreamEnded(streamId, Number(code || 0));
    console.log(`FFmpeg exited for stream ${streamId} with code ${code}`);
  });

  ffmpegProcess.on('error', (error) => {
    const current = sessions.get(streamId);
    if (!current) return;
    clearInterval(current.interval);
    sessions.delete(streamId);
    const message = error instanceof Error ? error.message : String(error);
    console.error(`FFmpeg failed for stream ${streamId}: ${message}`);
    void notifyStreamEnded(streamId, 127);
  });

  sessions.set(streamId, session);
  console.log(`Transcode session started for stream ${streamId}`);

  // Send first heartbeat quickly so stream setup can continue.
  await sendHeartbeat(session);

  return res.status(200).json({
    success: true,
    stream_id: streamId,
    status: 'started',
  });
});

app.post('/transcode/stop', async (req, res) => {
  const streamId = Number(req.body?.stream_id);
  if (!streamId) {
    return res.status(400).json({ error: 'stream_id is required' });
  }

  const session = sessions.get(streamId);
  if (!session) {
    await notifyStreamEnded(streamId, 0);
    return res.status(200).json({ success: true, stream_id: streamId, status: 'already_stopped' });
  }

  clearInterval(session.interval);
  try {
    if (!session.ffmpegProcess.killed) {
      session.ffmpegProcess.kill('SIGTERM');
      setTimeout(() => {
        if (!session.ffmpegProcess.killed) {
          session.ffmpegProcess.kill('SIGKILL');
        }
      }, 2000);
    }
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    console.error(`Failed to stop ffmpeg for stream ${streamId}: ${message}`);
  }
  sessions.delete(streamId);
  console.log(`Transcode session stopped for stream ${streamId}`);
  await notifyStreamEnded(streamId, 0);

  return res.status(200).json({
    success: true,
    stream_id: streamId,
    status: 'stopped',
  });
});

app.listen(PORT, '0.0.0.0', () => {
  console.log(`Server B worker running on port ${PORT}`);
});
