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
} = require('./gcsSync');

const app = express();
app.use(express.json());

const PORT = Number(process.env.PORT || 8080);
const HEARTBEAT_INTERVAL_MS = Number(process.env.HEARTBEAT_INTERVAL_MS || 2000);
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
/** One in-flight finalize per stream; concurrent callers await the same promise so notify always runs once. */
const finalizeInflight = new Map();
let ffmpegAvailableCache = null;

function formatError(error) {
  if (!error) return 'Unknown error';
  const url = error.config?.url || 'unknown url';
  const method = (error.config?.method || 'POST').toUpperCase();

  if (error.response) {
    return `Status ${error.response.status} (${error.response.statusText || 'Error'}) on ${method} ${url}: ${JSON.stringify(error.response.data)}`;
  }
  if (error.request) {
    const connInfo = error.code ? `Code: ${error.code}` : 'unknown';
    const address = error.address || (error.request?.socket?.remoteAddress) || 'unknown';
    const port = error.port || (error.request?.socket?.remotePort) || 'unknown';
    return `Connection failed on ${method} ${url}. Code: ${connInfo}, TargetAddress: ${address}, TargetPort: ${port}. Message: ${error.message}`;
  }
  return `${error.message || String(error)} on ${method} ${url}`;
}

function deriveEnvironment(callbackBaseUrl) {
  if (!callbackBaseUrl) return 'unknown';
  const url = callbackBaseUrl.toLowerCase();
  if (url.includes('dev') || url.includes('localhost') || url.includes('127.0.0.1') || url.includes('10.138.0.2')) {
    return 'development';
  }
  if (url.includes('prod') || url.includes('10.138.0.3')) {
    return 'production';
  }
  return 'production';
}

function sessionLogPrefix(session) {
  const env = deriveEnvironment(session.config.callbackBaseUrl);
  return `[streamId:${session.streamId}][env:${env}][bucket:${session.config.bucket || 'none'}][rtmp:${session.config.rtmpInputBase}][out:${session.outputDir}]`;
}

function logSession(session, message, ...args) {
  console.log(`${sessionLogPrefix(session)} ${message}`, ...args);
}

function logSessionError(session, message, ...args) {
  console.error(`${sessionLogPrefix(session)} ${message}`, ...args);
}

function withBase(pathname, callbackBaseUrl) {
  try {
    const url = new URL(callbackBaseUrl);
    return `${url.origin}${pathname}`;
  } catch {
    return `${callbackBaseUrl}${pathname}`;
  }
}

function withApiPrefix(pathname, callbackBaseUrl) {
  return `${callbackBaseUrl.replace(/\/+$/, '')}${pathname}`;
}

async function postServerA(pathname, payload, callbackBaseUrl) {
  try {
    return await axios.post(withApiPrefix(pathname, callbackBaseUrl), payload);
  } catch (error) {
    if (error?.response?.status === 404) {
      return axios.post(withBase(pathname, callbackBaseUrl), payload);
    }
    throw error;
  }
}

async function sendHeartbeat(session) {
  try {
    void enqueueGcsSync(session);
    session.segmentsWritten = countSegments(session.outputDir);
    logSession(session, `Sending heartbeat. segments=${session.segmentsWritten}`);
    await postServerA('/internal/worker/heartbeat', {
      stream_id: session.streamId,
      segments_written: session.segmentsWritten,
      current_bitrate: session.currentBitrate,
      status: 'ok',
    }, session.config.callbackBaseUrl);
  } catch (error) {
    logSessionError(session, `Heartbeat failed: ${formatError(error)}`);
  }
}

function liveUrlsForStream(session) {
  const gcs = gcsPayloadForWebhook(session.streamId, session.config.bucket, session.config.cdnUrl)?.gcs;
  if (gcs?.https_master_uri) {
    return {
      liveUrl: gcs.https_master_uri,
      thumbnailUrl: gcs.https_thumbnail_uri || null,
    };
  }
  const cdnBase = (session.config.cdnUrl || '').replace(/\/+$/, '');
  if (cdnBase) {
    return {
      liveUrl: `${cdnBase}/hls/${session.streamId}/master.m3u8`,
      thumbnailUrl: null,
    };
  }
  return {
    liveUrl: null,
    thumbnailUrl: null,
  };
}

async function notifyStreamStarted(session) {
  const { streamId, startedAt } = session;
  const { liveUrl, thumbnailUrl } = liveUrlsForStream(session);
  const payload = {
    stream_id: streamId,
    started_at: startedAt,
    status: 'live',
    live_url: liveUrl,
    thumbnail_url: thumbnailUrl
  };
  try {
    logSession(session, 'Sending stream-started notification');
    await postServerA('/internal/worker/stream-started', payload, session.config.callbackBaseUrl);
  } catch (error) {
    if (error?.response?.status === 404) {
      logSession(session, 'stream-started endpoint not found on Server A; skipping notify');
      return;
    }
    logSessionError(session, `stream-started webhook failed: ${formatError(error)}`);
  }
}

function streamEndedIncludeGcs() {
  return process.env.STREAM_ENDED_INCLUDE_GCS !== 'false';
}

async function notifyStreamEnded(streamId, exitCode = 0, callbackBaseUrl, bucket, cdnUrl) {
  const sid = Number(streamId);
  const code = Number(exitCode);
  if (!Number.isFinite(sid)) {
    console.error(`[worker][streamId:${streamId}] notifyStreamEnded skipped: invalid stream_id`);
    return;
  }
  const basePayload = {
    stream_id: sid,
    exit_code: Number.isFinite(code) ? code : 0,
  };
  const gcsPart = streamEndedIncludeGcs() ? gcsPayloadForWebhook(sid, bucket, cdnUrl) : {};
  const fullPayload = { ...basePayload, ...gcsPart };
  try {
    console.log(`[worker][streamId:${sid}][env:${deriveEnvironment(callbackBaseUrl)}] notifyStreamEnded: exitCode=${exitCode}`);
    await postServerA('/internal/worker/stream-ended', fullPayload, callbackBaseUrl);
  } catch (error) {
    if (error?.response?.status === 400 && streamEndedIncludeGcs() && Object.keys(gcsPart).length > 0) {
      console.error(`[worker][streamId:${sid}] stream-ended rejected payload with gcs; retrying base fields only. Details: ${formatError(error)}`);
      try {
        await postServerA('/internal/worker/stream-ended', basePayload, callbackBaseUrl);
        return;
      } catch (retryError) {
        console.error(`[worker][streamId:${sid}] stream-ended webhook retry failed: ${formatError(retryError)}`);
        return;
      }
    }
    console.error(`[worker][streamId:${sid}] stream-ended webhook failed: ${formatError(error)}`);
  }
}

/**
 * Flush GCS, archive output, notify backend once. Safe if /transcode/stop and ffmpeg exit race.
 */
async function finalizeTranscodeSession(streamId, session, exitCode) {
  if (!session) {
    return;
  }
  const existing = finalizeInflight.get(streamId);
  if (existing) {
    await existing;
    return;
  }

  const run = (async () => {
    try {
      try {
        clearInterval(session.interval);
      } catch {
        /* ignore */
      }
      sessions.delete(streamId);

      try {
        logSession(session, 'Flushing dynamic GCS uploader files...');
        await flushGcsSync(session);
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        logSessionError(session, `GCS sync on finalize failed: ${message}`);
      }

      try {
        if (session.outputDir && fs.existsSync(session.outputDir)) {
          archiveDir(session.outputDir, session);
        }
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        logSessionError(session, `Archive on finalize failed: ${message}`);
      }

      await notifyStreamEnded(
        streamId,
        exitCode,
        session.config.callbackBaseUrl,
        session.config.bucket,
        session.config.cdnUrl
      );
    } finally {
      finalizeInflight.delete(streamId);
    }
  })();

  finalizeInflight.set(streamId, run);
  await run;
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

function archiveDir(outputDir, session) {
  try {
    const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
    const streamId = path.basename(outputDir);
    const archivePath = path.join(ARCHIVE_ROOT, `${streamId}_${timestamp}`);
    fs.renameSync(outputDir, archivePath);
    logSession(session, `Stream directory archived to ${archivePath}`);
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    logSessionError(session, `Failed to archive stream directory: ${message}`);
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
        console.log('[worker] Old archive deleted', { file, ageMs: age });
      }
    }
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    console.error('[worker] Cleanup failed', { message });
  }
}

function removeDir(outputDir) {
  fs.rmSync(outputDir, { recursive: true, force: true });
}

function buildInputUrl(rtmpInputBase, streamKey) {
  return `${rtmpInputBase.replace(/\/$/, '')}/${streamKey}`;
}


function buildFfmpegArgs(inputUrl, outputDir) {
  return [
    '-y',

    '-i',
    inputUrl,

    // ─────────────────────────────
    // Video encoding
    // ─────────────────────────────
    '-preset',
    'veryfast',

    '-profile:v',
    'main',

    '-sc_threshold',
    '0',

    // 2-second GOP for 2-second HLS segments
    '-g',
    '48',

    '-keyint_min',
    '48',

    // ─────────────────────────────
    // 720p
    // ─────────────────────────────
    '-map',
    '0:v:0',

    '-map',
    '0:a?',

    // ─────────────────────────────
    // 480p
    // ─────────────────────────────
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

    // 720p
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

    // 480p
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

    // ─────────────────────────────
    // Low-latency HLS
    // ─────────────────────────────
    '-f',
    'hls',

    // 1-second segments instead of 2 seconds
    '-hls_time',
    '1',

    // Keep only 4 segments in playlist
    '-hls_list_size',
    '4',

    '-hls_flags',
    'delete_segments+independent_segments+append_list',

    '-master_pl_name',
    'master.m3u8',

    '-hls_segment_filename',
    path.join(
      outputDir,
      'v%v_seg_%06d.ts',
    ),

    '-var_stream_map',
    'v:0,a:0,name:720p v:1,a:1,name:480p',

    path.join(
      outputDir,
      'v%v.m3u8',
    ),
  ];
}


function isFfmpegAvailable() {
  if (ffmpegAvailableCache !== null) return ffmpegAvailableCache;
  const probe = spawnSync('ffmpeg', ['-version'], { stdio: 'ignore' });
  ffmpegAvailableCache = probe.status === 0;
  return ffmpegAvailableCache;
}

function startFfmpegSession(session) {
  const inputUrl = buildInputUrl(session.config.rtmpInputBase, session.streamKey);
  const ffmpegArgs = buildFfmpegArgs(inputUrl, session.outputDir);
  const processRef = spawn('ffmpeg', ffmpegArgs, {
    stdio: ['ignore', 'ignore', 'pipe'],
  });

  processRef.stderr.on('data', (chunk) => {
    const line = String(chunk || '').trim();
    if (line) {
      console.log(`${sessionLogPrefix(session)}[ffmpeg] ${line}`);
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
  const streamId = Number(req.body?.streamId || req.body?.stream_id);
  const streamKey = String(req.body?.streamKey || req.body?.stream_key || '');

  // Fallbacks: if parameters are not present in request body, fall back to environment variables or defaults
  const rtmpInputBase = String(
    req.body?.rtmpInputBase ||
    req.body?.rtmp_input_base ||
    process.env.RTMP_INPUT_BASE ||
    'rtmp://localhost/live'
  );
  const callbackBaseUrl = String(
    req.body?.callbackBaseUrl ||
    req.body?.callback_base_url ||
    `${process.env.SERVER_A_INTERNAL_URL || 'http://localhost:3000'}${process.env.SERVER_A_API_PREFIX || '/api/v1'}`
  );
  const bucket = String(
    req.body?.bucket ||
    req.body?.gcs_bucket ||
    process.env.GCS_BUCKET ||
    ''
  ).trim();
  const cdnUrl = String(
    req.body?.cdnUrl ||
    req.body?.cdn_url ||
    process.env.CDN_URL ||
    ''
  ).trim();

  // Mock a temporary session object for logging the incoming request validation status
  const tempSession = {
    streamId: streamId || 0,
    streamKey,
    outputDir: outputDirForStream(streamId || 0),
    config: { streamId, streamKey, rtmpInputBase, callbackBaseUrl, bucket, cdnUrl }
  };

  logSession(tempSession, 'Received start request');

  if (!streamId || !streamKey) {
    logSessionError(tempSession, 'Invalid start request payload: streamId and streamKey are required');
    return res.status(400).json({ error: 'streamId and streamKey are required' });
  }

  if (!isFfmpegAvailable()) {
    logSessionError(tempSession, 'FFmpeg not available, cannot start stream');
    return res.status(500).json({
      error: 'ffmpeg binary not found. Install ffmpeg or run Server B via docker image.',
    });
  }

  if (sessions.has(streamId)) {
    logSession(tempSession, 'Start request ignored; stream already running');
    return res.status(200).json({ success: true, stream_id: streamId, already_running: true });
  }

  const outputDir = outputDirForStream(streamId);
  tempSession.outputDir = outputDir;

  logSession(tempSession, 'Preparing output directory');
  cleanupOldArchives();
  if (fs.existsSync(outputDir)) {
    removeDir(outputDir);
  }
  fs.mkdirSync(outputDir, { recursive: true });
  logSession(tempSession, 'Output directory ready');

  const session = {
    streamId,
    streamKey,
    outputDir,
    startedAt: new Date().toISOString(),
    segmentsWritten: 0,
    currentBitrate: 3200,
    interval: null,
    ffmpegProcess: null,
    config: {
      streamId,
      streamKey,
      rtmpInputBase,
      callbackBaseUrl,
      bucket,
      cdnUrl
    },
    gcsState: createSessionGcsState(streamId, bucket, cdnUrl),
  };

  const ffmpegProcess = startFfmpegSession(session);
  session.ffmpegProcess = ffmpegProcess;
  logSession(session, 'FFmpeg process spawned');

  session.interval = setInterval(() => {
    void sendHeartbeat(session);
  }, HEARTBEAT_INTERVAL_MS);
  logSession(session, `Heartbeat interval established: ${HEARTBEAT_INTERVAL_MS}ms`);

  ffmpegProcess.on('exit', (code) => {
    const current = sessions.get(streamId);
    if (!current) return;
    logSession(current, `FFmpeg exited with code ${code}`);
    void finalizeTranscodeSession(streamId, current, Number(code || 0));
  });

  ffmpegProcess.on('error', (error) => {
    const current = sessions.get(streamId);
    if (!current) return;
    const message = error instanceof Error ? error.message : String(error);
    logSessionError(current, `FFmpeg failed during startup: ${message}`);
    void finalizeTranscodeSession(streamId, current, 127);
  });

  sessions.set(streamId, session);
  logSession(session, 'Transcode session registered');

  logSession(session, 'Sending first heartbeat for new session');
  await sendHeartbeat(session);
  await notifyStreamStarted(session);

  return res.status(200).json({
    success: true,
    stream_id: streamId,
    status: 'started',
  });
});

app.post('/transcode/stop', async (req, res) => {
  const streamId = Number(req.body?.streamId || req.body?.stream_id);

  if (!streamId) {
    console.error('[worker] Invalid stop request payload: streamId is missing');
    return res.status(400).json({ error: 'streamId is required' });
  }

  const session = sessions.get(streamId);
  if (!session) {
    console.log(`[worker][streamId:${streamId}] No active session found to stop. Triggering mock callback.`);
    // Derive callback parameters if possible, otherwise use fallback defaults
    const callbackBaseUrl = String(
      req.body?.callbackBaseUrl ||
      req.body?.callback_base_url ||
      `${process.env.SERVER_A_INTERNAL_URL || 'http://localhost:3000'}${process.env.SERVER_A_API_PREFIX || '/api/v1'}`
    );
    const bucket = String(req.body?.bucket || req.body?.gcs_bucket || process.env.GCS_BUCKET || '').trim();
    const cdnUrl = String(req.body?.cdnUrl || req.body?.cdn_url || process.env.CDN_URL || '').trim();
    await notifyStreamEnded(streamId, 0, callbackBaseUrl, bucket, cdnUrl);
    return res.status(200).json({ success: true, stream_id: streamId, status: 'already_stopped' });
  }

  logSession(session, 'Stopping active session');
  try {
    clearInterval(session.interval);
  } catch {
    /* ignore */
  }
  logSession(session, 'Heartbeat interval cleared');
  try {
    if (!session.ffmpegProcess.killed) {
      logSession(session, 'Sending SIGTERM to FFmpeg process');
      session.ffmpegProcess.kill('SIGTERM');
      setTimeout(() => {
        if (!session.ffmpegProcess.killed) {
          logSession(session, 'SIGTERM did not stop FFmpeg; sending SIGKILL');
          session.ffmpegProcess.kill('SIGKILL');
        }
      }, 2000);
    }
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    logSessionError(session, `Failed to stop ffmpeg process: ${message}`);
  }

  await finalizeTranscodeSession(streamId, session, 0);
  logSession(session, 'Stream ended notification sent');

  return res.status(200).json({
    success: true,
    stream_id: streamId,
    status: 'stopped',
  });
});

app.listen(PORT, '0.0.0.0', () => {
  console.log(`Server B worker running on port ${PORT}`);
});
