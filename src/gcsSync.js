const path = require('path');
const fs = require('fs');

/** Read at use-time so `.env` is applied even if this module loads after dotenv in some entrypoints. */
function gcsBucket() {
  return (process.env.GCS_BUCKET || '').trim();
}

function gcsPrefix() {
  return (process.env.GCS_HLS_PREFIX || 'live_stream').replace(/^\/+/, '').replace(/\/+$/, '');
}

let storageClient = null;
let bucketRef = null;

/** @type {WeakMap<object, Promise<void>>} */
const gcsUploadTail = new WeakMap();

function logGcsError(session, error) {
  const message = error instanceof Error ? error.message : String(error);
  const id = session?.streamId ?? '?';
  console.error('[worker][gcs]', `stream ${id}`, message);
}

function isGcsEnabled() {
  return Boolean(gcsBucket());
}

function ensureBucket() {
  const name = gcsBucket();
  if (!name) return null;
  if (!storageClient) {
    const { Storage } = require('@google-cloud/storage');
    storageClient = new Storage();
    bucketRef = storageClient.bucket(name);
  }
  return bucketRef;
}

function objectPrefixForStream(streamId) {
  const id = String(streamId);
  const prefix = gcsPrefix();
  const base = prefix ? `${prefix}/${id}/` : `${id}/`;
  return base.replace(/\/+/g, '/');
}

function contentTypeForName(name) {
  if (name.endsWith('.m3u8')) return 'application/vnd.apple.mpegurl';
  if (name.endsWith('.ts')) return 'video/mp2t';
  return 'application/octet-stream';
}

function cacheControlForName(name) {
  if (name.endsWith('.m3u8')) return 'max-age=2, must-revalidate';
  return 'public, max-age=31536000, immutable';
}

/**
 * @param {number|string} streamId
 * @returns {{ objectPrefix: string, uploaded: Map<string, { mtimeMs: number, size: number }> } | null}
 */
function createSessionGcsState(streamId) {
  if (!isGcsEnabled()) return null;
  ensureBucket();
  return {
    objectPrefix: objectPrefixForStream(streamId),
    uploaded: new Map(),
  };
}

/**
 * Upload new/changed HLS files under session.outputDir to GCS.
 * @param {{ outputDir: string, gcsState: ReturnType<typeof createSessionGcsState> }} session
 */
async function syncOutputDirToGcs(session) {
  const bucket = ensureBucket();
  const gcsState = session.gcsState;
  if (!bucket || !gcsState) return;

  const dir = session.outputDir;
  let names;
  try {
    names = fs.readdirSync(dir);
  } catch {
    return;
  }

  for (const name of names) {
    if (!/\.(ts|m3u8)$/.test(name)) continue;
    const fullPath = path.join(dir, name);
    let stat;
    try {
      stat = fs.statSync(fullPath);
    } catch {
      continue;
    }
    if (!stat.isFile()) continue;

    const prev = gcsState.uploaded.get(name);
    if (prev && prev.mtimeMs === stat.mtimeMs && prev.size === stat.size) continue;

    const objectName = `${gcsState.objectPrefix}${name}`.replace(/\/+/g, '/');
    const buf = await fs.promises.readFile(fullPath);
    const file = bucket.file(objectName);
    await file.save(buf, {
      resumable: false,
      contentType: contentTypeForName(name),
      metadata: {
        cacheControl: cacheControlForName(name),
      },
    });
    gcsState.uploaded.set(name, { mtimeMs: stat.mtimeMs, size: stat.size });
  }
}

/**
 * Queue a GCS sync for this session; runs after prior queued syncs finish. Does not block the caller.
 * @param {{ outputDir: string, gcsState: ReturnType<typeof createSessionGcsState>, streamId?: number }} session
 * @returns {Promise<void>}
 */
function enqueueGcsSync(session) {
  if (!isGcsEnabled() || !session?.gcsState) return Promise.resolve();
  if (!ensureBucket()) return Promise.resolve();
  const prev = gcsUploadTail.get(session) || Promise.resolve();
  const next = prev.then(() =>
    syncOutputDirToGcs(session).catch((err) => {
      logGcsError(session, err);
    })
  );
  gcsUploadTail.set(session, next);
  return next;
}

/**
 * Wait for all queued uploads plus one final pass (call before moving/deleting output dir).
 * @param {{ outputDir: string, gcsState: ReturnType<typeof createSessionGcsState>, streamId?: number }} session
 */
async function flushGcsSync(session) {
  if (!isGcsEnabled() || !session?.gcsState) return;
  if (!ensureBucket()) return;
  const tail = enqueueGcsSync(session);
  await tail;
  gcsUploadTail.delete(session);
}

/**
 * Fields to merge into heartbeat / stream-ended for Server A and other services.
 * @param {number} streamId
 * @param {{ objectPrefix: string } | null} gcsState
 */
function gcsPayloadForWebhook(streamId, gcsState) {
  const bucket = gcsBucket();
  if (!bucket || !gcsState) return {};
  const prefix = gcsState.objectPrefix;
  const masterObject = `${prefix}master.m3u8`.replace(/\/+/g, '/');
  return {
    gcs: {
      enabled: true,
      bucket,
      object_prefix: prefix,
      master_manifest_object: masterObject,
      gs_master_uri: `gs://${bucket}/${masterObject}`,
      stream_id: streamId,
    },
  };
}

module.exports = {
  isGcsEnabled,
  createSessionGcsState,
  syncOutputDirToGcs,
  enqueueGcsSync,
  flushGcsSync,
  gcsPayloadForWebhook,
  gcsConfig: () => ({ bucket: gcsBucket() || null, prefix: gcsPrefix() }),
};
