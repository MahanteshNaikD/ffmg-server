# GCP storage (live HLS archive) and accessing content from another server

This worker can mirror each live transcode’s HLS output (`*.ts`, `*.m3u8`, `master.m3u8`) into a **Google Cloud Storage** bucket while streaming continues. Any other service (your “Server A” or a separate API host) can then **list objects** or **generate signed HTTPS URLs** so browsers or players can read the same HLS from GCP.

---

## 1. Worker configuration

| Variable | Required | Description |
|----------|----------|-------------|
| `GCS_BUCKET` | To enable uploads | Bucket name, e.g. `my-project-live-archive` |
| `GCS_HLS_PREFIX` | No | Logical folder under the bucket. Default: `live_stream`. Objects end up at `{prefix}/{stream_id}/...` |

**Google credentials** (pick one):

- **Local / VM:** set `GOOGLE_APPLICATION_CREDENTIALS` to a JSON key path for a service account that has `roles/storage.objectCreator` and `roles/storage.objectViewer` on the bucket (or a custom role with `storage.objects.create`, `storage.objects.get`, `storage.objects.list`, `storage.objects.update` for overwrites).
- **GKE / Cloud Run:** attach a workload identity or default service account with the same permissions; do not commit keys.

If `GCS_BUCKET` is unset or empty, **no** GCS code runs and no `@google-cloud/storage` client is created at runtime for uploads (the dependency is still installed).

---

## 2. Object layout in GCS

For `stream_id = 42` and default prefix:

```text
gs://{GCS_BUCKET}/live_stream/42/master.m3u8
gs://{GCS_BUCKET}/live_stream/42/v0.m3u8
gs://{GCS_BUCKET}/live_stream/42/v1.m3u8
gs://{GCS_BUCKET}/live_stream/42/v0_seg_000001.ts
…
```

`master.m3u8` is the multivariant playlist your player should use for **VOD-style replay** of what was captured during the live window (note: during live, playlists are sliding-window HLS; after the process ends, the last uploaded state reflects the final sliding window, not necessarily a full event VOD unless you change FFmpeg flags separately).

---

## 3. Webhooks your other server receives

The worker POSTs to Server A as today, and always includes a **`gcs`** object (paths are derived from `stream_id` and env so they are present even when there is no in-memory session).

### 3.1 `POST /internal/worker/heartbeat` (and `/api/v1/internal/worker/heartbeat` fallback)

Example JSON body:

```json
{
  "stream_id": 42,
  "segments_written": 120,
  "current_bitrate": 3200,
  "status": "ok",
  "gcs": {
    "enabled": true,
    "bucket": "my-project-live-archive",
    "object_prefix": "live_stream/42/",
    "master_manifest_object": "live_stream/42/master.m3u8",
    "gs_master_uri": "gs://my-project-live-archive/live_stream/42/master.m3u8",
    "https_master_uri": "https://storage.googleapis.com/my-project-live-archive/live_stream/42/master.m3u8",
    "stream_id": 42
  }
}
```

If `GCS_BUCKET` is unset, `gcs.enabled` is `false` and only `gcs.stream_id` is set. When `gcs.enabled` is `true`, persist at least `gcs.bucket`, `gcs.object_prefix` (or `gcs.master_manifest_object`) for your DB / “update stream” logic.

### 3.2 `POST /internal/worker/stream-ended`

Same `gcs` shape as the heartbeat, plus `exit_code`:

```json
{
  "stream_id": 42,
  "exit_code": 0,
  "gcs": {
    "enabled": true,
    "stream_id": 42,
    "bucket": "my-project-live-archive",
    "object_prefix": "live_stream/42/",
    "master_manifest_object": "live_stream/42/master.m3u8",
    "gs_master_uri": "gs://my-project-live-archive/live_stream/42/master.m3u8",
    "https_master_uri": "https://storage.googleapis.com/my-project-live-archive/live_stream/42/master.m3u8"
  }
}
```

---

## 4. How the other server gets content from GCP

Three common patterns:

### A. Signed URL to `master.m3u8` (recommended for private buckets)

On **your** API server (not necessarily this worker), use the official client with the **same** or another service account that has `storage.objects.get` on the bucket:

```js
const { Storage } = require('@google-cloud/storage');
const storage = new Storage();
const BUCKET = 'my-project-live-archive';
const file = storage.bucket(BUCKET).file('live_stream/42/master.m3u8');

const [url] = await file.getSignedUrl({
  version: 'v4',
  action: 'read',
  expires: Date.now() + 60 * 60 * 1000, // 1 hour
});
// Return `url` to the client; the player will fetch child playlists/segments — each also needs to be readable.
```

**Important:** HLS loads `v0.m3u8`, `v1.m3u8`, and many `.ts` files. Either:

- use a **bucket IAM / public read** only for archived prefixes (not always desirable), or  
- expose **your** API routes that redirect or proxy each path with signed URLs, or  
- sign URLs for the manifest only if segments are public (hybrid), or  
- use **Cloud CDN + signed cookies/URLs** in front of a bucket backend.

For a minimal private setup, many teams use a **VOD packaging step** after live ends and a single MP4 or a packaged DASH/HLS output with one manifest—this worker currently mirrors raw HLS as produced by FFmpeg.

### B. Google Cloud Storage JSON API (language-agnostic)

**List objects** under a stream prefix:

```http
GET https://storage.googleapis.com/storage/v1/b/BUCKET_NAME/o?prefix=live_stream/42/
Authorization: Bearer ACCESS_TOKEN
```

`ACCESS_TOKEN` is an OAuth2 token for a service account or user with `storage.objects.list`. Response items include `name`, `size`, `updated`, `mediaLink`, etc.

**Download an object** (binary):

```http
GET https://storage.googleapis.com/storage/v1/b/BUCKET_NAME/o/OBJECT_PATH/alt=media
Authorization: Bearer ACCESS_TOKEN
```

`OBJECT_PATH` must be URL-encoded (e.g. `live_stream%2F42%2Fmaster.m3u8`).

Reference: [Objects: list](https://cloud.google.com/storage/docs/json_api/v1/objects/list), [Objects: get](https://cloud.google.com/storage/docs/json_api/v1/objects/get).

### C. `gsutil` / gcloud (ops, not usually for app servers)

```bash
gsutil ls gs://my-project-live-archive/live_stream/42/
gsutil cp gs://my-project-live-archive/live_stream/42/master.m3u8 .
```

---

## 5. Caching note for players

The worker sets **short** `Cache-Control` on `*.m3u8` and longer cache on segments. If you put **Cloud CDN** in front of the bucket, align CDN cache keys so playlists stay fresh.

---

## 6. Summary

| Concern | Approach |
|--------|----------|
| Enable upload | Set `GCS_BUCKET` (+ credentials) |
| Know where data is | Read `gcs` from heartbeat / stream-ended webhooks |
| Another server reads content | Service account + Storage client, JSON API, or signed URLs |
| Browser playback | HTTPS URLs (signed or public); ensure all HLS referenced files are reachable with the same access model |

For product behavior (full-event recording, DRM, CDN), plan those in Server A or a media pipeline; this worker’s role is **parallel mirror of the live HLS output** into GCS and **reporting the prefix** to your control plane.
