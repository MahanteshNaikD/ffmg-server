# Server B Worker (Deployable Separately)

This folder contains **Server B**, a standalone worker service for live stream transcoding.

It is designed to be deployed independently from Server A (Nest backend), while staying in the same monorepo.

## Exposed APIs

- `GET /health`
- `POST /transcode/start` body: `{ stream_id, stream_key }`
- `POST /transcode/stop` body: `{ stream_id }`
- `GET /hls/:stream_id/master.m3u8` (generated playback manifest)

## Integration with Server A

Server A calls this service using:

- `FFMPEG_WORKER_URL=http://<server-b-host>:8080`

Server B sends callbacks to Server A:

- `POST /internal/worker/heartbeat`
- `POST /internal/worker/stream-ended`

Configured via:

- `SERVER_A_INTERNAL_URL=http://<server-a-host>:3000`

## Local run

```bash
cd server-b-worker
npm install
cp .env.example .env
npm run dev
```

## Docker run

```bash
docker build -t server-b-worker ./server-b-worker
docker run --rm -p 8080:8080 --env-file ./server-b-worker/.env server-b-worker
```

## FFmpeg pipeline

- Input source URL: `${RTMP_INPUT_BASE}/{stream_key}`
- Output: HLS adaptive playlists in `${HLS_ROOT}/{stream_id}`
- Variants:
  - 720p (~3000 kbps)
  - 480p (~1200 kbps)
- Master playlist: `master.m3u8`

Server B also sends periodic heartbeat based on actual generated segment files.
