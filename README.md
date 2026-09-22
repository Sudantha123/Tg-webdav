# Tg-webdav

Lightweight Telegram-backed WebDAV server written in Go.

## Endpoints

- WebDAV: `http://VPS_IP:PORT/dav`
- Web UI: `http://VPS_IP:PORT/web`

## Features

- Telegram MTProto storage/read path using gotd/td.
- Persistent Telegram bot session.
- Incoming Telegram media indexing into configurable `DEFAULT_FOLDER`.
- Incoming files forwarded to `TELEGRAM_CHANNEL_ID`.
- WebDAV Basic Authentication.
- HTTP Range/seek reads using Telegram chunk requests.
- Bounded in-memory chunk cache.
- WebDAV MKCOL, DELETE and MOVE/RENAME.
- WebDAV PUT uploads sent to the configured Telegram channel through MTProto.
- Embedded HTML/CSS/JavaScript management UI.
- SQLite metadata.
- Linux amd64/arm64 GitHub Actions builds.
- No Node.js runtime.

## Configuration

Copy `.env.example` to `.env` and configure Telegram API ID/hash, bot token, target channel ID and WebDAV credentials.

The Telegram bot must be able to receive incoming files. The Telegram bot account must have permission to post files to the configured channel.

For public Internet exposure, put HTTPS/TLS in front of the service.