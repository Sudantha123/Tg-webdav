# Tg-webdav

Telegram-backed WebDAV server written in Go.

## Endpoints

- WebDAV: /dav
- Web UI: /web

## Current core

- Telegram MTProto bot session using gotd/td.
- SQLite metadata index.
- Telegram file locations persisted with gob.
- Range-aware reads using upload.getFile offsets.
- Bounded in-memory chunk cache.
- WebDAV Basic Authentication.
- Lightweight embedded Web UI.
- GitHub Actions Linux amd64/arm64 builds.

## Configuration

Copy .env.example to .env and set Telegram API credentials, bot token, channel ID, and WebDAV credentials.

The first implementation is read-optimized. MKCOL, DELETE and MOVE are reserved for the next phase so the range reader and Telegram storage path can be validated independently.

For public Internet exposure, use HTTPS/TLS in front of the HTTP server.
