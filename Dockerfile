FROM gcr.io/distroless/static-debian12:nonroot
COPY tg-webdav /tg-webdav
EXPOSE 8080
ENTRYPOINT ["/tg-webdav"]
