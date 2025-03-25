# minio/Dockerfile
FROM minio/minio:RELEASE.2024-10-13T13-34-11Z.fips

EXPOSE 9000
EXPOSE 9001

ENTRYPOINT ["minio"]
CMD ["server", "/data"]

