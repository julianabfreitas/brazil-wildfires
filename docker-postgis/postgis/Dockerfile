# postgis/Dockerfile
FROM postgis/postgis:12-3.5

EXPOSE 25432

COPY cleanup.sql /docker-entrypoint-initdb.d/

