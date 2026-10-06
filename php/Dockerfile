FROM php:8.5-cli
WORKDIR /opt/queueforge
RUN docker-php-ext-install sockets
COPY src /opt/queueforge/src
COPY bin /opt/queueforge/bin
COPY config.docker.toml /opt/queueforge/config.docker.toml
RUN mkdir -p /var/lib/queueforge
EXPOSE 5672
ENTRYPOINT ["php", "bin/queueforge", "--config", "/opt/queueforge/config.docker.toml", "--dev-bootstrap"]
