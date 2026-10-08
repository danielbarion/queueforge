FROM php:8.5-cli
WORKDIR /opt/queueforge
RUN docker-php-ext-install sockets \
    && printf '%s\n' \
        'opcache.enable=1' \
        'opcache.enable_cli=1' \
        'opcache.jit_buffer_size=64M' \
        'opcache.jit=tracing' \
        'memory_limit=-1' \
        > /usr/local/etc/php/conf.d/zz-jit.ini
COPY src /opt/queueforge/src
COPY bin /opt/queueforge/bin
COPY config.docker.toml /opt/queueforge/config.docker.toml
RUN mkdir -p /var/lib/queueforge
EXPOSE 5672 15672
ENTRYPOINT ["php", "bin/queueforge", "--config", "/opt/queueforge/config.docker.toml", "--dev-bootstrap"]
