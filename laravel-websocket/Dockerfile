FROM dunglas/frankenphp:1.12.2-builder-php8.5-trixie AS builder

COPY --from=caddy:builder /usr/bin/xcaddy /usr/bin/xcaddy

RUN CGO_ENABLED=1 \
    XCADDY_SETCAP=1 \
    XCADDY_GO_BUILD_FLAGS="-ldflags='-w -s' -tags=nobadger,nomysql,nopgx" \
    CGO_CFLAGS="-D_GNU_SOURCE $(php-config --includes)" \
    CGO_LDFLAGS="$(php-config --ldflags) $(php-config --libs)" \
    xcaddy build \
    --output /usr/local/bin/frankenphp \
    --with github.com/dunglas/frankenphp@v1.12.2 \
    --with github.com/dunglas/frankenphp/caddy@v1.12.2 \
    --with github.com/dunglas/caddy-cbrotli@v1.0.1 \
    --with github.com/y-l-g/websocket/module@main

FROM serversideup/php:8.5.6-frankenphp-trixie

COPY --from=builder /usr/local/bin/frankenphp /usr/local/bin/frankenphp

ARG APP_PATH=benchmarks/laravel-websocket/pogo

COPY --chown=www-data:www-data ${APP_PATH}/. /var/www/html/

WORKDIR /var/www/html

USER www-data

RUN composer install -v \
    --no-dev \
    --optimize-autoloader \
    --no-scripts \
    --prefer-dist \
    --classmap-authoritative
