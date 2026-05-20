FROM dunglas/frankenphp:1.12.3-builder-php8.5-trixie AS builder

COPY --from=caddy:builder /usr/bin/xcaddy /usr/bin/xcaddy

ARG POGO_WS_MODULE_REF=3a48a17503f71f5b1cfb2ec9f3c7115b64046736

RUN CGO_ENABLED=1 \
    XCADDY_SETCAP=1 \
    XCADDY_GO_BUILD_FLAGS="-ldflags='-w -s' -tags=nobadger,nomysql,nopgx" \
    CGO_CFLAGS="-D_GNU_SOURCE $(php-config --includes)" \
    CGO_LDFLAGS="$(php-config --ldflags) $(php-config --libs)" \
    xcaddy build \
    --output /usr/local/bin/frankenphp \
    --with github.com/dunglas/frankenphp@v1.12.3 \
    --with github.com/dunglas/frankenphp/caddy@v1.12.3 \
    --with github.com/dunglas/caddy-cbrotli@v1.0.1 \
    --with github.com/y-l-g/websocket/module@${POGO_WS_MODULE_REF}

FROM golang:1.26.3-bookworm AS bench-receiver-builder

WORKDIR /src

COPY bench-receiver/go.mod bench-receiver/go.sum ./

RUN go mod download

COPY bench-receiver/. ./

RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /usr/local/bin/bench-receiver .

FROM debian:trixie-slim AS bench-receiver

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=bench-receiver-builder /usr/local/bin/bench-receiver /usr/local/bin/bench-receiver

ENTRYPOINT ["/usr/local/bin/bench-receiver"]

FROM serversideup/php:8.5.6-frankenphp-trixie

COPY --from=builder /usr/local/bin/frankenphp /usr/local/bin/frankenphp

ARG APP_PATH=pogo

COPY --chown=www-data:www-data ${APP_PATH}/. /var/www/html/

WORKDIR /var/www/html

USER www-data

RUN rm -f bootstrap/cache/packages.php bootstrap/cache/services.php \
    && composer install -v \
    --no-dev \
    --optimize-autoloader \
    --no-scripts \
    --prefer-dist \
    --classmap-authoritative
