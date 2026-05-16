# Pogo Laravel Broadcast Benchmark

This is a minimal Laravel application for benchmarking `pogo/websocket` broadcasting.

It is intentionally kept small and should not be used as a feature showcase. Human-facing demos belong in `pogoShowcase`.

## Run

```bash
composer install
cp .env.example .env
php artisan key:generate
php artisan migrate --force
frankenphp run --config Caddyfile
```

From `benchmarks/laravel-broadcast`, run:

```bash
DRIVER=pogo HOST=localhost HTTP_PORT=80 WS_PORT=80 k6 run benchmark.js
```
