# Reverb Laravel Broadcast Benchmark

This is a minimal Laravel application for benchmarking Laravel Reverb against the same broadcast workload used by the Pogo app.

It is intentionally kept small and should not be used as a feature showcase. Human-facing demos belong in `pogoShowcase`.

## Run

```bash
composer install
cp .env.example .env
php artisan key:generate
php artisan migrate --force
```

Start the Laravel HTTP app and Reverb websocket server according to the Laravel Reverb setup, then from `benchmarks/laravel-broadcast` run:

```bash
DRIVER=reverb HOST=localhost HTTP_PORT=8000 WS_PORT=8080 k6 run benchmark.js
```
