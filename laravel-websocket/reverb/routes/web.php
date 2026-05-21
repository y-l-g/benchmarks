<?php

use App\Events\BenchEvent;
use Illuminate\Support\Facades\Route;
use Illuminate\Http\Request;

// 1. Simple helper to fire events
Route::get('/fire', function (Request $request) {
    $count = min(max((int) $request->input('count', 1), 0), 10000);
    $size = min(max((int) $request->input('size', 100), 0), 65536);
    $intervalMs = min(max((float) $request->input('interval_ms', 0), 0), 1000);

    $start = microtime(true);

    for ($i = 0; $i < $count; $i++) {
        event(new BenchEvent($i, $size));

        if ($intervalMs > 0 && $i + 1 < $count) {
            usleep((int) round($intervalMs * 1000));
        }
    }

    return response()->json([
        'status' => 'fired',
        'count' => $count,
        'interval_ms' => $intervalMs,
        'duration_ms' => (microtime(true) - $start) * 1000,
    ]);
});

// 2. Auth for private channels (if needed later)
Route::post('/broadcasting/auth', function () {
    return true; // Open auth for benchmarking
});
