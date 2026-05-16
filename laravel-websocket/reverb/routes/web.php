<?php

use App\Events\BenchEvent;
use Illuminate\Support\Facades\Route;
use Illuminate\Http\Request;

// 1. Simple helper to fire events
Route::get('/fire', function (Request $request) {
    $count = min(max((int) $request->input('count', 1), 0), 10000);
    $size = min(max((int) $request->input('size', 100), 0), 65536);

    $start = microtime(true);

    for ($i = 0; $i < $count; $i++) {
        event(new BenchEvent($i, $size));
    }

    return response()->json([
        'status' => 'fired',
        'count' => $count,
        'duration_ms' => (microtime(true) - $start) * 1000,
    ]);
});

// 2. Auth for private channels (if needed later)
Route::post('/broadcasting/auth', function () {
    return true; // Open auth for benchmarking
});
