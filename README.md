# Zig Async Flate Server (`zio_flate_server`)

A high-performance, fully asynchronous, compressed file server built on Zig's new `std.Io` interface. 

## Features
* **std.Io:** Functions are runtime-agnostic. See [Available Backends](#available-backends).
* **Compression:** Uses `std.compress.flate` to compress files before sending them over the network.
* **Graceful Shutdown:** Safely handles `SIGINT` (Ctrl+C).
* **Safe Cancellation:** Protects file-transfer regions to ensure clients never receive half-written data during a shutdown.
* **Caching:** Caches compressed files using a `StringHashMap` and `Io.RwLock`. Currently doesn't feature any cache invalidation, the server must be restarted if file data changes.

## Requirements
* **Zig 0.16**.
* The target OS must support `sigaction()`.

## Building and Running

This project supports multiple `Io` backends via the Zig build system. You can select the backend using the `-Dio=` flag.

### Available Backends:
* `zio` (Default): Uses the [zio](https://github.com/lalinsky/zio) runtime. Highly optimized stackful coroutines.
* `std`: Uses Zig's built-in `std.Io.Threaded` runtime.
* `single_threaded`: A purely synchronous, blocking fallback without a concurrent scheduler.

### Run Commands:
```bash
# Run with the default zio backend
zig build run

# Run with Zig's standard threaded Io
zig build -Dio=std run

# Run in single-threaded blocking mode
zig build -Dio=single_threaded run
```

## Client Usage

To verify that the server responds with correct data, you can use `nc` and `gzip`.

1. Start the server in one terminal: `zig build run`.
2. In a second terminal, you can act as a client:
   ```bash
   echo -ne 'README.md\0' | nc localhost 3000 | gzip -d
   ```
   This should output the contents of this README file.

### Stress Testing:
To test the server's concurrency and observe it in action, you can use the included benchmark program to spam the server with requests.

1. Start the server in one terminal: `zig build -Doptimize=ReleaseFast run`.
2. In a second terminal, execute the benchmark:
   ```
   zig build -Doptimize=ReleaseFast benchmark
   ```
3. To test cancelation and error handling while the stress test is hammering the server, return to the first terminal and press Ctrl+C.
