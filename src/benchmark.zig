const std = @import("std");
const Io = std.Io;
const IpAddress = Io.net.IpAddress;
const log = std.log;
const flate = std.compress.flate;

const zio = @import("zio");

const server_addr = IpAddress.parseLiteral("127.0.0.1:3000") catch unreachable;
const run_seconds_default = 10;
const connections_default = 1000;

pub fn main(init: std.process.Init) !void {
    const rt = try zio.Runtime.init(init.gpa, .{});
    defer rt.deinit();
    const io = rt.io();

    var reqs: std.atomic.Value(u64) = .init(0);
    var bytes: std.atomic.Value(usize) = .init(0);
    var start: std.Io.Event = .unset;
    var end: std.Io.Event = .unset;

    var group: Io.Group = .init;
    defer group.cancel(io);

    var run_seconds: u64 = run_seconds_default;
    var connections: u64 = connections_default;
    var args = init.minimal.args.iterate();
    _ = args.skip();
    if (args.next()) |arg| connections = try std.fmt.parseInt(u64, arg, 0);
    if (args.next()) |arg| run_seconds = try std.fmt.parseInt(u64, arg, 0);
    if (args.skip()) return error.UnhandledArgument;

    for (0..connections) |_| {
        group.async(io, request, .{
            io,
            &reqs,
            &bytes,
            &start,
            &end,
        });
    }

    start.set(io);
    const Result = union(enum) { any: Io.Cancelable!void };
    var result_buf: [1]Result = undefined;
    var select: Io.Select(Result) = .init(io, &result_buf);
    defer _ = select.cancel();
    select.async(.any, sample, .{ io, &reqs, &bytes, &end, run_seconds });
    select.async(.any, Io.Event.wait, .{ &end, io });
    _ = try select.await();
}

fn sample(
    io: Io,
    reqs_atomic: *std.atomic.Value(u64),
    bytes_atomic: *std.atomic.Value(usize),
    end: *Io.Event,
    run_seconds: u64,
) Io.Cancelable!void {
    const clock: std.Io.Clock = .awake;

    for (0..run_seconds) |i| {
        const ts = clock.now(io);
        try io.sleep(.fromSeconds(1), clock);
        const dur = ts.durationTo(clock.now(io));

        const reqs_sample = reqs_atomic.swap(0, .monotonic);
        const bytes_sample = bytes_atomic.swap(0, .monotonic);

        const requests: f128 = @floatFromInt(reqs_sample);
        const bytes: f128 = @floatFromInt(bytes_sample);
        const nanoseconds: f128 = @floatFromInt(dur.nanoseconds);
        const seconds = nanoseconds / std.time.ns_per_s;

        std.log.info("{}: RPS: {:.2}, decompressed throughput: {:.2} MiB/s", .{
            i, requests / seconds, (bytes / (1024.0 * 1024.0)) / seconds,
        });
    }
    end.set(io);
}

fn request(
    io: Io,
    reqs_atomic: *std.atomic.Value(u64),
    bytes_atomic: *std.atomic.Value(usize),
    start: *Io.Event,
    end: *Io.Event,
) Io.Cancelable!void {
    const stream = server_addr.connect(io, .{ .mode = .stream }) catch |err| switch (err) {
        error.Canceled => return error.Canceled,
        else => {
            if (!end.isSet()) {
                log.err("Failed to connect", .{});
            }
            end.set(io);
            return;
        },
    };
    defer stream.close(io);

    var write_buf: [1024]u8 = undefined;
    var read_buf: [1024]u8 = undefined;
    var decomp_buf: [flate.max_window_len]u8 = undefined;
    var sr = stream.reader(io, &read_buf);
    var sw = stream.writer(io, &write_buf);
    const sri = &sr.interface;
    const swi = &sw.interface;

    try start.wait(io);
    const err = while (true) {
        // Prevent main task starvation
        if (end.isSet()) {
            return;
        }

        swi.writeAll("README.md\x00") catch break sw.err.?;
        swi.flush() catch break sw.err.?;

        // Check for server error
        const err_token_opt = sri.peekArray("ERROR".len) catch
            if (sr.err) |err| break err else null;
        if (err_token_opt) |err_token| {
            if (std.mem.eql(u8, err_token, "ERROR")) {
                const full_err = sr.interface.takeDelimiter(0) catch |err|
                    break sr.err orelse err;
                log.err("{s}", .{full_err.?});
                continue;
            }
        }

        var decomp: flate.Decompress = .init(sri, .gzip, &decomp_buf);

        const read = decomp.reader.discardRemaining() catch
            break decomp.err orelse sr.err.?;

        _ = reqs_atomic.fetchAdd(1, .monotonic);
        _ = bytes_atomic.fetchAdd(read, .monotonic);
    };

    switch (err) {
        error.Canceled => return error.Canceled,
        error.ReadFailed => {},
        else => {
            if (!end.isSet()) {
                log.err("Fatal error {t}", .{err});
            }
            end.set(io);
        },
    }
}
