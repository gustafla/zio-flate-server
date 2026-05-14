const std = @import("std");
const Io = std.Io;
const IpAddress = Io.net.IpAddress;
const log = std.log;

const zio = @import("zio");

pub fn main(init: std.process.Init) !void {
    const rt = try zio.Runtime.init(init.gpa, .{});
    defer rt.deinit();
    const io = rt.io();

    var rps: std.atomic.Value(u64) = .init(0);
    var bytes: std.atomic.Value(u64) = .init(0);
    var connect_fail: Io.Event = .unset;

    var group: Io.Group = .init;
    defer group.cancel(io);

    for (0..1000) |_| {
        group.async(io, request, .{ io, &rps, &bytes, &connect_fail });
    }

    for (0..10) |i| {
        if (connect_fail.isSet()) {
            log.err("Failed to connect", .{});
            return;
        }
        try io.sleep(.fromSeconds(1), .awake);
        const rps_sample = rps.swap(0, .acquire);
        const bytes_sample = bytes.swap(0, .acquire);
        std.log.info("{}: RPS: {}, decompressed throughput: {:.2} MiB/s", .{
            i, rps_sample, @as(f128, @floatFromInt(bytes_sample)) / (1024.0 * 1024.0),
        });
    }
}

fn request(
    io: Io,
    rps: *std.atomic.Value(u64),
    bytes: *std.atomic.Value(u64),
    connect_fail: *Io.Event,
) Io.Cancelable!void {
    const server_addr = comptime IpAddress.parseLiteral("127.0.0.1:3000") catch unreachable;

    var write_buf: [1024]u8 = undefined;
    var read_buf: [1024]u8 = undefined;
    var flate_buf: [std.compress.flate.max_window_len]u8 = undefined;

    var stream = server_addr.connect(io, .{ .mode = .stream }) catch {
        connect_fail.set(io);
        return;
    };
    defer stream.close(io);

    const err = while (true) {
        var streamw = stream.writer(io, &write_buf);
        var streamr = stream.reader(io, &read_buf);
        var flate: std.compress.flate.Decompress = .init(
            &streamr.interface,
            .gzip,
            &flate_buf,
        );

        streamw.interface.writeAll("README.md\x00") catch break streamw.err.?;
        streamw.interface.flush() catch break streamw.err.?;

        // Check for server error
        const err_token = streamr.interface.peekArray("ERROR".len) catch
            if (streamr.err) |err| break err else null;
        if (err_token) |str| {
            if (std.mem.eql(u8, str, "ERROR")) {
                const full_err = streamr.interface.takeDelimiter(0) catch |err|
                    break streamr.err orelse err;
                log.err("{s}", .{full_err.?});
                continue;
            }
        }

        const read_bytes = flate.reader.discardRemaining() catch
            break flate.err orelse streamr.err.?;

        _ = rps.fetchAdd(1, .release);
        _ = bytes.fetchAdd(@intCast(read_bytes), .release);
    };

    if (err == error.Canceled) return error.Canceled;
    log.err("{t}", .{err});
}
