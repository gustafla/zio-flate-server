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

    var group: Io.Group = .init;
    defer group.cancel(io);

    for (0..1000) |_| {
        group.async(io, request, .{ io, &rps, &bytes });
    }

    for (0..10) |i| {
        try io.sleep(.fromSeconds(1), .awake);
        const rps_sample = rps.swap(0, .acquire);
        const bytes_sample = bytes.swap(0, .acquire);
        std.log.info("{}: RPS: {}, compressed throughput: {:.2} MiB/s", .{
            i, rps_sample, @as(f128, @floatFromInt(bytes_sample)) / (1024.0 * 1024.0),
        });
    }
}

pub fn request(
    io: Io,
    rps: *std.atomic.Value(u64),
    bytes: *std.atomic.Value(u64),
) Io.Cancelable!void {
    const server_addr = comptime IpAddress.parseLiteral("127.0.0.1:3000") catch unreachable;
    var write_buf: [1024]u8 = undefined;
    var read_buf: [1024]u8 = undefined;

    while (true) {
        const stream = server_addr.connect(io, .{ .mode = .stream }) catch |e|
            if (e == error.Canceled) return error.Canceled else continue;
        defer stream.close(io);
        var streamw = stream.writer(io, &write_buf);
        var streamr = stream.reader(io, &read_buf);

        streamw.interface.writeAll("README.md\x00") catch
            if (streamw.err.? == error.Canceled) return error.Canceled else continue;
        streamw.interface.flush() catch
            if (streamw.err.? == error.Canceled) return error.Canceled else continue;
        const read_bytes = streamr.interface.discardRemaining() catch
            if (streamr.err.? == error.Canceled) return error.Canceled else continue;

        _ = rps.fetchAdd(1, .release);
        _ = bytes.fetchAdd(@intCast(read_bytes), .release);
    }
}
