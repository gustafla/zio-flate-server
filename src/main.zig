const std = @import("std");
const Allocator = std.mem.Allocator;
const Io = std.Io;
const log = std.log;

pub fn main(init: std.process.Init) !void {
    const gpa = init.gpa;
    const io = init.io;

    const listen_addr = comptime Io.net.IpAddress.parseLiteral("0.0.0.0:3000") catch unreachable;
    var server = try listen_addr.listen(io, .{ .reuse_address = true });
    defer server.deinit(io);
    log.info("Listening on {f}", .{listen_addr});

    var group: Io.Group = .init;
    defer group.cancel(io);

    while (true) {
        const stream = server.accept(io) catch |err| {
            log.err("Failed to accept connection: {}", .{err});
            continue;
        };

        group.async(io, handleClient, .{ io, gpa, stream });
    }
}

fn handleClient(io: Io, gpa: Allocator, stream: Io.net.Stream) Io.Cancelable!void {
    _ = gpa;
    defer stream.close(io);

    const addr = stream.socket.address;

    var path_buf: [Io.Dir.max_path_bytes]u8 = undefined;
    var stream_reader = stream.reader(io, &path_buf);
    const reader = &stream_reader.interface;

    const path_opt = reader.takeDelimiter(0) catch |err| switch (err) {
        error.ReadFailed => switch (stream_reader.err.?) {
            error.Canceled => return error.Canceled,
            else => |stream_err| {
                // A real protocol would send some encoded error message
                log.err("Client {f} error: {}", .{ addr, stream_err });
                return;
            },
        },
        error.StreamTooLong => {
            // A real protocol would send some encoded error message
            log.err("Client {f} request path too long", .{addr});
            return;
        },
    };

    const path = path_opt orelse {
        log.info("Client {f} disconnected before sending a path", .{addr});
        return;
    };

    log.info("Client {f} requested {s}", .{ addr, path });
}
