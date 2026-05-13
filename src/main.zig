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
            log.err("Failed to accept connection: {t}", .{err});
            continue;
        };

        group.async(io, handleClient, .{ io, gpa, stream });
    }
}

fn handleClient(io: Io, gpa: Allocator, stream: Io.net.Stream) Io.Cancelable!void {
    _ = gpa;
    defer stream.close(io);

    const addr = stream.socket.address;

    var write_buf: [1024]u8 = undefined;
    var path_buf: [Io.Dir.max_path_bytes]u8 = undefined;
    var stream_reader = stream.reader(io, &path_buf);
    var stream_writer = stream.writer(io, &write_buf);
    const reader = &stream_reader.interface;
    const writer = &stream_writer.interface;
    defer writer.flush() catch log.err(
        "Can't send to {f}: {t}",
        .{ addr, stream_writer.err.? },
    );

    const path_opt = reader.takeDelimiter(0) catch |err| return switch (err) {
        error.ReadFailed => switch (stream_reader.err.?) {
            error.Canceled => error.Canceled,
            else => |stream_err| log.err("Client {f} error: {t}", .{ addr, stream_err }),
        },
        error.StreamTooLong => {
            writer.writeAll("ERROR PathTooLong\n\x00") catch {};
            log.err("Client {f} request path too long", .{addr});
        },
    };

    const path = path_opt orelse {
        log.info("Client {f} disconnected before sending a path", .{addr});
        return;
    };

    log.info("Client {f} requested {s}", .{ addr, path });

    var file = Io.Dir.cwd().openFile(io, path, .{
        .allow_directory = false,
        .resolve_beneath = true,
    }) catch |err| return switch (err) {
        error.Canceled => error.Canceled,
        else => |open_err| {
            writer.print("ERROR {t}\n\x00", .{open_err}) catch {};
            log.err("Can't open file {s} for {f}: {}", .{ path, addr, open_err });
        },
    };
    defer file.close(io);
    var file_buf: [1024]u8 = undefined;
    var file_reader = file.reader(io, &file_buf);

    var comp_buf: [std.compress.flate.max_window_len * 4]u8 = undefined;
    var comp = std.compress.flate.Compress.init(
        writer,
        &comp_buf,
        .gzip,
        .default,
    ) catch return;
    defer comp.finish() catch {};
    const comp_writer = &comp.writer;

    const read = file_reader.interface.streamRemaining(comp_writer) catch |err| return switch (err) {
        error.ReadFailed => switch (file_reader.err.?) {
            error.Canceled => error.Canceled,
            else => |read_err| {
                writer.print("ERROR {t}\n\x00", .{read_err}) catch {};
                log.err("Can't read file {s} for {f}: {}", .{ path, addr, read_err });
            },
        },
        error.WriteFailed => if (stream_writer.err) |e| if (e == error.Canceled) error.Canceled,
    };
    log.info("Successfully served {} bytes to {f}", .{ read, addr });
}
