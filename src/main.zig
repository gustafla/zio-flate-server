const std = @import("std");
const builtin = @import("builtin");
const Allocator = std.mem.Allocator;
const Io = std.Io;
const IpAddress = Io.net.IpAddress;
const log = std.log;

const zio = @import("zio");

var sig_stream: [2]Io.net.Stream = undefined;

fn sigintHandler(_: std.c.SIG) callconv(.c) void {
    _ = std.c.write(sig_stream[1].socket.handle, &.{1}, 1);
}

const ShutdownCtx = struct {
    server: *Io.net.Server,
    is_shutting_down: *std.atomic.Value(bool),
};

pub fn main(init: std.process.Init) !void {
    const gpa = init.gpa;
    const zio_rt = try zio.Runtime.init(gpa, .{});
    defer zio_rt.deinit();
    const io = zio_rt.io();
    // const io = init.io;

    const sig_ok = blk: {
        const loopback_addr = comptime IpAddress.parseLiteral("127.0.0.1:0") catch unreachable;
        var temp_listener = loopback_addr.listen(io, .{}) catch break :blk false;
        defer temp_listener.deinit(io);

        const bound_addr = temp_listener.socket.address;
        sig_stream[1] = bound_addr.connect(io, .{
            .mode = .stream,
        }) catch break :blk false;
        sig_stream[0] = temp_listener.accept(io) catch {
            sig_stream[1].close(io);
            break :blk false;
        };

        var sa: std.c.Sigaction = .{
            .handler = .{ .handler = sigintHandler },
            .mask = undefined,
            .flags = 0,
        };
        if (std.c.sigemptyset(&sa.mask) != 0) {
            sig_stream[0].close(io);
            sig_stream[1].close(io);
            break :blk false;
        }
        break :blk std.c.sigaction(std.c.SIG.INT, &sa, null) == 0;
    };

    defer if (sig_ok) {
        sig_stream[0].close(io);
        sig_stream[1].close(io);
    };

    const listen_addr = comptime IpAddress.parseLiteral("0.0.0.0:3000") catch unreachable;
    var server = try listen_addr.listen(io, .{ .reuse_address = true });
    var is_shutting_down: std.atomic.Value(bool) = .init(false);
    defer if (!is_shutting_down.load(.monotonic)) server.deinit(io);
    log.info("Listening on {f}", .{listen_addr});

    var admin_group: Io.Group = .init;
    defer admin_group.cancel(io);

    const shutdown_ctx: ShutdownCtx = .{
        .server = &server,
        .is_shutting_down = &is_shutting_down,
    };
    if (sig_ok) {
        log.info("Spawning SIGINT watcher coroutine", .{});
        admin_group.async(io, watchSigintPipe, .{ io, shutdown_ctx });
    }

    var group: Io.Group = .init;
    defer group.cancel(io);

    while (true) {
        const stream = server.accept(io) catch |err| {
            if (is_shutting_down.load(.monotonic)) break;
            log.err("Failed to accept connection: {t}", .{err});
            continue;
        };

        group.async(io, handleClient, .{ io, gpa, stream });
    }

    log.info("Stopped accepting connections...", .{});
    group.await(io) catch {};
    log.info("All connections closed. Goodbye!", .{});
}

fn watchSigintPipe(io: Io, ctx: ShutdownCtx) Io.Cancelable!void {
    var buf: [1]u8 = undefined;
    var reader = sig_stream[0].reader(io, &buf);

    _ = reader.interface.takeByte() catch |err| return switch (err) {
        error.EndOfStream => {},
        error.ReadFailed => switch (reader.err.?) {
            error.Canceled => error.Canceled,
            else => log.err("SIGINT Watcher died unexpectedly: {t}", .{err}),
        },
    };

    if (!ctx.is_shutting_down.swap(true, .monotonic)) {
        log.info("Caught interrupt. Initiating shutdown...", .{});
        ctx.server.deinit(io);
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
        .resolve_beneath = false, // TODO: not supported by zio, not working in std
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

    {
        const prev_prot = io.swapCancelProtection(.blocked);
        defer _ = io.swapCancelProtection(prev_prot);

        var comp_buf: [std.compress.flate.max_window_len * 4]u8 = undefined;
        var comp = std.compress.flate.Compress.init(
            writer,
            &comp_buf,
            .gzip,
            .default,
        ) catch {
            std.debug.assert(stream_writer.err.? != error.Canceled);
            return;
        };
        defer comp.finish() catch {};
        const comp_writer = &comp.writer;

        const read = file_reader.interface.streamRemaining(comp_writer) catch |err| switch (err) {
            error.ReadFailed => {
                const read_err = file_reader.err.?;
                std.debug.assert(read_err != error.Canceled);
                writer.print("ERROR {t}\n\x00", .{read_err}) catch {};
                log.err("Can't read file {s} for {f}: {}", .{ path, addr, read_err });
                return;
            },
            error.WriteFailed => {
                std.debug.assert(stream_writer.err.? != error.Canceled);
                return;
            },
        };
        log.info("Successfully served {} bytes to {f}", .{ read, addr });
    }
    io.checkCancel() catch return error.Canceled;
}
