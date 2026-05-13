const std = @import("std");
const Allocator = std.mem.Allocator;
const Io = std.Io;
const IpAddress = Io.net.IpAddress;
const log = std.log;
const builtin = @import("builtin");

const options = @import("options");
const zio = @import("zio");

var single_threaded_io: Io.Threaded = .init_single_threaded;

var sig_io: Io = undefined;
var sig_event: Io.Event = .unset;

fn sigintHandler(_: std.c.SIG) callconv(.c) void {
    sig_event.set(sig_io);
}

pub fn main(init: std.process.Init) !void {
    const gpa = init.gpa;

    const io, const rt = switch (options.io) {
        .zio => blk: {
            const rt = try zio.Runtime.init(gpa, .{});
            break :blk .{ rt.io(), rt };
        },
        .std => .{ init.io, {} },
        .single_threaded => .{ single_threaded_io.io(), {} },
    };
    defer if (@TypeOf(rt) != void) rt.deinit();

    const sig_ok = blk: {
        sig_io = io;
        var sa: std.c.Sigaction = .{
            .handler = .{ .handler = sigintHandler },
            .mask = undefined,
            .flags = 0,
        };
        if (std.c.sigemptyset(&sa.mask) != 0) break :blk false;
        break :blk std.c.sigaction(std.c.SIG.INT, &sa, null) == 0;
    };

    const listen_addr = comptime IpAddress.parseLiteral("0.0.0.0:3000") catch unreachable;
    var server = try listen_addr.listen(io, .{ .reuse_address = true });
    var is_shutting_down: std.atomic.Value(bool) = .init(false);
    defer if (!is_shutting_down.load(.monotonic)) server.deinit(io);
    log.info("Listening on {f}", .{listen_addr});

    var group: Io.Group = .init;
    defer group.cancel(io);

    const MainSelect = union(enum) { accept_loop, signal };
    var buf: [1]MainSelect = undefined;
    var select: Io.Select(MainSelect) = .init(io, &buf);
    log.debug("select.init() returned", .{});
    defer _ = select.cancel();

    select.async(.accept_loop, acceptLoop, .{ io, gpa, &server, &group });
    log.debug("select.async() returned", .{});

    if (sig_ok) {
        log.info("Registering SIGINT handler", .{});
        select.async(.signal, Io.Event.waitUncancelable, .{ &sig_event, io });
    }

    _ = select.await() catch {};
    log.debug("select.await() returned", .{});

    _ = select.cancel();
    log.info("Stopped accepting connections...", .{});

    group.await(io) catch {};
    log.info("All connections closed. Goodbye!", .{});
}

fn acceptLoop(
    io: Io,
    gpa: Allocator,
    server: *Io.net.Server,
    client_group: *Io.Group,
) void {
    while (true) {
        const stream = server.accept(io) catch |err| switch (err) {
            error.Canceled => return,
            else => {
                log.err("Failed to accept connection: {t}", .{err});
                continue;
            },
        };

        client_group.async(io, handleClient, .{ io, gpa, stream });
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
