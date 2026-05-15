const std = @import("std");
const Allocator = std.mem.Allocator;
const Io = std.Io;
const IpAddress = Io.net.IpAddress;
const log = std.log;
const flate = std.compress.flate;
const builtin = @import("builtin");

const options = @import("options");
const zio = @import("zio");

var single_threaded_io: Io.Threaded = .init_single_threaded;
const concurrent = switch (options.io) {
    .zio, .std => !builtin.single_threaded,
    .single_threaded => false,
};

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

    const sig_ok = if (concurrent) blk: {
        sig_io = io;
        var sa: std.c.Sigaction = .{
            .handler = .{ .handler = sigintHandler },
            .mask = undefined,
            .flags = 0,
        };
        if (std.c.sigemptyset(&sa.mask) != 0) break :blk false;
        break :blk std.c.sigaction(std.c.SIG.INT, &sa, null) == 0;
    } else false;

    const listen_addr = comptime IpAddress.parseLiteral("0.0.0.0:3000") catch unreachable;
    var server = try listen_addr.listen(io, .{ .reuse_address = true });
    log.info("Listening on {f}", .{listen_addr});

    var group: Io.Group = .init;
    defer group.cancel(io);

    var accept_loop = io.async(acceptLoop, .{ io, gpa, &server, &group });
    defer accept_loop.cancel(io) catch {};

    if (sig_ok) {
        sig_event.waitUncancelable(io);
        accept_loop.cancel(io) catch {};
        group.cancel(io);
    }

    accept_loop.await(io) catch {};
    log.info("Stopped accepting connections...", .{});

    log.info("Draining active clients...", .{});
    group.await(io) catch {};

    log.info("All connections closed. Goodbye!", .{});
}

fn acceptLoop(
    io: Io,
    gpa: Allocator,
    server: *Io.net.Server,
    client_group: *Io.Group,
) Io.Cancelable!void {
    while (true) {
        const stream = server.accept(io) catch |err| switch (err) {
            error.Canceled => return error.Canceled,
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

    var read_buf: [Io.Dir.max_path_bytes]u8 = undefined;
    var write_buf: [1024]u8 = undefined;
    var sr = stream.reader(io, &read_buf);
    var sw = stream.writer(io, &write_buf);
    const sri = &sr.interface;
    const swi = &sw.interface;

    const err = while (true) {
        defer swi.flush() catch {};

        const path = sri.takeDelimiter(0) catch |err| switch (err) {
            error.ReadFailed => break sr.err.?,
            error.StreamTooLong => {
                swi.writeAll("ERROR PathTooLong\n\x00") catch break sw.err.?;
                continue;
            },
        } orelse return; // Client can disconnect silently

        log.info("Client {f} requested {s}", .{ addr, path });

        var file = Io.Dir.cwd().openFile(io, path, .{
            .allow_directory = false,
            .resolve_beneath = false, // TODO: not supported by zio, not working in std
        }) catch |err| {
            swi.print("ERROR {t}\n\x00", .{err}) catch break sw.err.?;
            continue;
        };
        defer file.close(io);

        var file_buf: [1024]u8 = undefined;
        var fr = file.reader(io, &file_buf);
        const fri = &fr.interface;

        {
            const prev_prot = io.swapCancelProtection(.blocked);
            defer _ = io.swapCancelProtection(prev_prot);

            var buf: [flate.max_window_len]u8 = undefined;
            var comp = flate.Compress.init(swi, &buf, .gzip, .default) catch break sw.err.?;

            const read = fri.streamRemaining(&comp.writer) catch |err| switch (err) {
                error.ReadFailed => {
                    swi.print("ERROR {t}\n\x00", .{fr.err.?}) catch break sw.err.?;
                    continue;
                },
                error.WriteFailed => break sw.err.?,
            };

            comp.finish() catch break sw.err.?;
            swi.flush() catch break sw.err.?;

            log.info("Successfully served {} bytes to {f}", .{ read, addr });
        }
        io.checkCancel() catch |err| break err;
    };

    switch (err) {
        // Cancelation by e.g. SIGINT
        error.Canceled => return error.Canceled,
        // Ignore disconnects
        error.SocketUnconnected,
        error.ConnectionResetByPeer,
        => {},
        // Log serious errors
        else => log.err("Client {f} fatal error {t}", .{ addr, err }),
    }
}
