const std = @import("std");
const Allocator = std.mem.Allocator;
const Io = std.Io;
const IpAddress = Io.net.IpAddress;
const log = std.log;
const flate = std.compress.flate;
const linux = std.os.linux;
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

    var cache: FileCache = .init(gpa);
    defer cache.deinit(io);

    var invalidator = switch (builtin.os.tag) {
        .linux => io.concurrent(cacheInvalidatorLinux, .{ io, &cache }) catch null,
        else => null,
    };
    if (invalidator == null) log.warn("No file watching supported. Expect stale data.", .{});
    defer if (invalidator) |*inv| inv.cancel(io) catch {};

    var group: Io.Group = .init;
    defer group.cancel(io);

    var accept_loop = io.async(acceptLoop, .{ io, &cache, &server, &group });
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
    cache: *FileCache,
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

        client_group.async(io, handleClient, .{ io, cache, stream });
    }
}

fn handleClient(
    io: Io,
    cache: *FileCache,
    stream: Io.net.Stream,
) Io.Cancelable!void {
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

        if (path.len == 0) return; // Graceful session termination

        log.info("Client {f} requested {s}", .{ addr, path });

        {
            const prev_prot = io.swapCancelProtection(.blocked);
            defer _ = io.swapCancelProtection(prev_prot);

            const data = cache.get(io, path) catch |err| {
                if (err == error.Canceled) break err;
                swi.print("ERROR {t}\n\x00", .{err}) catch break sw.err.?;
                continue;
            };

            swi.writeAll(data) catch break sw.err.?;
            swi.flush() catch break sw.err.?;

            log.info("Successfully served {} bytes to {f}", .{ data.len, addr });
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

const FileCache = struct {
    lock: Io.RwLock,
    gpa: Allocator,
    map: std.StringHashMapUnmanaged([]const u8),

    pub const Error = Allocator.Error || Io.File.OpenError || Io.File.Reader.Error;

    pub fn init(gpa: Allocator) FileCache {
        return .{
            .lock = .init,
            .gpa = gpa,
            .map = .empty,
        };
    }

    /// Safe to call only after all users have finished or been canceled
    pub fn deinit(self: *FileCache, io: Io) void {
        self.lock.lockUncancelable(io);
        defer self.lock.unlock(io);

        var iterator = self.map.iterator();
        while (iterator.next()) |entry| {
            self.gpa.free(entry.key_ptr.*);
            self.gpa.free(entry.value_ptr.*);
        }
        self.map.deinit(self.gpa);
    }

    pub fn get(self: *FileCache, io: Io, path: []const u8) Error![]const u8 {
        {
            try self.lock.lockShared(io);
            defer self.lock.unlockShared(io);
            if (self.map.get(path)) |data| {
                return data;
            }
        }

        try self.lock.lock(io);
        defer self.lock.unlock(io);

        var file = try Io.Dir.cwd().openFile(io, path, .{
            .allow_directory = false,
            .resolve_beneath = false, // TODO: not supported by zio, not working in std
        });
        defer file.close(io);

        var file_buf: [1024]u8 = undefined;
        var file_reader = file.reader(io, &file_buf);
        const fri = &file_reader.interface;

        var allocating: Io.Writer.Allocating = .init(self.gpa);
        defer allocating.deinit();
        try allocating.ensureUnusedCapacity(64);

        var comp_buf: [flate.max_window_len]u8 = undefined;
        var comp = flate.Compress.init(
            &allocating.writer,
            &comp_buf,
            .gzip,
            .default,
        ) catch return error.OutOfMemory;

        _ = fri.streamRemaining(&comp.writer) catch |e| switch (e) {
            error.ReadFailed => return file_reader.err.?,
            error.WriteFailed => return error.OutOfMemory,
        };
        comp.finish() catch return error.OutOfMemory;

        const key = try self.gpa.dupe(u8, path);
        errdefer self.gpa.free(key);

        const data = try allocating.toOwnedSlice();
        errdefer self.gpa.free(data);

        try self.map.put(self.gpa, key, data);
        return data;
    }

    pub fn invalidate(self: *FileCache, io: Io, path: []const u8) void {
        self.lock.lockUncancelable(io);
        defer self.lock.unlock(io);

        if (self.map.fetchRemove(path)) |entry| {
            self.gpa.free(entry.key);
            self.gpa.free(entry.value);
            log.info("Cache invalidated for: {s}", .{path});
        }
    }
};

fn cacheInvalidatorLinux(io: Io, cache: *FileCache) Io.Cancelable!void {
    const inotify = linux.inotify_init1(linux.IN.CLOEXEC);
    switch (linux.errno(inotify)) {
        .SUCCESS => {},
        else => {
            log.err("inotify_init1 failed", .{});
            return;
        },
    }
    const inotify_fd: linux.fd_t = @intCast(inotify);

    var inotify_file = Io.File{
        .handle = inotify_fd,
        .flags = .{ .nonblocking = false },
    };
    defer inotify_file.close(io);

    const watch = linux.inotify_add_watch(
        inotify_fd,
        ".",
        linux.IN.MODIFY | linux.IN.MOVED_TO | linux.IN.DELETE,
    );
    switch (linux.errno(watch)) {
        .SUCCESS => {},
        else => {
            log.err("inotify_add_watch failed", .{});
            return;
        },
    }

    var buf: [@sizeOf(linux.inotify_event) + Io.Dir.max_path_bytes + 1]u8 = undefined;

    while (true) {
        const bytes_read = inotify_file.readStreaming(io, &.{&buf}) catch |err| switch (err) {
            error.Canceled => return error.Canceled,
            else => {
                log.err("Inotify read failed: {t}", .{err});
                return;
            },
        };

        var i: usize = 0;
        while (i < bytes_read) {
            const event: *linux.inotify_event = @ptrCast(@alignCast(&buf[i]));

            if (event.len > 0) {
                const name_start = i + @sizeOf(linux.inotify_event);
                const name_ptr: [*:0]const u8 = @ptrCast(&buf[name_start]);
                const name = std.mem.span(name_ptr);
                log.debug("inotify event: {s}", .{name});
                cache.invalidate(io, name);
            }

            i += @sizeOf(linux.inotify_event) + event.len;
        }
    }
}
