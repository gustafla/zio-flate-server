const std = @import("std");
const Allocator = std.mem.Allocator;
const Io = std.Io;
const flate = std.compress.flate;
const linux = std.os.linux;
const log = std.log;
const builtin = @import("builtin");

lock: Io.RwLock,
gpa: Allocator,
data_map: std.StringHashMapUnmanaged([]const u8),
invalidator: Invalidator,

const Cache = @This();

const Invalidator = switch (builtin.os.tag) {
    .linux => InvalidatorLinux,
    else => InvalidatorNone,
};

pub const Error = Allocator.Error ||
    Io.File.OpenError ||
    Io.File.Reader.Error ||
    Invalidator.Error;

pub fn init(io: Io, gpa: Allocator) Error!*Cache {
    log.debug("Cache.init", .{});
    const cache = try gpa.create(Cache);
    cache.* = .{
        .lock = .init,
        .gpa = gpa,
        .data_map = .empty,
        .invalidator = try Invalidator.init(),
    };
    cache.invalidator.start(io) catch {
        log.warn("No file watching supported. Expect stale data.", .{});
        cache.invalidator.deinit(io);
    };

    return cache;
}

/// Safe to call only after all users have finished or been canceled
pub fn deinit(self: *Cache, io: Io) void {
    log.debug("Cache.deinit", .{});
    self.takeLockUncancelable(io);
    defer self.unlock(io);

    self.invalidator.deinit(io);

    var iterator = self.data_map.iterator();
    while (iterator.next()) |entry| {
        const key_sentinel: [:0]const u8 = @ptrCast(entry.key_ptr.*);
        self.gpa.free(key_sentinel);
        self.gpa.free(entry.value_ptr.*);
    }
    self.data_map.deinit(self.gpa);

    self.gpa.destroy(self);
}

pub fn get(self: *Cache, io: Io, path: []const u8) Error![]const u8 {
    log.debug("Cache.get", .{});
    {
        try self.takeLockShared(io);
        defer self.unlockShared(io);
        if (self.data_map.get(path)) |data| {
            log.debug("Cache.get fast path return", .{});
            return data;
        }
    }

    try self.takeLock(io);
    defer self.unlock(io);

    if (self.data_map.get(path)) |data| {
        log.debug("Cache.get double-check return", .{});
        return data;
    }

    const file = try Io.Dir.cwd().openFile(io, path, .{
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

    const key = try self.gpa.dupeSentinel(u8, path, 0);
    errdefer self.gpa.free(key);

    const data = try allocating.toOwnedSlice();
    errdefer self.gpa.free(data);

    try self.data_map.put(self.gpa, key, data);
    self.invalidator.addWatch(key);

    log.debug("Cache.get slow path return", .{});
    return data;
}

const InvalidatorLinux = struct {
    inotify_fd: linux.fd_t,
    path_map: std.AutoHashMapUnmanaged(c_int, [:0]const u8),
    task: ?Io.Future(Io.Cancelable!void),

    pub const Error = error{InotifyInitFailed};

    pub fn init() @This().Error!InvalidatorLinux {
        log.debug("InvalidatorLinux.init", .{});
        const inotify = linux.inotify_init1(linux.IN.CLOEXEC);
        switch (linux.errno(inotify)) {
            .SUCCESS => {},
            else => return error.InotifyInitFailed,
        }
        const fd: linux.fd_t = @intCast(inotify);
        errdefer _ = linux.close(fd);

        return .{
            .inotify_fd = fd,
            .path_map = .empty,
            .task = null,
        };
    }

    pub fn start(self: *InvalidatorLinux, io: Io) Io.ConcurrentError!void {
        log.debug("InvalidatorLinux.start", .{});
        self.task = try io.concurrent(worker, .{ io, self });
    }

    /// This may only be called when parent cache lock is held.
    /// Indempotent.
    pub fn deinit(self: *InvalidatorLinux, io: Io) void {
        log.debug("InvalidatorLinux.deinit", .{});
        const cache: *Cache = @fieldParentPtr("invalidator", self);

        if (self.inotify_fd < 0) return;

        if (self.task) |*task| task.cancel(io) catch {};
        self.task = null;
        self.path_map.deinit(cache.gpa);
        // Already closed by worker cancelation
        self.inotify_fd = -1;
    }

    /// This may only be called when parent cache lock is held.
    fn addWatch(self: *InvalidatorLinux, path: [:0]const u8) void {
        log.debug("InvalidatorLinux.addWatch", .{});
        const cache: *Cache = @fieldParentPtr("invalidator", self);

        if (self.inotify_fd < 0) return;

        const watch = linux.inotify_add_watch(
            self.inotify_fd,
            path.ptr,
            linux.IN.MODIFY | linux.IN.MOVED_TO | linux.IN.DELETE,
        );
        switch (linux.errno(watch)) {
            .SUCCESS => {},
            else => {
                log.err("inotify_add_watch failed", .{});
                return;
            },
        }
        const wd: c_int = @intCast(watch);

        self.path_map.put(cache.gpa, wd, path) catch {
            log.err("inotify map allocation failed", .{});
            _ = linux.inotify_rm_watch(self.inotify_fd, wd);
        };
    }

    fn worker(io: Io, self: *InvalidatorLinux) Io.Cancelable!void {
        log.debug("InvalidatorLinux.worker", .{});
        const cache: *Cache = @fieldParentPtr("invalidator", self);
        const file: Io.File = .{
            .handle = self.inotify_fd,
            .flags = .{ .nonblocking = false },
        };
        defer file.close(io);

        const event_size = @sizeOf(linux.inotify_event);
        const buf_size = event_size + Io.Dir.max_path_bytes;
        var buffer: [buf_size]u8 = undefined;

        const err = while (true) {
            log.debug("InvalidatorLinux.worker calling readStreaming", .{});
            const read = file.readStreaming(io, &.{&buffer}) catch |e| break e;

            var i: usize = 0;
            while (i < read) {
                const slice = buffer[i..event_size];
                const event = std.mem.bytesAsValue(linux.inotify_event, slice);
                i += event_size;
                log.debug("{any}", .{event});
                i += event.len;

                if (event.wd < 0) continue;

                // This prevents addWatch and deinit from being called and protects
                // path_map from concurrent access
                try cache.takeLock(io);
                defer cache.unlock(io);

                const path = self.path_map.fetchRemove(event.wd) orelse continue;
                const data = cache.data_map.fetchRemove(path.value) orelse continue;

                log.info("Cache invalidated for {s}", .{data.key});
                cache.gpa.free(@as([:0]const u8, @ptrCast(data.key)));
                cache.gpa.free(data.value);
                _ = linux.inotify_rm_watch(self.inotify_fd, event.wd);
            }
        };

        if (err == error.Canceled) return error.Canceled;
        log.err("Inotify read failed: {t}", .{err});
    }
};

fn takeLockUncancelable(self: *Cache, io: Io) void {
    log.debug("Cache.takeLockUncancelable", .{});
    self.lock.lockUncancelable(io);
}

fn takeLock(self: *Cache, io: Io) Io.Cancelable!void {
    log.debug("Cache.takeLock", .{});
    try self.lock.lock(io);
}

fn unlock(self: *Cache, io: Io) void {
    log.debug("Cache.unlock", .{});
    self.lock.unlock(io);
}

fn takeLockShared(self: *Cache, io: Io) Io.Cancelable!void {
    log.debug("Cache.takeLockShared", .{});
    try self.lock.lockShared(io);
}

fn unlockShared(self: *Cache, io: Io) void {
    log.debug("Cache.unlockShared", .{});
    self.lock.unlockShared(io);
}

const InvalidatorNone = struct {
    pub const Error = error{};
    pub fn init() @This().Error!@This() {
        log.warn(
            "File notifications not implemented on {t}. Expect stale data.",
            .{builtin.os.tag},
        );
        return .{};
    }
    pub fn start(_: *@This(), _: Io) Io.ConcurrentError!void {}
    pub fn deinit(_: *@This(), _: Io) void {}
    pub fn addWatch(_: *@This(), _: [:0]const u8) void {}
};
