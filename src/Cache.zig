const std = @import("std");
const Allocator = std.mem.Allocator;
const Io = std.Io;
const flate = std.compress.flate;
const linux = std.os.linux;
const log = std.log;
const builtin = @import("builtin");

lock: Io.RwLock,
gpa: Allocator,
map: std.StringHashMapUnmanaged([]const u8),
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
    const cache = try gpa.create(Cache);
    cache.* = .{
        .lock = .init,
        .gpa = gpa,
        .map = .empty,
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
    self.lock.lockUncancelable(io);
    defer self.lock.unlock(io);

    self.invalidator.deinit(io);

    var iterator = self.map.iterator();
    while (iterator.next()) |entry| {
        const key_sentinel: [:0]const u8 = @ptrCast(entry.key_ptr.*);
        self.gpa.free(key_sentinel);
        self.gpa.free(entry.value_ptr.*);
    }
    self.map.deinit(self.gpa);

    self.gpa.destroy(self);
}

pub fn get(self: *Cache, io: Io, path: []const u8) Error![]const u8 {
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

    const key = try self.gpa.dupeSentinel(u8, path, 0);
    errdefer self.gpa.free(key);

    const data = try allocating.toOwnedSlice();
    errdefer self.gpa.free(data);

    try self.map.put(self.gpa, key, data);

    self.invalidator.addWatch(key);
    return data;
}

pub fn invalidate(self: *Cache, io: Io, path: []const u8) void {
    self.lock.lockUncancelable(io);
    defer self.lock.unlock(io);

    if (self.map.fetchRemove(path)) |entry| {
        const key_sentinel: [:0]const u8 = @ptrCast(entry.key);
        log.info("Cache invalidated for: {s}", .{path});
        self.gpa.free(key_sentinel);
        self.gpa.free(entry.value);
    }
}

const InvalidatorLinux = struct {
    inotify_fd: linux.fd_t,
    map: std.AutoHashMapUnmanaged(c_int, [:0]const u8),
    task: ?Io.Future(Io.Cancelable!void),

    pub const Error = error{InotifyInitFailed};

    pub fn init() @This().Error!InvalidatorLinux {
        const inotify = linux.inotify_init1(linux.IN.CLOEXEC);
        switch (linux.errno(inotify)) {
            .SUCCESS => {},
            else => return error.InotifyInitFailed,
        }
        const fd: linux.fd_t = @intCast(inotify);
        errdefer _ = linux.close(fd);

        return .{
            .inotify_fd = fd,
            .map = .empty,
            .task = null,
        };
    }

    pub fn start(self: *InvalidatorLinux, io: Io) Io.ConcurrentError!void {
        const cache: *Cache = @fieldParentPtr("invalidator", self);
        self.task = try io.concurrent(worker, .{ io, cache });
    }

    pub fn deinit(self: *InvalidatorLinux, io: Io) void {
        const cache: *Cache = @fieldParentPtr("invalidator", self);
        if (self.task) |*fut| fut.cancel(io) catch {};
        self.map.deinit(cache.gpa);
        _ = linux.close(self.inotify_fd);
    }

    fn addWatch(self: *InvalidatorLinux, path: [:0]const u8) void {
        const cache: *Cache = @fieldParentPtr("invalidator", self);

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

        self.map.put(cache.gpa, wd, path) catch {
            log.err("inotify map allocation failed", .{});
            _ = linux.inotify_rm_watch(self.inotify_fd, wd);
        };
    }

    fn worker(io: Io, cache: *Cache) Io.Cancelable!void {
        const self = &cache.invalidator;
        const file = Io.File{
            .handle = self.inotify_fd,
            .flags = .{ .nonblocking = false },
        };
        defer file.close(io);

        var buf: [@sizeOf(linux.inotify_event) * 4]u8 = undefined;

        while (true) {
            const bytes_read = file.readStreaming(io, &.{&buf}) catch |err| switch (err) {
                error.Canceled => return error.Canceled,
                else => {
                    log.err("Inotify read failed: {t}", .{err});
                    return;
                },
            };

            var i: usize = 0;
            while (i < bytes_read) {
                const event: *linux.inotify_event = @ptrCast(@alignCast(&buf[i]));

                if (event.wd >= 0) {
                    const path = self.map.get(event.wd) orelse {
                        log.err("inotify returned unknown wd", .{});
                        continue;
                    };
                    cache.invalidate(io, path);
                }

                i += @sizeOf(linux.inotify_event) + event.len;
            }
        }
    }
};

const InvalidatorNone = struct {
    pub const Error = error{};
    pub fn init() @This().Error!@This() {
        log.warn("No file watching supported. Expect stale data.", .{});
        return .{};
    }
    pub fn start(_: *@This(), _: Io) Io.ConcurrentError!void {}
    pub fn deinit(_: *@This(), _: Io) void {}
    pub fn addWatch(_: *@This(), _: [:0]const u8) void {}
};
