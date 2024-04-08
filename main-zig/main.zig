const std = @import("std");
const fs = @import("std").fs;
const heap = @import("std").heap;

pub fn main() anyerror!void {
    var dir = fs.cwd();
    var file = try dir.openFile("measurements-pere.txt", .{});

    var offset: u64 = 0;
    _ = offset;
    const buffer_size = 1024 * 1024;
    _ = buffer_size;

    var main_arena = heap.ArenaAllocator.init(heap.page_allocator);
    var allocator = main_arena.allocator();
    var buf = try allocator.alloc(u8, buffer_size);
    var file_start_address: *u8 = @ptrCast(*u8, std.c.mmap(null, 1024 * 1024 * 1024 * 14, std.os.PROT.READ, std.os.MAP.SHARED, file.handle, 0));
    _ = file_start_address;
    while (true) {
        var n = file.read(buf) catch 0;
        if (n == 0) {
            break;
        }
        std.log.debug("read {d}", .{n});
    }
}

test "basic test" {
    try std.testing.expectEqual(10, 3 + 7);
}
