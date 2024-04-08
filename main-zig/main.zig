const std = @import("std");
const fs = @import("std").fs;
const heap = @import("std").heap;
const assert = @import("std").debug.assert;

const State = struct {
    min: i64,
    max: i64,
    sum: i64,
    count: i64,
};

const FastStringContext = struct {
    pub fn hash(self: FastStringContext, key: []const u8) u64 {
        _ = self;
        var res: u64 = 0;
        res += @as(u64, key[0]) << 32;
        res += @as(u64, key[1]) << 16;
        res += @as(u64, key[2]) << 8;
        res += @as(u64, key.len);
        return res;
    }

    pub fn eql(self: FastStringContext, a: []const u8, b: []const u8) bool {
        _ = self;
        return std.mem.eql(u8, a, b);
    }
};

const FastStringHashMap = std.HashMap([]const u8, u64, std.hash_map.StringContext, 80);
const Chunk = struct {
    addr: []u8,
    eof: bool,
    name_map: FastStringHashMap,
    state_map: []State,
    main_allocator: heap.ArenaAllocator
};


fn brc_float_parse(buffer: []u8) i64 {
    // from 1brc:  Temperature value: non null double between -99.9 (inclusive) and 99.9 (inclusive), always with one fractional digit
    var res: i64 = 0;
    var negative = false;
    var offset: usize = 0;
    if (buffer[0] == '-') {
        offset += 1;
        negative = true;
    }

    while (offset < buffer.len) {
        if (buffer[offset] == '.') {
            offset += 1;
            continue;
        }
        res *= 10;
        res += (buffer[offset] - '0');
        offset += 1;
    }
    return res;
}

fn process_thread(chunk: *Chunk) !void {
    var chunk_offset: usize = 0;
    var name_id: usize = 0;
    while (chunk_offset < chunk.addr.len) {
        var semi_colon = chunk_offset;
        while (chunk.addr[semi_colon] != ';') {
            semi_colon += 1;
        }
        var jump_line = semi_colon;
        while (jump_line < chunk.addr.len and chunk.addr[jump_line] != '\n') {
            jump_line += 1;
        }
        var name = chunk.addr[chunk_offset..semi_colon];
        var value = brc_float_parse(chunk.addr[semi_colon + 1 .. jump_line]);
        var id = chunk.name_map.get(name);
        if (id == null) {
            try chunk.name_map.put(name, name_id);
            var state = &chunk.state_map[name_id];
            state.min = std.math.maxInt(i64);
            state.max = std.math.minInt(i64);
            state.count = 0;
            state.sum = 0;

            id = name_id;
            name_id += 1;
        }
        var state = &chunk.state_map[id.?];
        state.min = @min(state.min, value);
        state.max = @max(state.max, value);
        state.count += 1;
        assert(std.math.maxInt(i64) - state.sum > value);
        state.sum += value;

        chunk_offset = jump_line + 1;
    }
}

pub fn main() anyerror!void {
    var dir = fs.cwd();
    var file = try dir.openFile("measurements-pere.txt", .{});

    var offset: u64 = 0;
    _ = offset;
    const buffer_size = 1024 * 1024;
    _ = buffer_size;

    var main_arena = heap.ArenaAllocator.init(heap.page_allocator);
    var allocator = main_arena.allocator();
    defer main_arena.deinit();
    var file_stat = try file.stat();
    var file_start_address: *u8 = @ptrCast(std.c.mmap(null, file_stat.size, std.os.PROT.READ, std.os.MAP.SHARED, file.handle, 0));
    var file_start_address_slice: []u8 = std.mem.asBytes(file_start_address);
    file_start_address_slice.len = file_stat.size;
    var chunks = std.ArrayListUnmanaged(*Chunk){};
    var threads = std.ArrayListUnmanaged(std.Thread){};
    const num_threads = try std.Thread.getCpuCount();

    {
        // create chunks
        var chunk_offset: usize = 0;
        const chunk_size = file_stat.size / num_threads;
        while (chunk_offset < file_stat.size) {
            var amount = @min(chunk_size, file_stat.size - chunk_offset);
            while (file_start_address_slice[chunk_offset+amount-1] != '\n') {
                amount -= 1;
                // TODO: missing eof
            }
            var chunk_arena = heap.ArenaAllocator.init(heap.page_allocator);
            var chunk_allocator = chunk_arena.allocator();
            var chunk = &(try allocator.alloc(Chunk, 1))[0];
            chunk.addr = file_start_address_slice[chunk_offset .. chunk_offset + amount];
            chunk.eof = (chunk_offset + amount) == file_stat.size;
            chunk.name_map = FastStringHashMap.init(chunk_allocator);
            chunk.state_map = try chunk_allocator.alloc(State, 10000);
            assert(chunk.addr[chunk.addr.len-1] == '\n' or chunk.eof);
            chunk.main_allocator = chunk_arena;
            try chunks.append(allocator, chunk);

            chunk_offset += amount;
            var thread = try std.Thread.spawn(.{}, process_thread, .{chunk});
            std.debug.print("chunk {d} {}\n", .{ chunk.addr[0], chunk.eof });
            try threads.append(allocator, thread);
        }
    }

    for (threads.items) |thread| {
        thread.join();
    }
    var name_map = std.StringHashMap(u64).init(allocator);
    _ = name_map;
    var state_map = try allocator.alloc(State, 10000);
    _ = state_map;
    for (chunks.items) |chunk| {
        _ = chunk;
        std.debug.print("chunk \n", .{});
    }


}

test "basic test" {
    try std.testing.expectEqual(10, 3 + 7);
}
