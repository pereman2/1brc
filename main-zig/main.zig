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

const FastStringHashMap = std.StringHashMap(u64);
const Chunk = struct {
    addr: []u8,
    eof: bool,
    name_map: *FastStringHashMap,
    state_map: []State,
    main_allocator: *heap.ArenaAllocator,
    id: u64
};

fn compare_string(context: void, lhs: []const u8, rhs: []const u8) bool {
    _ = context;
    return std.mem.lessThan(u8, lhs, rhs);
}


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
    if (negative) {
        res *= -1;
    }
    return res;
}

fn process_thread(chunk: *Chunk) void {
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
            // var name_copy = chunk.main_allocator.allocator().alloc(u8, name.len) catch |err| {
            //     std.debug.print("Error allocating {}\n", .{err});
            //     std.os.exit(1);
            // };
            // std.mem.copyForwards(u8, name_copy, name);
            // std.debug.print("adding {d} {s} {d}\n", .{chunk.id, name, name_id});
            chunk.name_map.put(name, name_id) catch |err| {
                std.debug.print("Error adding name {}\n", .{err});
            };
            var state = &chunk.state_map[name_id];
            state.min = std.math.maxInt(i64);
            state.max = std.math.minInt(i64);
            state.count = 0;
            state.sum = 0;

            id = name_id;
            name_id += 1;
        }
        assert(id != null);
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

    var main_arena = heap.ArenaAllocator.init(heap.page_allocator);
    var allocator = main_arena.allocator();
    var file_stat = try file.stat();
    var file_start_address: *u8 = @ptrCast(std.c.mmap(null, file_stat.size, std.os.PROT.READ, std.os.MAP.SHARED, file.handle, 0));
    var file_start_address_slice: []u8 = std.mem.asBytes(file_start_address);
    file_start_address_slice.len = file_stat.size;
    const num_threads = 32;
    var chunks = std.ArrayList(Chunk).init(allocator);
    var threads = std.ArrayList(std.Thread).init(allocator);
    std.debug.print("num_threads: {d}\n", .{num_threads});

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
            assert(chunk_offset < chunk_offset + amount);
            var chunk_arena = try allocator.create(heap.ArenaAllocator);
            chunk_arena.* = heap.ArenaAllocator.init(heap.page_allocator);
            var chunk_allocator = chunk_arena.allocator();
            var fast_map: *FastStringHashMap = try chunk_allocator.create(FastStringHashMap);
            fast_map.* = FastStringHashMap.init(chunk_allocator);
            var chunk = Chunk{
                .addr = file_start_address_slice[chunk_offset .. chunk_offset + amount],
                .eof = (chunk_offset + amount) == file_stat.size,
                .name_map = fast_map,
                .state_map = try chunk_allocator.alloc(State, 10000),
                .main_allocator = chunk_arena,
                .id = chunks.items.len

            };
            assert(chunk.addr[chunk.addr.len-1] == '\n' or chunk.eof);
            try chunks.append(chunk);
            chunk_offset += amount;

        }
    }

    for (0..num_threads) |thread_id| {

        var last_chunk = &chunks.items[thread_id];
        try threads.append(try std.Thread.spawn(.{}, process_thread, .{last_chunk}));
    }

    for (threads.items) |thread| {
        thread.join();
    }
    var name_state_map = std.StringHashMap(State).init(allocator);
    var names = std.ArrayList([]const u8).init(allocator);
    for (chunks.items) |chunk| {
        var iterator = chunk.name_map.iterator();
        while (iterator.next()) |entry| {
            var key_ptr: *[]const u8 = entry.key_ptr;
            var value_ptr: *u64 = entry.value_ptr;
            var state = &chunk.state_map[value_ptr.*];
            if (!name_state_map.contains(key_ptr.*)) {
                try name_state_map.put(key_ptr.*, State{ .min = std.math.maxInt(i64), .max=std.math.minInt(i64), .count = 0, .sum = 0});
                try names.append(key_ptr.*);
            }
            var other_state = name_state_map.getPtr(key_ptr.*).?;
            other_state.min = @min(other_state.min, state.min);
            other_state.max = @max(other_state.max, state.max);
            other_state.count += state.count;
            other_state.sum += state.sum;
        }
    }
    std.sort.insertion([]const u8, names.items, {}, compare_string);
    std.debug.print("{{", .{});
    for (names.items) |name| {
        var state = name_state_map.getPtr(name).?;

        std.debug.print("{s}={d}/{d}/{d}, ",  .{
            name,
            (@as(f64, @floatFromInt(state.min)) / 10), 
            (@as(f64, @floatFromInt(state.max)) / 10), 
            (@as(f64, @floatFromInt(state.sum)) / 10) / @as(f64, @floatFromInt(state.count))
        });
    }
    std.debug.print("}}", .{});

}

test "basic test" {
    try std.testing.expectEqual(10, 3 + 7);
}
