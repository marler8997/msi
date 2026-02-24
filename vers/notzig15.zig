pub fn memmove(dest: []u8, src: []const u8) void {
    const len = @min(dest.len, src.len);
    if (@intFromPtr(dest.ptr) <= @intFromPtr(src.ptr)) {
        std.mem.copyForwards(u8, dest[0..len], src[0..len]);
    } else {
        std.mem.copyBackwards(u8, dest[0..len], src[0..len]);
    }
}

const std = @import("std");
