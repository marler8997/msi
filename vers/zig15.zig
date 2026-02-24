pub fn memmove(dest: []u8, src: []const u8) void {
    @memmove(dest, src);
}
