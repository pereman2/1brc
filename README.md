[go source](main.go)
[zig source](main-zig/main.zig)




zig:
```bash
real    0m1.495s
user    1m15.188s
sys     0m0.799s
```

architecture: 64 chunked mmap parser threads
golang:

```bash
real    0m5.495s
user    1m44.097s
sys     0m5.780s # as exepected from read syscalls
```
architecture: 1 sequential fread thread + 64 parser threads
