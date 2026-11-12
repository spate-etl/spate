**The CPU features line at start** (`spate-core`)

The first pipeline to start in a process logs one `INFO` line under the message `cpu features`. It names the CPU architecture, the SIMD features the build compiles in, and the SIMD features the process can use on the host. The parsing and decompression libraries a pipeline uses choose their SIMD code from those features at run time, and a virtual machine can hide features that its CPU has. A feature the build compiles in is always reported as usable, without asking the CPU.
