---
title: Running DOOM inside ClickHouse
description: "DOOM in ClickHouse two ways: the 1993 engine on a RISC-V CPU written in SQL, and the engine's own simulation and renderer rewritten as SQL."
authors: [marcus]
image: /img/clickdoom-social.png
---

ClickDOOM runs DOOM inside ClickHouse, two ways. The first executes the real
1993 engine on a RISC-V CPU written in SQL: fetch, decode, execute, registers,
RAM and memory-mapped I/O, all of it inside the database. The second throws the
CPU away and rewrites the engine's own simulation and renderer as SQL, checked
against the first frame by frame. This post is about what each one costs, and
about what I learned from ClickHouse by asking it to do something it was never
built for.

<!-- truncate -->

![A DOOM screenshot: the player firing a shotgun at an imp in a stone corridor, a second enemy to the right, muzzle flash lighting the scene, status bar showing 10 ammo and 100 percent health](/img/clickdoom-frame220.png)

*Frame 220 of `-timedemo demo3`, after 221,639,724 instructions. Every pixel
computed inside ClickHouse, read back out by a `SELECT`, and hashed to
`aa27f0470c7c5f3a`, the same value the reference emulator produces from its own
independent run.*

## Two ways to put DOOM in a database

There is an established genre of DOOM-in-a-database projects, and it is a good
one: DOOMHouse, DOOMQL and DuckDB-DOOM all build something DOOM-shaped out of
queries. ClickDOOM is not that. Both of its modes run the actual engine, and the
whole project is arranged around being able to prove it.

**Emulation mode runs the real binary.** DOOM's C source, through the
doomgeneric port, compiles unmodified to bare-metal RV32IM. That binary then
executes one instruction at a time on a CPU emulator written in ClickHouse SQL.
The lineage here is [Click-V](https://github.com/SpencerTorres/Click-V), which
proved a RISC-V CPU can live inside ClickHouse. ClickDOOM builds a different
execution engine and points it at a much larger program.

**Native mode runs the same game logic, written as SQL.** DOOM's tic simulation
and its software renderer, function for function, as ClickHouse queries. Not a
reimplementation that looks like DOOM. The engine's own arithmetic, its own
random number table, its own BSP traversal and its own clipping rules, held to
the real thing bit for bit.

Native mode is checked against emulation mode. The emulator runs the actual
engine, so it can say what the real game state is at any tic and what any
frame's bytes are. Every row and every frame native mode produces is compared
against that.

A document called `PURITY.md` defines what "in SQL" is allowed to mean, and it
was committed before the first line of implementation so that it could not drift
to fit the code. It rules out user-defined functions, the `executable` and
`python` table functions, and any other way of handing computation to a
subprocess. It also rules out precomputing anything outside the database. The
driver program is allowed to issue queries, insert raw bytes, and copy pixels to
a window. It is not allowed to compute, "however trivial it looks."

## A CPU in a fold

ClickHouse has no loop. It has exactly one construct that threads a value from
one element of an array to the next, and that is `arrayFold`.

So the CPU is one `arrayFold` over `range(K)`. The accumulator carries the whole
machine: the program counter, the 31 general-purpose registers as an array, the
retired instruction count, and the pending memory writes. One batch is one
`INSERT ... SELECT` that executes up to K instructions, and the project runs at
K = 60,000.

Memory is the part that does not fit. RAM is 24 MiB, and ClickHouse arrays are
immutable, so mutating RAM inside the accumulator would copy 24 MiB on every
store. Instead RAM enters the query as a captured constant, which is read with
`arrayElement` and never written, and stores append to a small write log carried
in the accumulator. A load checks the log in reverse order first and falls back
to the constant. At the end of the batch the log merges into a
`ReplacingMergeTree` keyed by word address.

**The version column is each store's own instruction count.** Using the batch's
would give two stores to the same address inside one batch the same version, and
a `ReplacingMergeTree` tie has an unspecified winner. That is the kind of bug
that produces a wrong pixel four hours into a run.

**Decoding happens inside the database.** The text segment is turned into a
table of decoded fields by a SQL query over RAM at load time. This is worth 7.4x
against decoding inside the fold, and doing it outside ClickHouse and inserting
the result would break the purity rules. The fastest option was also the only
legal one.

There is a memory map, and it is small:

| Region | Base | Size |
| --- | --- | --- |
| RAM | `0x80000000` | 24 MiB |
| Memory-mapped I/O | `0x10000000` | 4 KiB |
| Framebuffer | `0x11000000` | 64,000 bytes |
| Palette | `0x11010000` | 768 bytes |

The framebuffer is 320 by 200 at 8 bits per pixel, palette-indexed, rather than
doomgeneric's default of 32 bits. That is four times fewer store instructions
per frame, and on a CPU this slow the store count is the budget.

`TICKS_MS` is the retired instruction count divided by a constant, so the
virtual machine runs at a virtual 10 MHz no matter how fast the host is. A
timedemo therefore produces identical frames whether the emulator manages a
thousand instructions a second or a million. Nothing on a computation path is
allowed to read a clock.

My favorite thing I learned from this mode has nothing to do with ClickHouse. A
successful `-timedemo demo3` run exits with code 4,294,967,295, which is
`(uint32_t)-1`. It looks like a failure and it is not. The completion path for a
timedemo is not the ordinary quit path: it is `G_CheckDemoStatus` calling
`I_Error` directly, which falls through `ZenityErrorBox` to `ZenityAvailable`,
which calls `system("zenity --help")`, a syscall the ROM never implements,
before reaching its own `exit(-1)`. The exit code of a successful DOOM benchmark
is the error path's, by way of a dialog box that was never there. It is pinned
in the specification now, confirmed on two complete runs of 2,836,207,097 and
2,300,210,133 instructions against two different pinned binaries.

## What an instruction costs

Emulation mode works and it is slow. An expression engine charges for work in a
way a CPU emulator cannot avoid, and none of the slowness is specific to my SQL.

**Every query pays to be analyzed, and there is no plan cache.** On ClickHouse
26.7.5.10 the cost is about 25 microseconds per node of the query's syntax tree.
A thousand-node statement costs 24 milliseconds before it executes a row. The
CPU's fold is roughly 90,000 nodes, so a batch pays about 1,650 milliseconds of
analysis before any instruction retires. Nothing caches that. A SQL user-defined
function or a parameterized view is inlined and analyzed again on every query,
so neither is an escape.

**Every node of a lambda is evaluated on every step, whichever branch is taken.**
This is the one that decides the architecture. Measured on a fold of 16 steps
with a 19,200-node lambda: 100 milliseconds with short-circuit evaluation on, 89
with it forced, and 77 with it off. About a quarter of a microsecond per node
per step, paid whether or not the instruction being executed needed that node. A
CPU step is a large `multiIf` over opcodes, so every instruction pays for every
opcode.

Turning short-circuit evaluation off is worth 17.27 percent, plus or minus 1.14.
It also broke everything immediately. With short-circuiting off, an expression
of the form `if(divisor = 0, all_ones, intDiv(a, divisor))` evaluates `intDiv`
on every instruction, and since `rs2` is register zero on ordinary instructions,
the divisor is zero on ordinary instructions. The fold threw on every program.
It turned out that exactly four arms of the whole generated step were not
already total, and making those four safe costs 2.27 percent. The net was still
worth having, and the specification now requires that every setting a
computation depends on is pinned in that query's own `SETTINGS` clause rather
than inherited from anywhere.

The other numbers that shape the ceiling: a compiled expression node costs about
4.4 nanoseconds and an interpreted one about 0.29 microseconds, and the JIT
cannot touch arrays, tuples or `arrayElement`, which is most of what a CPU
written this way is made of.

Current throughput, measured for this post on 12 September 2026 on commit
`259346f`, on an idle Apple M5 Max with 128 GiB of memory, against the pinned
ClickHouse 26.8.2.7 image, at K = 60,000 and a write-log high-water mark of
20,000. Each arm runs four warm-up batches and three timed ones in a fresh
container of its own, and the figures are the mean of five repeats, in
instructions per second:

| Window | Fold alone | End to end |
| --- | --- | --- |
| Boot, from instruction 0 | 5,107 ± 74 | 5,128 ± 66 |
| Gameplay, from instruction 194,583,691 | 4,424 ± 113 | 4,272 ± 234 |

The specification sets a bar of 5,000 sustained, and the gameplay window does
not clear it on this version. Whether it still did on 26.8.2.7 was an open
question, and this run is the answer: boot holds, and gameplay is about 12
percent down on the 4,875 the same window measured on 26.7.5.10. The gameplay
end-to-end arm also spreads further than this project is used to, 5.5 percent
across repeats against 1.3 for boot, and I do not have an explanation for that
yet.

At the gameplay rate the full `demo3` timedemo, which is 2,300,210,133
instructions, takes about six days. I have accounted for where the remaining
time goes as far as I can: the ceiling for all remaining node-level work is
somewhere around 6,200 to 6,590 instructions per second, and between 11 and 32
percent of every step is still unattributed. I have not closed that gap.

## The shape of the problem was wrong

Six days for a benchmark run is a fine result for a curiosity and a bad one for
a game. The frame rate DOOM was built for is 35 a second, and that is four
orders of magnitude away. No amount of tuning crosses four orders of magnitude.

The mistake was in the granularity. Emulating a CPU asks the expression engine
to pay its per-expression cost once per instruction, and DOOM executes something
like 1.38 million instructions per frame. DOOM itself has a completely different
shape. A tic is one transformation of a few hundred things and sectors. A frame
is 64,000 pixels that do not depend on each other. Written directly as SQL, each
of those pays the per-expression cost once per tic or once per frame, and 64,000
independent pixels are exactly the kind of work a column store is built for.

So the direction changed: keep the emulator, and write DOOM's own simulation and
renderer as SQL, function for function. Keeping the emulator is what makes the
rewrite checkable. A Doom-like in SQL would be a fine project. This is DOOM in
SQL with the real DOOM as the referee.

## A statement that never ends

A tic of DOOM is tens of thousands of syntax-tree nodes. At 25 microseconds a
node, issuing that query 35 times a second was never going to work on any
hardware. The analysis cost alone would eat the frame.

What does work is a statement that is analyzed once and then never ends.

An `INSERT INTO ... SELECT ... FROM input(...)` sent over one HTTP request whose
body stays open is parsed and analyzed a single time. With
`max_insert_block_size` set to one, every row streamed into that open body is
processed as it arrives. The driver sends one small row per tic, and the
statement that has been resident for the whole session computes the next game
state from it.

State carries from tic to tic through the destination table. When the
destination is a `Join` table, a row written by one block is readable by the
next block of the same statement through `joinGet`. A `Memory` table does not
work here, because it commits its blocks when the statement ends, which means a
resident statement writing into one is invisible until it dies. That took three
wrong measurements to establish, and two of them were my instruments rather than
the database: `curl` fills a 64 KB upload buffer before sending anything, and
`now64()` is constant for the whole query, so an arrival-time column recorded
the query's start time for every row.

The shape ends up looking like this:

```sql
INSERT INTO native_state
WITH
    joinGet('native_state', 'p_x', tic - 1) AS prev_x
    -- and several hundred more stages
SELECT
    tic,
    ...
FROM input('tic UInt32, source UInt8, keys UInt32, mouse_dx Int32, mouse_dy Int32')
```

Two details that cost me time. The `WITH` clause has to sit between `INSERT
INTO` and `SELECT`; putting it before `INSERT` is a syntax error, and in a
streamed statement that error does not surface until the body closes, so it
looks like a hang. And the statement text cannot travel as a URL parameter,
because that is capped near 64 KB and these statements are far larger. It leads
the request body instead. Since the server pre-reads `max_query_size` bytes
before it parses, the first row after the statement has to be padding, which the
query filters out.

**A resident statement gives up most of SQL.** No `GROUP BY`, no `ORDER BY`, no
window functions, no recursive CTEs, because all of those wait for end of input
and the input never ends. Every stage is an array expression inside a single
row. Aggregates and window functions are still available at load time, where the
level geometry, the composed textures and the BSP ancestor paths are built with
ordinary queries.

## What a deep statement holds

The cost model inside one of these statements caught me out repeatedly, and none
of it is specific to DOOM.

**Every node in the statement is evaluated for every row.** Both arms of an `if`
included, and a lambda's body even when the array it maps over is empty.
`arrayFold` is the single exception: it runs its body once per element and not
at all for none. So any stage that has nothing to do on a given tic is written
as the body of a fold over the list of work it actually has, which is the
difference between paying for the monster AI on a tic with no monsters and not
paying for it.

**A constant array is held as one value per element.** In a statement dozens of
subqueries deep each element costs kilobytes, and one version of the renderer
asked for 33.9 GiB of memory for its sprite pixel pools. Held as `String`s and
read with `substring`, the same renderer peaks at 909 MiB. A string constant is
one value whatever its length.

**A per-frame array captured inside a lambda is copied once per element of the
array being mapped.** This is the rule that shapes every hot query in the
project. 320 columns capturing a 1,371-element array of level segments costs 1.5
milliseconds. 64,000 pixels capturing a 54,000-element frame array costs 129.
Constants declared with `WITH` are never replicated, so per-pixel lookups go to
constants only and per-frame data has to be consumed element-wise.

**Analysis cost is not a function of statement size, and where it comes from is
not settled.** The simulation began as one resident statement and took 81.37
seconds to analyze. Split into two chained through a staging table, the same
work analyzes in 32.75 plus 1.10 seconds. The statement did not get smaller. The
nesting got shallower. I have measured a good deal around this and cannot yet
give you a clean rule. Every piece added to a statement now gets measured with
`QueryAnalysisMicroseconds` from `system.query_log`.

## The referee

The reference emulator runs the real ROM, and a probe reads the engine's game
state straight out of its RAM by symbol name and struct layout at every frame it
commits. It writes that state as a row in exactly the shape the SQL simulation
writes. Parity is then one query: the first tic and the first field on which the
two disagree. Frames are compared the same way, by hash over the framebuffer and
palette. Two hashes pin the demo, frame 220 at `aa27f0470c7c5f3a` and the final
frame at `d303721d8116e877`.

One field is excluded from the comparison. `T_VerticalDoor` never reads a door's
`topcountdown` while the door is going up, so the engine leaves whatever the
zone allocator happened to return there. That field can never be made to agree,
so it is named in the contract and skipped. Everything else is compared.

**A tic the SQL cannot produce exactly is refused.** The state row carries a
bitmask, and any path the implementation does not faithfully run sets a named
bit instead of producing a plausible answer. The comparison reads the mask first
and stops before comparing a single field, so a refused tic never contributes a
false pass.

The bits read like a to-do list written by someone being hard on themselves:

- `PL_HURTS`, the player stands in a sector that damages it.
- `PLANE_CRUSH`, a plane thinker crushes, and a crusher keeps moving into
whatever is stuck under it.
- `PLAYER_DIES`, a hit on the player leaves its health at zero or below.
- `TX_CROWDED`, two movers this tic stand close enough that running one after
the other would not read the world the other one left.
- `AT_DRAW_UNSURE`, a melee attack's worst-case draw count could still
undercount: a demon's claw connects, its target's health sits under the fall
damage, and the height between them clears the fall check, so a real roll under
the worst case might still draw the extra number.
- `PLAT_NEXT_HIGHEST_OVERFLOW`, a platform whose sector has a 23rd qualifying
neighbor would overflow a 22-slot buffer and crash the original game, so this
leaves the tic unresolved instead of picking a behavior.

And there is a test asserting that every named bit is actually set somewhere in
the generated SQL, because "a bit with nothing behind it would never tell a
caller why a run stopped." A refusal mechanism nothing reaches is
indistinguishable from a refusal mechanism that works, which is the same failure
mode as a test that never runs.

## Where it stands

**The renderer is done and it is exact.** All 2,172 frames of `demo3` render
inside ClickHouse from the game states the probe recorded, and every one hashes
to the value the engine produced. The last four holdouts were fixed by spacing
the sprite pixel pool the way the original zone allocator does: `R_DrawColumn`
reads its source with an unsigned shift that, on a post's first row, can fall
just below zero and wrap, reading up to 127 bytes past the post. The engine's
cached lumps sit in zone blocks with a 24-byte header between them, and my pool
had packed them end to end, so the overrun read different bytes. It steps by the
zone's stride now.

That playback runs at 35 frames per second in a window, which is DOOM's own
rate. Those frames come from the engine's recorded state, so what they test is
the renderer. The simulation is not in that path.

**The simulation is partial, and interactive play is not yet at 35 Hz.** The
simulation is exact on every compared field for a long stretch of the demo, and
then it refuses. On the current main branch the first tic it refuses is 181. The
combat layer is in: hitscan tracing, puffs and blood, damage and kills, monster
chase and attack, missiles in flight, doors, platforms, floors and switches. The
player's own death is not: `PLAYER_DIES` is still a refusal bit.

Cost is the open problem. A tic needs to fit in 28.6 milliseconds and does not.
The most recent figures I have, recorded on the issue tracking the monster
thinker work, put an idle tic between 38.65 and 43.20 milliseconds on a
development machine, and the critical path is the tic. Sustained interactive 35
Hz is not demonstrated, and I do not have a measured basis for promising that
the finished simulation will reach it. An independent review of the project made
that point in exactly those terms, and it was right.

## How it was built

Most of ClickDOOM was written by Claude agents working in parallel git
worktrees, each owning a stack of pull requests, with me setting direction,
reviewing every one and ruling on the questions that were mine. The project's
`AI_POLICY.md` says the bar a change clears is the same however it was produced,
and names the model in the pull request description when a model did most of the
work.

A differential against the real engine cannot be argued with. An agent cannot
reason its way past a frame hash, and the refusal bitmask means the honest
answer to "I cannot do this part exactly" is available and cheap, so there is no
pressure to produce something plausible instead. Most of what I had to catch by
hand was in the places the oracle does not reach: a color channel swap that
every hash agreed with because the hash covers the framebuffer and the palette
and not the words handed to the window, and a licensing question about whether a
crate that translates the engine's functions can sit under the same license as
an emulator that only runs the binary. It cannot, and that part of the tree is
GPL now.

## If you want to break it

ClickDOOM is on [GitHub](https://github.com/MarcusKainth/ClickDOOM), with a
build log that carries the failed experiments as well as the ones that worked.
The most useful thing anyone could send me is a case where the SQL CPU and the
reference emulator disagree on the same ROM, or a place where computation has
quietly left SQL and landed in the driver. Both would be real bugs, and the
second one is the kind I would never find by looking.

---

*I build [spate](https://github.com/spate-etl/spate), an open-source streaming
ETL framework in Rust, and a ClickHouse sink is one of the things it ships.
ClickDOOM is what I do with ClickHouse when nobody is watching. The [user
guide](/docs/user-guide/) is the place to start if the day job is more your
thing.*
