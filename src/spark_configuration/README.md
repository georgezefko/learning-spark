1) The bare minimum you must provide
These drive everything else:
Input Data Size (GB) — logical size of what the job reads (uncompressed).
Used to estimate partitions and memory.
Example: 500 GB Parquet.
Input Format — affects default file-split size and memory multiplier.
Parquet/Delta are compact; CSV/JSON are heavier.
Workload Type — sets GB heap per core and a working-set multiplier.
“Joins/Aggs” and “Shuffle-Heavy” expect more memory than “Simple ETL”.
Waves — how many batches you’re willing to run the tasks in.
target_cores ≈ partitions / waves. 2–3 is a good default.
Example: If you have 2,000 partitions and waves=2 → target 1,000 cores.
Optimize for (Balanced/Memory/CPU) — if memory and CPU requirements diverge, this decides which one you satisfy first when picking workers.
2) Estimated Requirements — what they mean
These appear after you set the inputs and are the targets the recommender will try to meet:
Minimum total memory (GB) — total executor heap your cluster should provide.
It’s the max of:
(Working set) ≈ input_size × (workload × format × skew) + cache, and
(Per-core rule) target_cores × GB_per_core.
If you see spilling/OOMs, this number was too low. If GC is high with idle CPUs, it may be too high.
Target total cores — how much parallelism you want to keep waves low.
ceil(partitions / waves).
Target input partitions — derived from format and file-split size.
For Parquet, default is ~256 MB per split.
Example: 500 GB Parquet → ceil(500×1024 / 256) = 2,000 partitions.
Under the hood the tool also sets an AQE advisory partition size (post-shuffle) to ~2× your file split (min 512 MB) so AQE can coalesce shuffles appropriately.
3) Caching & headroom — when and how to use
Open Optional: Caching & Headroom if either applies:
Cache a major dataset? — If you persist() a big DataFrame, add its cache size (GB). This memory is on top of the working set.
Use it: iterative ETL, ML feature sets, repeated joins.
Skew/headroom factor — Multiplies the memory estimate to cover uneven keys or bursts (default 1.1).
Use it: heavy joins with skewed keys, wide aggregations, or flaky upstream sizes.
Rule of thumb: 1.1–1.2 for mild skew; up to 1.5 for known hotspots.
4) Advanced options — what they control (and when to touch them)
Open Advanced Layout & Overrides only if you have signals from metrics or platform constraints:
Reserve cores/mem per node (defaults: 1 core, 6 GB)
Leave headroom for OS/daemons (or for Databricks worker services). Increase on very large nodes if you see system pressure.
Cores per executor (default 4)
Trade-off: more cores/executor = fewer JVMs but longer GC; fewer cores/executor = more JVMs and better GC isolation.
Typical: 4–6 for shuffle-heavy batch.
Memory overhead % (off-heap, default 12%)
Off-heap space for shuffle/unroll/Arrow. Use 15–25% on wide shuffles/UDF-heavy jobs.
Heap GB per core (override)
Forces a memory density target (GB heap / core). Use if you already know, e.g., this pipeline needs ~4 GB/core regardless of input size.
Target partition size (MB, override)
Overrides file split size (spark.sql.files.maxPartitionBytes).
Use smaller splits for slow sources or severe skew; larger splits for fast storage/engines. (The tool keeps AQE advisory ≈ 2× this.)
Enable Dynamic Allocation
On for elastic/bursty jobs (good default). Turn off for highly predictable batch SLAs or some streaming patterns where stable executor counts are desirable.
5) Recommendation — what it actually recommends
This section picks a concrete cluster shape and a matching Spark config:
Suggested Cluster — Instance family and number of workers that meet your targets, considering your “Optimize for” choice.
It shows total cores and total executor heap you’ll get.
Workers (by memory) vs (by CPU) — how many workers you’d need if you satisfied only memory vs only cores.
Chosen workers is the max of those (or the one you selected via “Optimize for”).
Autoscaling hints (when Dynamic Allocation is on) — translates total executors into Databricks-style min/initial/max workers.
Use these directly for an autoscaling cluster.
Spark config output — ready-to-paste spark-defaults.conf lines (plus spark-submit flags / Databricks JSON). Key lines:
spark.executor.cores — cores per executor (parallelism per container).
spark.executor.memory — integer GB heap per executor (post-overhead).
spark.executor.memoryOverhead — off-heap; computed from your overhead % (≥384 MB).
spark.sql.files.maxPartitionBytes — file split target (bytes).
spark.sql.adaptive.advisoryPartitionSizeInBytes — post-shuffle target (bytes), ~2× file split.
spark.sql.shuffle.partitions — starting shuffle partition count; capped and AQE will coalesce.
spark.sql.adaptive.enabled=true and spark.sql.adaptive.skewJoin.enabled=true — enable AQE + skew mitigation.
spark.dynamicAllocation.* — min/initial/max executors (if enabled).
spark.driver.cores, spark.driver.memory — driver sizing; driver heap ≈ executor heap clamped (4–32 GB).
spark.driver.maxResultSize=2g — protect driver from huge collects.
Candidates table — alternates sorted by fewest workers, then cores.
“Meets memory/cores” tells you why a candidate is or isn’t feasible.
6) Quick end-to-end example
Goal: 500 GB Parquet join pipeline, waves=2, “Joins/Aggs”, Balanced.
Inputs
Data size = 500 GB, Format = Parquet, Workload = Joins/Aggs, Waves = 2, Optimize = Balanced.
Estimated Requirements
Partitions ≈ ceil(500×1024 / 256) = 2,000.
Target cores = ceil(2,000 / 2) = 1,000.
Memory target = max( working set, per-core ). With default multipliers, per-core often wins for wide joins.
Recommendation
Tool proposes an instance family + N workers that deliver ≥1,000 cores and enough executor heap.
Sees whether Memory or CPU is the limiting factor and sets dynamic allocation bounds accordingly.
Apply config
Use spark-defaults.conf as emitted, or the spark-submit flags, or the Databricks JSON snippet.
Validate & iterate
If you see spill → bump overhead % or heap/core, or reduce cores/executor.
If you see starvation (few tasks running) → increase waves (fewer cores needed) or choose Optimize for CPU.
If GC dominates → lower cores/executor (e.g., 6→4) and/or reduce heap/core.
7) When to favor each “Optimize for …”
Memory — frequent spills, OOM, wide aggregations, large broadcast failures.
CPU — tiny tasks, light shuffles, IO-bound sources where you want more concurrency.
Balanced — default; lets the tool pick the smallest cluster that meets both targets.
8) Fast troubleshooting checklist
Driver OOMs? Increase spark.driver.memory (or stop collecting huge DataFrames).
Shuffle too many partitions? Raise file split or waves; AQE will still coalesce to advisory size.
Executors idle with many partitions left? Target cores too low or dynamic allocation too conservative → increase min/initial.
Skewed joins? Keep AQE + skew join enabled; add Skew/headroom (1.2–1.5), and consider larger advisory partitions.