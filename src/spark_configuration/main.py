import math
from typing import List, Dict

import json
import pandas as pd
import streamlit as st

# -----------------------
# 🧾 Quick tooltip text (UI help)
# -----------------------
CONFIG_TIPS_MD = """
- **spark.executor.instances** — Total executors (parallel containers). Drives cluster size.
- **spark.executor.cores** — Cores per executor. 4–6 is a good default for shuffle-heavy batch.
- **spark.executor.memory** — Executor heap (JVM). Raise for spill/OOM; keep heap 8–32 GB typically.
- **spark.executor.memoryOverhead** — Off-heap memory (shuffle/unroll/Arrow). ~10–30% of heap.
- **spark.driver.cores** — Driver CPU. Bump only if the driver is CPU-bound.
- **spark.driver.memory** — Driver heap. Raise if the driver OOMs during planning/collect.
- **spark.driver.maxResultSize** — Cap to prevent giant collects (e.g., 2g).
- **spark.default.parallelism** — RDD default tasks. ≈ max(2× cores, input partitions).
- **spark.sql.shuffle.partitions** — SQL shuffle tasks. ≈ 2× cores (cap ~4× cores); AQE coalesces.
- **spark.dynamicAllocation.enabled** — Auto-scale executors. Usually true (with shuffle tracking).
- **spark.dynamicAllocation.{min,initial,max}Executors** — Bounds for dynamic allocation.
- **spark.sql.adaptive.enabled** — Adaptive Query Execution. Keep on.
- **spark.serializer** — Kryo for faster/smaller serialization.
- **spark.rdd.compress / spark.shuffle.*.compress** — Keep true; saves IO.
- **spark.sql.files.maxPartitionBytes** — Input split size (bytes). 256 MiB typical for Parquet/Delta.
- **spark.sql.adaptive.advisoryPartitionSizeInBytes** — AQE target partition size.
- **spark.sql.autoBroadcastJoinThreshold** — Broadcast truly small dimension tables to avoid shuffles.
"""

# -----------------------
# 🎛️ Inputs & Heuristics
# -----------------------
WORKLOAD_PRESETS = {
    "Simple ETL / Reads": {"memory_multiplier": 1.5, "gb_per_core": 2.0, "target_partition_mb": 256},
    "Joins / Aggregations": {"memory_multiplier": 2.5, "gb_per_core": 3.5, "target_partition_mb": 256},
    "Shuffle-Heavy / Wide": {"memory_multiplier": 4.0, "gb_per_core": 5.0, "target_partition_mb": 256},
    "ML / UDF-Heavy": {"memory_multiplier": 3.0, "gb_per_core": 4.0, "target_partition_mb": 128},
}

FILE_FORMATS = {
    "Delta/Parquet": {"partition_mb": 256, "mem_multiplier_adj": 1.0},
    "CSV": {"partition_mb": 128, "mem_multiplier_adj": 1.2},
    "JSON": {"partition_mb": 128, "mem_multiplier_adj": 1.3},
    "ORC": {"partition_mb": 256, "mem_multiplier_adj": 1.0},
}

# -----------------------
# 📦 Built-in catalogs (offline)
# Memory/cores are typical values; exact specs vary slightly by region/SKU.
# -----------------------
AZURE_CATALOG = [
    # Dv3 (≈4 GiB/vCPU)
    {"cloud": "Azure", "name": "Standard_D4s_v3",   "memory_gb": 16,   "cores": 4},
    {"cloud": "Azure", "name": "Standard_D8s_v3",   "memory_gb": 32,   "cores": 8},
    {"cloud": "Azure", "name": "Standard_D16s_v3",  "memory_gb": 64,   "cores": 16},
    {"cloud": "Azure", "name": "Standard_D32s_v3",  "memory_gb": 128,  "cores": 32},
    {"cloud": "Azure", "name": "Standard_D64s_v3",  "memory_gb": 256,  "cores": 64},
    # Dv5 (≈4 GiB/vCPU)
    {"cloud": "Azure", "name": "Standard_D8s_v5",   "memory_gb": 32,   "cores": 8},
    {"cloud": "Azure", "name": "Standard_D16s_v5",  "memory_gb": 64,   "cores": 16},
    {"cloud": "Azure", "name": "Standard_D32s_v5",  "memory_gb": 128,  "cores": 32},
    {"cloud": "Azure", "name": "Standard_D48s_v5",  "memory_gb": 192,  "cores": 48},
    {"cloud": "Azure", "name": "Standard_D64s_v5",  "memory_gb": 256,  "cores": 64},
    # Ev3 (≈8 GiB/vCPU; E64s_v3 is 432 GiB)
    {"cloud": "Azure", "name": "Standard_E4s_v3",   "memory_gb": 32,   "cores": 4},
    {"cloud": "Azure", "name": "Standard_E8s_v3",   "memory_gb": 64,   "cores": 8},
    {"cloud": "Azure", "name": "Standard_E16s_v3",  "memory_gb": 128,  "cores": 16},
    {"cloud": "Azure", "name": "Standard_E32s_v3",  "memory_gb": 256,  "cores": 32},
    {"cloud": "Azure", "name": "Standard_E64s_v3",  "memory_gb": 432,  "cores": 64},
    # Ebdsv5 (≈8 GiB/vCPU, local NVMe)
    {"cloud": "Azure", "name": "Standard_E8bds_v5",  "memory_gb": 64,   "cores": 8},
    {"cloud": "Azure", "name": "Standard_E16bds_v5", "memory_gb": 128,  "cores": 16},
    {"cloud": "Azure", "name": "Standard_E32bds_v5", "memory_gb": 256,  "cores": 32},
    {"cloud": "Azure", "name": "Standard_E48bds_v5", "memory_gb": 384,  "cores": 48},
    {"cloud": "Azure", "name": "Standard_E64bds_v5", "memory_gb": 512,  "cores": 64},
    # Fsv2 (≈2 GiB/vCPU)
    {"cloud": "Azure", "name": "Standard_F8s_v2",   "memory_gb": 16,   "cores": 8},
    {"cloud": "Azure", "name": "Standard_F16s_v2",  "memory_gb": 32,   "cores": 16},
    {"cloud": "Azure", "name": "Standard_F32s_v2",  "memory_gb": 64,   "cores": 32},
    {"cloud": "Azure", "name": "Standard_F48s_v2",  "memory_gb": 96,   "cores": 48},
    {"cloud": "Azure", "name": "Standard_F72s_v2",  "memory_gb": 144,  "cores": 72},
    # Lsv2 (≈8 GiB/vCPU, storage-optimized with NVMe)
    {"cloud": "Azure", "name": "Standard_L8s_v2",   "memory_gb": 64,   "cores": 8},
    {"cloud": "Azure", "name": "Standard_L16s_v2",  "memory_gb": 128,  "cores": 16},
    {"cloud": "Azure", "name": "Standard_L32s_v2",  "memory_gb": 256,  "cores": 32},
    {"cloud": "Azure", "name": "Standard_L48s_v2",  "memory_gb": 384,  "cores": 48},
    {"cloud": "Azure", "name": "Standard_L64s_v2",  "memory_gb": 512,  "cores": 64},
    {"cloud": "Azure", "name": "Standard_L80s_v2",  "memory_gb": 640,  "cores": 80},
]

AWS_CATALOG = [
    {"cloud": "AWS", "name": "m5d.2xlarge",  "memory_gb": 32,   "cores": 8},
    {"cloud": "AWS", "name": "m5d.4xlarge",  "memory_gb": 64,   "cores": 16},
    {"cloud": "AWS", "name": "m5d.8xlarge",  "memory_gb": 128,  "cores": 32},
    {"cloud": "AWS", "name": "m5d.12xlarge", "memory_gb": 192,  "cores": 48},
    {"cloud": "AWS", "name": "m5d.24xlarge", "memory_gb": 384,  "cores": 96},
    {"cloud": "AWS", "name": "r5d.2xlarge",  "memory_gb": 64,   "cores": 8},
    {"cloud": "AWS", "name": "r5d.4xlarge",  "memory_gb": 128,  "cores": 16},
    {"cloud": "AWS", "name": "r5d.8xlarge",  "memory_gb": 256,  "cores": 32},
    {"cloud": "AWS", "name": "r5d.12xlarge", "memory_gb": 384,  "cores": 48},
    {"cloud": "AWS", "name": "r5d.24xlarge", "memory_gb": 768,  "cores": 96},
    {"cloud": "AWS", "name": "i3.2xlarge",   "memory_gb": 61,   "cores": 8},
    {"cloud": "AWS", "name": "i3.4xlarge",   "memory_gb": 122,  "cores": 16},
    {"cloud": "AWS", "name": "i3.8xlarge",   "memory_gb": 244,  "cores": 32},
    {"cloud": "AWS", "name": "i3.16xlarge",  "memory_gb": 488,  "cores": 64},
]

BUILTIN_CATALOG = {"Azure": AZURE_CATALOG, "AWS": AWS_CATALOG}

DEFAULTS = {
    "reserve_cores_per_node": 1,
    "reserve_mem_gb_per_node": 6,
    "cores_per_executor": 4,
    "memory_overhead_frac": 0.12,
}

# -----------------------
# 🧮 Helper functions
# -----------------------
def ceil_div(a: float, b: float) -> int:
    return int(math.ceil(a / b))

def partitions_for_data(size_gb: float, part_mb: int) -> int:
    return max(1, ceil_div(size_gb * 1024, part_mb))

def executors_layout(
    cores_per_node: int,
    mem_per_node_gb: float,
    reserve_cores: int,
    reserve_mem_gb: float,
    cores_per_executor: int,
    overhead_frac: float,
) -> Dict:
    usable_cores = max(0, cores_per_node - reserve_cores)
    if cores_per_executor <= 0 or usable_cores <= 0:
        return {"executors_per_node": 0, "executor_cores": 0, "executor_memory_gb": 0.0}

    exec_cores = min(cores_per_executor, usable_cores)
    executors_per_node = max(1, usable_cores // exec_cores)

    usable_mem_gb = max(0.0, mem_per_node_gb - reserve_mem_gb)
    if executors_per_node == 0 or usable_mem_gb <= 0:
        return {"executors_per_node": 0, "executor_cores": 0, "executor_memory_gb": 0.0}

    container_mem_per_exec = usable_mem_gb / executors_per_node
    executor_heap_gb = max(1.0, container_mem_per_exec * (1 - overhead_frac))

    return {
        "executors_per_node": executors_per_node,
        "executor_cores": exec_cores,
        "executor_memory_gb": round(executor_heap_gb, 1),
    }

def spark_conf_from_layout(
    total_execs: int,
    exec_cores: int,
    exec_mem_gb: float,
    part_count: int,
    part_mb: int,
    enable_dynamic_allocation: bool = True,
) -> Dict[str, str]:
    total_cores = max(1, total_execs * max(1, exec_cores))
    default_parallelism = max(2 * total_cores, part_count)  # RDD world
    shuffle_partitions = min(max(2 * total_cores, part_count), 4 * total_cores)  # cap at 4× cores
    bytes_per_part = part_mb * 1024 * 1024

    conf = {
        "spark.sql.adaptive.enabled": "true",
        "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
        "spark.rdd.compress": "true",
        "spark.shuffle.compress": "true",
        "spark.shuffle.spill.compress": "true",
        "spark.executor.cores": str(exec_cores),
        "spark.executor.memory": f"{int(exec_mem_gb)}g",
        "spark.default.parallelism": str(default_parallelism),
        "spark.sql.shuffle.partitions": str(shuffle_partitions),
        "spark.sql.files.maxPartitionBytes": str(bytes_per_part),
        "spark.sql.adaptive.advisoryPartitionSizeInBytes": str(bytes_per_part),
        "spark.driver.cores": str(min(8, max(2, exec_cores))),
        "spark.driver.maxResultSize": "2g",
    }

    if enable_dynamic_allocation:
        conf["spark.dynamicAllocation.enabled"] = "true"
        conf["spark.dynamicAllocation.shuffleTracking.enabled"] = "true"
    else:
        conf["spark.dynamicAllocation.enabled"] = "false"
        conf["spark.executor.instances"] = str(total_execs)
    return conf

def filter_catalog(catalog: List[Dict], families: List[str]) -> List[Dict]:
    if families:
        return [c for c in catalog if any(c["name"].lower().startswith(fam.lower()) for fam in families)]
    return catalog

# -----------------------
# 🖼️ UI
# -----------------------
st.set_page_config(page_title="Spark Cluster Recommender", page_icon="🧠", layout="wide")
st.title("🧠 Spark Cluster Recommender")

# Sidebar: cloud & catalog source (API is placeholder)
st.sidebar.header("🔌 Cloud & Instance Catalog")
cloud_choice = st.sidebar.radio("Cloud", ["Azure", "AWS"], index=0, horizontal=True)

catalog_source = st.sidebar.radio(
    "Instance catalog",
    ["Built-in catalog (offline)", "Databricks API (placeholder)"],
    index=0,
)
if catalog_source == "Databricks API (placeholder)":
    st.sidebar.info("API loading is disabled for now. Switch to the built-in catalog, or wire up the API later.")

# Sidebar: filters
st.sidebar.markdown("---")
working_catalog = BUILTIN_CATALOG[cloud_choice]
family_options = sorted(list({x["name"].split("_")[0] for x in working_catalog}))
family_filter = st.sidebar.multiselect(
    "Filter by family/prefix",
    options=family_options,
    help="Example: Standard_D, Standard_E, m5d, r5d, etc."
)

INSTANCE_CATALOG: List[Dict] = filter_catalog(working_catalog, family_filter)

left, right = st.columns([1.3, 1])
with left:
    st.header("📥 Inputs")
    data_size_gb = st.number_input("Input Data Size (GB)", value=250, min_value=1,
                                   help="Logical size of input read by the job (uncompressed).")
    file_fmt = st.selectbox("Input Format", list(FILE_FORMATS.keys()), index=0,
                            help="Impacts partition size & memory multiplier (CSV/JSON heavier).")
    workload = st.selectbox("Workload Type", list(WORKLOAD_PRESETS.keys()), index=1,
                            help="Sets GB heap per core & working-set multiplier.")
    waves = st.slider("Waves (batches of tasks)", 1, 6, 2,
                      help="total_cores ≈ partitions / waves. 2–3 is typical.")
    optimize_for = st.radio("Optimize for", ["Balanced", "Memory", "CPU"], index=0, horizontal=True,
                            help="Choose which constraint drives worker count when memory vs CPU differ.")

    with st.expander("Optional: Caching & Headroom"):
        caching = st.toggle("Cache a major dataset?", value=False)
        cache_size_gb = st.number_input("Estimated cache size (GB)", value=0, min_value=0,
                                        help="Add if you will persist a DataFrame in memory.") if caching else 0
        skew = st.slider("Skew/headroom factor", 1.0, 2.0, 1.1, 0.05,
                         help="Multiply memory target to account for data skew or spikes.")

    with st.expander("Advanced Layout & Overrides"):
        st.markdown(
            """
**Controls how executors are packed into machines and memory assigned to each.**
- **Reserve cores/mem**: leave a slice for OS/daemons.
- **Cores per executor**: 4–6 balances GC vs throughput for shuffles.
- **Overhead %**: off-heap headroom for shuffle/unroll/UDFs (10–30%).
- **Heap GB per core (override)**: force per-core heap; otherwise use workload preset.
- **Partition size (override)**: input split size; affects partition count → cores.
- **Dynamic allocation**: let Spark scale executors during the job.
            """
        )
        reserve_cores = st.number_input("Reserve cores/node", value=DEFAULTS["reserve_cores_per_node"], min_value=0, max_value=8)
        reserve_mem = st.number_input("Reserve mem/node (GB)", value=DEFAULTS["reserve_mem_gb_per_node"], min_value=0, max_value=64)
        cores_per_exec = st.number_input("Cores per executor", value=DEFAULTS["cores_per_executor"], min_value=1, max_value=32)
        overhead_frac = st.slider("Memory overhead % (off-heap)", 5, 40, int(DEFAULTS["memory_overhead_frac"] * 100)) / 100.0
        gb_per_core_override = st.number_input("Heap GB per core (override)", value=0.0, min_value=0.0, step=0.5)
        target_part_mb_override = st.number_input("Target partition size (MB, override)", value=0, min_value=0, step=64)
        dyn_alloc = st.toggle("Enable Dynamic Allocation", value=True)

with right:
    st.header("ℹ️ How these are used")
    st.markdown(
        """
- **Partitions:** ~128–256 MB per split; format matters.
- **Waves:** cores ≈ partitions / waves (2–3 typical).
- **Memory target:** workload × input (+ cache) × skew; also check GB/core.
- **Layout:** ~4–6 cores/executor; add 10–30% overhead; reserve OS CPU/RAM.
- We fit executors into instances and pick the smallest worker count meeting CPU & memory for your objective.
        """
    )
    st.caption("Tune via metrics: spilling → more heap; long GC → fewer cores/executor; starvation → more cores or fewer waves.")

# Partition sizing
preset = WORKLOAD_PRESETS[workload]
fmt = FILE_FORMATS[file_fmt]
part_mb = target_part_mb_override if target_part_mb_override > 0 else fmt["partition_mb"]
num_partitions = partitions_for_data(data_size_gb, part_mb)

# CPU sizing via waves
total_cores_target = max(4, ceil_div(num_partitions, max(1, waves)))

# Memory sizing
mem_mult = preset["memory_multiplier"] * fmt["mem_multiplier_adj"] * skew
gb_per_core = gb_per_core_override if gb_per_core_override > 0 else preset["gb_per_core"]
mem_req_by_input = (data_size_gb * mem_mult) + (cache_size_gb if caching else 0)
mem_req_by_core = total_cores_target * gb_per_core
min_total_memory_gb = max(mem_req_by_input, mem_req_by_core)

st.markdown("---")
st.header("🧮 Estimated Requirements")
c1, c2, c3 = st.columns(3)
c1.metric("Minimum total memory (GB)", f"{int(math.ceil(min_total_memory_gb))}",
          help=f"Working set ≈ {mem_mult:.2f}× input + cache; also ≥ {gb_per_core} GB/core")
c2.metric("Target total cores", f"{total_cores_target}", help=f"Cores ≈ partitions / waves (waves={waves})")
c3.metric("Target input partitions", f"{num_partitions}", help=f"~{part_mb} MB per partition for {file_fmt}")

# -----------------------
# 🔎 Candidate Search
# -----------------------
rows: List[Dict] = []

for inst in INSTANCE_CATALOG:
    mem_per_node = inst["memory_gb"]
    cores_per_node = inst["cores"]

    layout = executors_layout(
        cores_per_node=cores_per_node,
        mem_per_node_gb=mem_per_node,
        reserve_cores=reserve_cores,
        reserve_mem_gb=reserve_mem,
        cores_per_executor=cores_per_exec,
        overhead_frac=overhead_frac,
    )
    execs_per_node = layout["executors_per_node"]
    exec_cores = layout["executor_cores"]
    exec_heap_gb = layout["executor_memory_gb"]
    if execs_per_node == 0:
        continue

    cores_per_node_effective = execs_per_node * exec_cores
    heap_per_node = execs_per_node * exec_heap_gb

    nodes_for_memory = ceil_div(min_total_memory_gb, max(1e-9, heap_per_node))
    nodes_for_cpu = ceil_div(total_cores_target, max(1, cores_per_node_effective))

    if optimize_for == "Memory":
        nodes = max(1, nodes_for_memory)
    elif optimize_for == "CPU":
        nodes = max(1, nodes_for_cpu)
    else:
        nodes = max(1, max(nodes_for_memory, nodes_for_cpu))

    total_execs = nodes * execs_per_node
    total_cores = total_execs * exec_cores
    total_exec_heap = round(total_execs * exec_heap_gb, 1)

    meets_mem = total_exec_heap >= min_total_memory_gb
    meets_cores = total_cores >= total_cores_target

    limiting = (
        "Memory" if nodes == nodes_for_memory and nodes_for_memory >= nodes_for_cpu else
        "CPU" if nodes == nodes_for_cpu and nodes_for_cpu >= nodes_for_memory else
        ("Memory" if nodes_for_memory > nodes_for_cpu else "CPU")
    )

    conf = spark_conf_from_layout(
        total_execs=total_execs,
        exec_cores=exec_cores,
        exec_mem_gb=exec_heap_gb,
        part_count=num_partitions,
        part_mb=part_mb,
        enable_dynamic_allocation=dyn_alloc,
    ) if total_execs > 0 else {}

    rows.append({
        "Cloud": cloud_choice,
        "Instance": inst["name"],
        "Workers": int(nodes),
        "Workers by memory": int(nodes_for_memory),
        "Workers by CPU": int(nodes_for_cpu),
        "Limiting": limiting,
        "Execs / node": int(execs_per_node),
        "Exec cores": int(exec_cores),
        "Exec memory (GB)": exec_heap_gb,
        "Total executors": int(total_execs),
        "Total cores": int(total_cores),
        "Total exec heap (GB)": total_exec_heap,
        "Meets memory": meets_mem,
        "Meets cores": meets_cores,
        "_conf": conf,
    })

candidates = pd.DataFrame(rows)

st.markdown("---")
st.header("📈 Recommendation")

if candidates.empty:
    if not INSTANCE_CATALOG:
        st.warning("No instances in catalog. Adjust the family filter or switch clouds.")
    else:
        st.warning("No candidates computed. Check inputs and expand your instance catalog.")
else:
    candidates.sort_values(by=["Workers", "Total cores"], inplace=True)
    feasible = candidates[(candidates["Meets memory"]) & (candidates["Meets cores"])]
    table = feasible if not feasible.empty else candidates
    best = table.iloc[0]

    if feasible.empty:
        st.info("None of the instances meet both CPU and memory targets. Showing the closest fit.")

    st.subheader("✅ Suggested Cluster")
    st.write(
        f"**{best['Cloud']} {best['Instance']}** — **{int(best['Workers'])} workers** · "
        f"{int(best['Total cores'])} cores · ~{int(best['Total exec heap (GB)'])} GB executor heap"
    )

    # Worker rationale
    w_mem, w_cpu, w_chosen = int(best["Workers by memory"]), int(best["Workers by CPU"]), int(best["Workers"])
    colw1, colw2, colw3 = st.columns(3)
    colw1.metric("Workers (by memory)", w_mem)
    colw2.metric("Workers (by CPU)", w_cpu)
    colw3.metric("Chosen workers", w_chosen, help=f"Limiting factor: {best['Limiting']}")

    # Autoscaling hints
    execs_per_node = max(1, int(best["Execs / node"]))
    if dyn_alloc:
        total_execs = int(best["Total executors"])
        min_execs = max(1, int(total_execs * 0.2))
        init_execs = max(1, int(total_execs * 0.5))
        max_execs = total_execs
        min_workers = ceil_div(min_execs, execs_per_node)
        init_workers = ceil_div(init_execs, execs_per_node)
        max_workers = ceil_div(max_execs, execs_per_node)
        st.caption(f"Databricks autoscaling → **min_workers={min_workers}**, **initial≈{init_workers}**, **max_workers={max_workers}**")
    else:
        st.caption(f"Databricks fixed workers → **{w_chosen}**")

    # -----------------------
    # 🧾 Spark config output
    # -----------------------
    st.subheader("📝 Spark config output")
    _conf = dict(best["_conf"])

    # executor memoryOverhead from overhead fraction
    try:
        mem_overhead_mb = int(round((best["Exec memory (GB)"] * (overhead_frac / (1 - overhead_frac))) * 1024))
        _conf["spark.executor.memoryOverhead"] = str(max(384, mem_overhead_mb))
    except Exception:
        pass

    if dyn_alloc:
        total_execs = int(best["Total executors"])
        _conf["spark.dynamicAllocation.shuffleTracking.enabled"] = "true"
        _conf["spark.dynamicAllocation.minExecutors"] = str(max(1, int(total_execs * 0.2)))
        _conf["spark.dynamicAllocation.initialExecutors"] = str(max(1, int(total_execs * 0.5)))
        _conf["spark.dynamicAllocation.maxExecutors"] = str(total_execs)

    order = [
        "spark.dynamicAllocation.enabled",
        "spark.dynamicAllocation.shuffleTracking.enabled",
        "spark.dynamicAllocation.minExecutors",
        "spark.dynamicAllocation.initialExecutors",
        "spark.dynamicAllocation.maxExecutors",
        "spark.executor.instances",
        "spark.executor.cores",
        "spark.executor.memory",
        "spark.executor.memoryOverhead",
        "spark.driver.cores",
        "spark.driver.memory",
        "spark.driver.maxResultSize",
        "spark.default.parallelism",
        "spark.sql.shuffle.partitions",
        "spark.sql.adaptive.enabled",
        "spark.sql.files.maxPartitionBytes",
        "spark.sql.adaptive.advisoryPartitionSizeInBytes",
        "spark.serializer",
        "spark.rdd.compress",
        "spark.shuffle.compress",
        "spark.shuffle.spill.compress",
        "spark.executor.extraJavaOptions",
        "spark.driver.extraJavaOptions",
    ]
    lines = [f"{k} {_conf[k]}" for k in order if k in _conf] + \
            [f"{k} {_conf[k]}" for k in sorted(_conf.keys()) if k not in order]

    use_crlf = st.toggle("Windows line endings (CRLF)", value=False,
                         help="Enable if pasting into a tool that collapses LF-only newlines.")
    conf_text = ("\r\n" if use_crlf else "\n").join(lines)

    st.text_area("spark-defaults.conf", value=conf_text, height=260)
    st.download_button("Download spark-defaults.conf", conf_text, file_name="spark-defaults.conf")

    with st.expander("📘 What these configs do (quick tips)"):
        st.markdown(CONFIG_TIPS_MD)

    with st.expander("Alternative formats"):
        st.markdown("**spark-submit flags**")
        flags = " \\\n".join([f"--conf {k}={v}" for k, v in _conf.items()])
        st.code(f"spark-submit \\\n{flags}", language="bash")

        st.markdown("**Databricks JSON (cluster spec)**")
        st.code(json.dumps({"spark_conf": {k: str(v) for k, v in _conf.items()}}, indent=2), language="json")

    # -----------------------
    # 📋 Run plan summary
    # -----------------------
    with st.expander("📋 Run plan summary", expanded=True):
        st.markdown(f"""
- **Cloud**: **{cloud_choice}**
- **Input**: **{data_size_gb} GB** {file_fmt} · Workload: **{workload}**
- **Partitions**: **{num_partitions}** (~{part_mb} MB each) · **Waves**: {waves}
- **Targets**: **{total_cores_target} cores**, **≥ {int(math.ceil(min_total_memory_gb))} GB** executor heap
- **Cluster**: **{best['Instance']} × {int(best['Workers'])}** workers (limiting: **{best['Limiting']}**)
- **Executors**: **{int(best['Total executors'])}** total — {int(best['Execs / node'])}/node · {int(best['Exec cores'])} cores/executor · {best['Exec memory (GB)']} GB heap/executor
- **Shuffle partitions**: **{_conf['spark.sql.shuffle.partitions']}** · **Dynamic allocation**: **{str(dyn_alloc).lower()}**
        """)

    # -----------------------
    # 📊 Candidates table (highlight selection)
    # -----------------------
    st.subheader("🔎 Candidates (sorted by workers, then cores)")
    display_cols = [
        "Cloud", "Instance", "Workers", "Workers by memory", "Workers by CPU",
        "Limiting", "Execs / node", "Exec cores", "Exec memory (GB)",
        "Total executors", "Total cores", "Total exec heap (GB)", "Meets memory", "Meets cores"
    ]
    candidates["Selected"] = (candidates.index == best.name)
    show = candidates[display_cols + ["Selected"]].copy()
    show.loc[:, "Selected"] = show["Selected"].map(lambda x: "★" if x else "")
    st.dataframe(show, use_container_width=True)

st.markdown("---")
st.subheader("🔧 Quick guide")
st.markdown(
    """
- **Cloud selector** chooses Azure vs AWS; using the **built-in offline catalog** for now.
- **Filters** let you narrow by instance family (e.g., Standard_E, Standard_D, r5d, m5d).
- **Recommendation** packs executors into instances and proposes the smallest cluster that meets CPU & memory goals.

    """
)
