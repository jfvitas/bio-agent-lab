---
task_id: full_performance_analysis
role: performance_analyst
date: 2026-03-11
status: reviewed
---

# Performance Analysis Report — Full Repository Performance Review

## 1. Performance Overview

The repository implements a multi-stage bioinformatics pipeline: **RCSB ingestion → normalization → extraction → graph construction → feature engineering → dataset splitting → training example assembly**. It is currently functional for small-to-medium datasets (~hundreds of PDB entries) but has several architectural bottlenecks that will prevent scaling to large protein datasets (10k+ entries, 100k+ pairs).

**Current effective scale ceiling: ~500–1,000 PDB entries before wall-clock time and memory become prohibitive.**

## 2. Bottlenecks Identified

### B1. O(n²) All-Pairs Distance in Graph Construction — CRITICAL

**Files:** `src/pbdata/graph/structural_graphs.py:281-297` (residue), `:301-325` (atom)

```python
for i, left in enumerate(nodes):
    for right in nodes[i + 1:]:
        dist = _distance(left, right)
```

- **Residue-level:** A 500-residue protein = 124,750 distance calculations per structure. Manageable.
- **Atom-level:** A 5,000-atom protein = ~12.5M distance calculations **per structure**. A 20,000-atom complex = ~200M calculations.
- **No spatial indexing** (KD-tree, Ball tree, or cell list). Pure Python float arithmetic.
- **Estimated cost at scale:** For 1,000 structures at atom level: ~200 billion distance calculations.

### B2. Repeated mmCIF Parsing — HIGH

The same structure file is parsed by gemmi **3-4 separate times** per entry:
1. `parse_mmcif_supplement()` — full mmCIF parse
2. `parse_structure_quality()` — re-reads same text, re-parses
3. `build_structural_graphs()` — `gemmi.read_structure()` again
4. `_compute_structure_file_features()` — `gemmi.read_structure()` again

Each mmCIF parse for a large structure (100k atoms) costs ~0.5-2s. For 1,000 structures this wastes **30-120 minutes** in redundant parsing.

### B3. No Parallelism Anywhere — HIGH

Every pipeline stage is single-threaded:
- HTTP downloads: sequential (`rcsb_search.py:514` — batch loop)
- Structure file downloads: one at a time (`mmcif_supplement.py:86`)
- Graph construction: sequential per entry (`structural_graphs.py:498`)
- Feature computation: sequential per entry
- Training assembly: sequential per assay

No use of `concurrent.futures`, `asyncio`, `multiprocessing`, or any parallelism library.

### B4. Repeated Disk I/O with No Caching — HIGH

`_load_table_json()` is defined **4 times** identically across modules and called repeatedly:
- `features/builder.py:27`
- `training/assembler.py:36`
- `graph/builder.py:77`
- `data_pipeline/workflow_engine.py:74`

Each call re-reads and re-parses every JSON file in a directory from disk. When multiple pipeline stages run in sequence, the same data is loaded 3-4x.

### B5. Sequential HTTP with No Connection Pooling — MEDIUM

`rcsb_search.py` uses bare `requests.post()` / `requests.get()` calls. No `requests.Session()` for connection reuse, no retry strategy (except a 2s sleep on failure), no concurrent downloads.

## 3. Pipeline Hotspots

| Stage | Hotspot | Severity |
|-------|---------|----------|
| **Graph (atom-level)** | O(n²) pairwise distance, pure Python | CRITICAL |
| **mmCIF parsing** | 3-4 redundant parses per structure | HIGH |
| **Feature builder** | Re-reads all extracted JSON per run | HIGH |
| **Structure download** | Sequential HTTP, no parallelism | HIGH |
| **K-mer clustering** | O(n × candidates × kmer_size), no MMseqs2 | MEDIUM |
| **Training assembly** | Nested loop over interfaces per assay | MEDIUM |
| **JSON serialization** | `indent=2` on large arrays of Pydantic models | LOW |
| **ESM embedding** | Serial per-sequence, no batching | MEDIUM |

## 4. Scalability Risks

### 4a. Large-Scale Protein Datasets (10k+ entries)

- **K-mer clustering** (`splits.py:146`): Inverted index grows to millions of entries. With `max_candidates=500` cap it stays bounded, but Jaccard set operations on large frozensets (~295 5-mers per 300aa sequence) become expensive. At 100k records: **~hours on a single CPU**.
- **Download stage**: 10k entries × 100 entries/batch = 100 sequential HTTP requests + 10k sequential structure downloads. **~2-4 hours** even with fast network.

### 4b. Large Graph Generation

- **Atom-level graphs**: A single 100k-atom structure would require **5 billion distance calculations**. Completely infeasible without spatial indexing.
- **Canonical graph builder** (`graph/builder.py`): Linear scan through all ligand nodes for each assay (line 486-488) — O(assays × ligand_nodes).

### 4c. ML Dataset Generation

- **Custom k-means** (`dataset/engineering.py:159`): Pure Python, 12 fixed iterations, no convergence check, no vectorization. At 100k rows × 40-dim vectors × 8 clusters = ~384M float operations per iteration. **Orders of magnitude slower than numpy/sklearn**.
- **ESM embeddings** (`engineering.py:113`): Loads full ESM model per sequence. No batching. Each sequence = full forward pass. At 10k sequences: **~hours**.

### 4d. Training Workloads

- **Training assembler** (`training/assembler.py:252-261`): For each assay, iterates all interfaces for that PDB ID (nested loop). With 1000 interfaces per PDB: O(assays × interfaces).
- **Feature builder** (`features/builder.py:462-498`): Same pattern — inner loop over `bound_objects` per assay.

## 5. Memory Usage Risks

| Risk | Location | Impact |
|------|----------|--------|
| **All JSON loaded at once** | Every `_load_table_json()` call | 10k entries × ~50KB JSON each = ~500MB just for entries |
| **All graph nodes/edges in memory** | `graph/builder.py:549-555` | Large knowledge graphs with STRING/Reactome enrichment can reach millions of edges |
| **Frozenset k-mer storage** | `splits.py:170-173` | 100k sequences × ~295 k-mers × ~40 bytes/string = ~1.2GB |
| **Pydantic model instantiation** | Throughout pipeline | Each CanonicalBindingSample / EntryRecord creates ~50 validated fields |
| **Atom-level node lists** | `structural_graphs.py:507` | 100k atoms × ~15 fields × dict overhead = ~200MB per large structure |
| **Full mmCIF text in memory** | `mmcif_supplement.py:206` | Large structures: 50-200MB mmCIF text |

## 6. I/O Inefficiencies

### 6a. One JSON File Per PDB Per Table
`write_records_json()` writes **6 separate JSON files per PDB entry**. For 10k entries = **60k small files**. Directory listing and re-reading becomes the bottleneck.

### 6b. `indent=2` Everywhere
All JSON output uses `json.dumps(..., indent=2)`. Inflates file sizes ~3x vs compact JSON, slows serialization by ~30-40%.

### 6c. Parquet Written But Not Read
`structural_graphs.py` writes nodes/edges as Parquet, but all downstream consumers read JSON. The Parquet files are never used.

### 6d. No Streaming I/O
Every file is read entirely with `path.read_text()` then `json.loads()`. No streaming JSON parser. A 500MB graph_nodes.json requires 500MB for the raw string + 500MB+ for parsed objects.

## 7. Optimization Recommendations

### Priority 1 — Critical (10-100x improvement)

| # | Recommendation | Effort | Impact |
|---|---------------|--------|--------|
| **O1** | **Use scipy.spatial.cKDTree for graph edge construction.** Replace O(n²) brute-force with KD-tree radius query. `cKDTree.query_ball_point(radius)` reduces atom-level graphs from hours to seconds. | Medium | **100-1000x** for atom-level graphs |
| **O2** | **Parse mmCIF once, cache the gemmi.Structure object.** Create a `StructureCache` that loads each structure once and passes it to supplement, quality, graph, and feature stages. | Low | **3-4x** on structure-heavy pipeline |
| **O3** | **Add concurrent.futures.ThreadPoolExecutor for HTTP downloads.** Structure file downloads are I/O-bound. 8-16 concurrent downloads. | Low | **8-16x** download speed |

### Priority 2 — High (3-10x improvement)

| # | Recommendation | Effort | Impact |
|---|---------------|--------|--------|
| **O4** | **Unify `_load_table_json()` into a cached data loader.** Single load of extracted tables shared across graph, feature, and assembly stages via a `PipelineContext` object. | Medium | **3-4x** end-to-end I/O |
| **O5** | **Replace custom k-means with numpy/sklearn.** `_kmeans` in `engineering.py` should use `sklearn.cluster.KMeans` or at minimum numpy vectorization. | Low | **50-100x** for clustering |
| **O6** | **Batch ESM inference.** Use `batch_converter` with batches of 32+ sequences instead of one at a time. | Medium | **10-30x** for ESM embeddings |
| **O7** | **Use Parquet/Arrow for intermediate tables instead of per-PDB JSON.** Replace 60k small JSON files with 6 Parquet tables. Enables columnar reads and predicate pushdown. | High | **5-10x** I/O, **90%** storage reduction |

### Priority 3 — Medium (incremental improvements)

| # | Recommendation | Effort | Impact |
|---|---------------|--------|--------|
| **O8** | **Add `requests.Session()` with retry/backoff.** Connection pooling + `urllib3.Retry` strategy. | Low | **1.5-2x** HTTP throughput |
| **O9** | **Remove `indent=2` from non-human-readable output.** Use compact JSON for data files; keep indented JSON only for manifests. | Trivial | **30-40%** disk space, **30%** faster writes |
| **O10** | **Add MMseqs2 fast path for clustering at >10k sequences.** Already noted as TODO in `splits.py:27`. | High | **100x** for large-scale splits |
| **O11** | **Pre-index bound_objects and interfaces by PDB ID** in feature builder and assembler instead of linear scanning per assay. | Low | **2-5x** for assembly |
| **O12** | **Multiprocess graph construction.** Each structure's graph is independent — use ProcessPoolExecutor with `max_workers=cpu_count`. | Medium | **4-8x** on multi-core |

## 8. Estimated Performance Gains

| Scenario | Current Est. | After Priority 1-2 | Speedup |
|----------|-------------|-------------------|---------|
| **100 entries, residue graphs** | ~5 min | ~30s | **10x** |
| **1,000 entries, residue graphs** | ~2 hr | ~5 min | **24x** |
| **1,000 entries, atom graphs** | ~days | ~20 min | **100x+** |
| **10,000 entries, full pipeline** | infeasible | ~2-4 hr | **enables** |
| **100k sequence clustering** | ~4-8 hr | ~2 min (MMseqs2) | **100x+** |
| **10k ESM embeddings** | ~6-10 hr | ~20-30 min (batched) | **20x** |
| **End-to-end I/O (1k entries)** | ~30 min (repeated loads) | ~5 min (cached + Parquet) | **6x** |

### Summary

The three highest-leverage fixes are:
1. **KD-tree for spatial queries** — unlocks atom-level graphs at any scale
2. **Single mmCIF parse + structure cache** — eliminates 75% of redundant file parsing
3. **Concurrent HTTP downloads** — immediate 8-16x on the most time-consuming I/O stage

These three changes alone would make the pipeline viable for 10k+ entry datasets with no architectural changes.
