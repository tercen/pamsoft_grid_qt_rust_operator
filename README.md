# pamsoft_grid_qt_rust_operator

Tercen operator for peptide-microarray **quantification**. Companion to
[`pamsoft_grid_rust_operator`](https://github.com/tercen/pamsoft_grid_rust_operator):
takes that step's melted grid output joined with the source images and
produces per-spot quantification stats (mean / median signal, background,
saturation, diameter, etc.).

A direct replacement for [`pamsoft_grid_qt_operator`](https://github.com/pamgene/pamsoft_grid_qt_operator)
(R/MATLAB-MCR), with the same input contract and column schema but no
MATLAB dependency at runtime — the algorithm is Rust calling into
OpenCV via the [`pamsoft_grid`](https://github.com/tercen/pamsoft_grid_rust)
library crate.

## Input contract

The operator step's input is a crosstab whose **column factor** has:

- **1 or 2 `documentId`-typed columns** — the image ZIP, and optionally
  the array-layout `.txt` file. If there's only one documentId, the
  layout is expected to live inside the image ZIP.
- **`grdImageNameUsed`** — the chip's reference image (constant per
  chip across cycles/exposures).
- **`Image`** — the per-image filename stem (one per `.ci`).
- **`spotRow`, `spotCol`** — the integer grid row/column for each spot.
- **`ID`** — the spot identifier (e.g. peptide name; `#REF` for the
  reference spot).

And whose **row factor** has:

- **`variable`** — the name of the gathered grid output column (produced
  by an upstream `Gather` step on the `pamsoft_grid_operator` result):
  one of `gridX`, `gridY`, `diameter`, `grdRotation`,
  `grdXFixedPosition`, `grdYFixedPosition`, `bad`, `empty`, `manual`.

Main data: `.ci`, `.ri`, `.y` (the gathered value).

This matches the legacy R operator's input shape one-for-one.

## Properties

| Name | Type | Default | Description |
|---|---|---:|---|
| `Min Diameter` | Double | 0.45 | Lower bound for the segmented spot diameter (fraction of pitch). |
| `Max Diameter` | Double | 0.85 | Upper bound for the segmented spot diameter. |
| `Saturation Limit` | Double | 4095 | Saturation cutoff in raw pixel intensity. |
| `Spot Pitch` | Double | 0 | Distance between adjacent spots in pixels. `0` = auto-detect (Evolve2 = 21.5, Evolve3 = 17.0). |
| `EdgeSensitivityLow` | Double | 0 | Canny edge low threshold. R hardcodes 0; Rust honours the property. |
| `Edge Sensitivity` | Double | 0.05 | Canny edge high threshold. |
| `Spot Size` | Double | 0.66 | Spot radius as a fraction of `Spot Pitch`. |
| `Segmentation Method` | Enum | `Edge` | `Edge` or `Hough`. |
| `Rotation` | String | `-2:0.25:2` | Rotation candidates in `min:step:max` syntax. |
| `Diagnostic Output` | Enum | `Yes` | When `No`, the result drops `Bad_Spot`, `Diameter`, `Empty_Spot`, `Fraction_Ignored`, `Position_Offset`, `Replaced_Spot`, `Mean_Background`, `Mean_SigmBg`, `Mean_Signal`. |

## Local development

The operator's binary entry point is `src/main.rs`; the production
container at `pamgene/pamsoft_grid_qt_rust_operator:<tag>` runs it with
`--taskId`, `--serviceUri`, `--token` injected by Tercen.

For local iteration against a real workflow without going through
Tercen's task-spawn machinery, the `dev` binary takes `WORKFLOW_ID` and
`STEP_ID` env vars and runs the same pipeline:

```bash
export TERCEN_URI=https://pamgene.tercen.com:443
export TERCEN_TOKEN=<your token>
export WORKFLOW_ID=<your workflow id>
export STEP_ID=<your step id>
cargo run --bin dev
```

## Build

System prerequisites (Debian/Ubuntu):

```
sudo apt install libopencv-dev libclang-dev clang pkg-config protobuf-compiler
```

Then:

```
cargo build --release --bin pamsoft_grid_qt_operator
```

The `pamsoft_grid` library dep brings in OpenCV bindings — first
build takes ~10 minutes, incremental rebuilds are seconds.

## CI / release

- **Every push to `main`** → `ci.yml` builds the Docker image and
  pushes `pamgene/pamsoft_grid_qt_rust_operator:<commit-sha>` and
  `:latest`.
- **Pushing a semver tag** (`0.1.0`, `0.2.0`, …) → `release.yml`
  rewrites `operator.json.container` to the tagged image, builds,
  pushes `:<tag>` and `:latest`, and creates a GitHub Release.

Install in Tercen via the UI or:

```
tercenctl operator install --repo https://github.com/tercen/pamsoft_grid_qt_rust_operator --tag <version>
```

## Architecture

```
Tercen task invocation
       │
       ▼
src/main.rs ─── parse --taskId/--serviceUri/--token → env vars
       │
       ▼
src/lib.rs::run  →  ProductionContext::from_task_id
       │            (or DevContext::from_workflow_step for the dev binary)
       ▼
src/lib.rs::execute
   │
   ├── src/props.rs        — read operator properties
   ├── src/input.rs        — stream the melted grid input + pivot back to per-.ci records
   ├── src/download.rs     — fetch + extract documentId ZIPs
   ├── src/algorithm.rs    — pamsoft_grid::batch::process_single_group_qt
   │                          (per chip, all images, using the upstream grid)
   ├── src/output.rs       — build the per-spot quantification DataFrame
   └── src/upload.rs       — save_table back to Tercen
```
