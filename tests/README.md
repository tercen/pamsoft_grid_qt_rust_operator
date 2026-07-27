# tests/ — work in progress

`test.json` + `test1_in.csv` + `alex_test_chip.zip` are complete and describe a real
run of this operator, but **`outputDataUri` is still empty** — the expected-output CSV
has not been captured yet, so this fixture does not yet validate results.

## What is here

- `alex_test_chip.zip` — 4 TIFFs (barcode 190007601, W1–W4) + the Array Layout, taken
  from the `tercen/pamsoft_grid_rust` crate's `test/data` chip
  (`190007601_190007602_190007603-on read test ALEX`). 4x12 spot grid + 8 `#REF`,
  552x413 16-bit images, Evolve3 spot pitch ~17.1 px.
- `test1_in.csv` — 2016 rows = 4 images x 56 layout spots x 9 gathered variables.
  Stands in for the upstream `pamsoft_grid` + Gather output. `gridX`/`gridY` were
  derived from the reference image (W4, per `grdUseImage: "Last"`) by intensity-
  projection peak detection plus a local centroid refinement; `diameter` is
  `pitch * 0.66` (the operator's `Spot Size` default). `grdRotation`,
  `grdXFixedPosition`, `grdYFixedPosition`, `bad`, `empty`, `manual` are 0.
- `test.json` — projection mapping: the 6 required column factors, `variable` as the
  row factor, `value` as yAxis. `equalityMethod: "R2"` with `r2: 0.99` because the
  operator emits computed floats.

## To finish it

1. Run this operator on `test1_in.csv` + `alex_test_chip.zip` in a real data step.
2. Export the result table to `tests/test1_out_1.csv`.
3. Set `"outputDataUri": ["test1_out_1.csv"]` in `test.json`.
4. Re-install with `tercenctl operator install -r <repo> -t <tag>` and confirm the
   test passes.

Note: the unit-test runner's own project is deleted when the test finishes, so the
expected output cannot be harvested from a test run — it has to come from a normal
workflow step.
