//! Input table reading for the pamsoft_grid_qt operator.
//!
//! The QT operator's input is the *melted* output of the upstream
//! pamsoft_grid step joined with the source images. Three Tercen tables
//! to stream:
//!
//!   * **Column-facet** (`column_hash`): one row per `(image × spot)`
//!     combination — i.e. one per crosstab column. Carries
//!     `documentId(s)`, `grdImageNameUsed`, `Image`, `spotRow`,
//!     `spotCol`, `ID`.
//!   * **Row-facet** (`row_hash`): one row per gathered variable name.
//!     Single column `variable` (typically namespace-prefixed by the
//!     Gather step, e.g. `"ds1.gridX"`).
//!   * **Main data** (`qt_hash`): `.ci`, `.ri`, `.y` — the actual
//!     gathered value at each `(column, row)` cell.
//!
//! We denormalize all three tables into one `Vec<QtInputRow>` (one row
//! per `.ci`), looking up each of the 9 grid variables via
//! `main_data[(ci, ri_for_variable)]`. R does the same thing via two
//! `left_join`s in main.R:367-379.
//!
//! Required column factors: `grdImageNameUsed`, `Image`, `spotRow`,
//! `spotCol`, `ID` (plus 1-2 `documentId` columns).
//!
//! Required row factor: `variable` carrying all 9 of `gridX`, `gridY`,
//! `diameter`, `grdRotation`, `grdXFixedPosition`, `grdYFixedPosition`,
//! `bad`, `empty`, `manual` (matching `req.variables` at R main.R:355).
//!
//! Errors loudly when the input shape doesn't match — wrong number of
//! doc-ID columns, missing factors, missing variables, etc.

use anyhow::{anyhow, bail, Context, Result};
use polars::prelude::*;
use std::collections::{BTreeSet, HashMap};
use tercen_rs::context::ContextBase;
use tercen_rs::tson_to_dataframe;

/// The 9 variables we need from the gathered grid output. Matches
/// `req.variables` at R main.R:355.
pub const REQUIRED_VARIABLES: &[&str] = &[
    "gridX",
    "gridY",
    "diameter",
    "grdRotation",
    "grdXFixedPosition",
    "grdYFixedPosition",
    "bad",
    "empty",
    "manual",
];

/// One column-facet row (one `(image × spot)` combination) fully
/// denormalized with all 9 gathered variables looked up from the main
/// data table.
#[derive(Debug, Clone)]
pub struct QtInputRow {
    /// Crosstab column index — positional row index in the column-facet
    /// table. The unit of work for output table construction: one
    /// `(image × spot)` per `.ci`.
    pub ci: i32,
    /// Document IDs referenced by this row (1 or 2 values). The first
    /// is the image ZIP; the second, if present, is the array-layout
    /// text file.
    pub document_ids: Vec<String>,
    /// `grdImageNameUsed` — the chip's reference image (constant across
    /// all `.ci`s belonging to the same chip).
    pub grd_image_name: String,
    /// `Image` — the filename stem of this individual image.
    pub image_label: String,
    /// `spotRow` (integer grid row).
    pub spot_row: i32,
    /// `spotCol` (integer grid column).
    pub spot_col: i32,
    /// `ID` — spot identifier (peptide name, `#REF`, …).
    pub spot_id: String,
    // 9 gathered variables. Within a chip these are constant across
    // images per spot — same `(spotRow, spotCol)` always yields the
    // same values regardless of which `.ci`'s image we look at.
    pub grid_x: f64,
    pub grid_y: f64,
    pub diameter: f64,
    pub rotation: f64,
    pub grd_x_fixed: f64,
    pub grd_y_fixed: f64,
    /// `bad` — R round-trips through `as.double(as.logical(...))`; we
    /// keep the same f64 representation (0.0 or 1.0).
    pub bad: f64,
    pub empty: f64,
    pub manual: f64,
}

/// All denormalized input rows + schema introspection results.
#[derive(Debug, Clone)]
pub struct InputData {
    /// One row per `.ci`, ordered by `.ci` ascending.
    pub rows: Vec<QtInputRow>,
    /// Names of the documentId columns in the column-facet schema, in
    /// schema order. `len()` is always 1 or 2.
    pub document_id_columns: Vec<String>,
    /// Diagnostic string describing the QT main-data TSON shape — what
    /// the manual decoder saw at the top level (MAP vs LST), the
    /// top-level keys, per-column key sets and value variants, and
    /// column lengths. Captured into the output table so we can debug
    /// the chip-11+ NaN-fill bug from the operator's result, since
    /// Tercen GCs the stderr.log within minutes of task completion.
    pub tson_diagnostic: String,
}

impl InputData {
    /// Number of `(image × spot)` rows in the input.
    pub fn n_rows(&self) -> usize {
        self.rows.len()
    }

    /// Number of distinct chips (unique `grdImageNameUsed` values).
    pub fn n_chips(&self) -> usize {
        let mut seen = std::collections::HashSet::new();
        for r in &self.rows {
            seen.insert(r.grd_image_name.as_str());
        }
        seen.len()
    }

    /// Every unique documentId across all rows (image ZIPs and the
    /// optional layout file). Stage 4 downloads each once.
    pub fn unique_document_ids(&self) -> BTreeSet<String> {
        self.rows
            .iter()
            .flat_map(|r| r.document_ids.iter().cloned())
            .collect()
    }
}

/// Required (un-namespaced) column-facet factor names. The schema names
/// actually carry a namespace prefix (e.g. `ds1.grdImageNameUsed`), so
/// we look them up by `endsWith` — same as R main.R:331-335.
const REQUIRED_CNAMES: &[&str] = &["grdImageNameUsed", "Image", "spotRow", "spotCol", "ID"];

/// Stream the three input tables and denormalize them into
/// `Vec<QtInputRow>`. See module-level docs for the full pipeline.
pub async fn load_input_data(ctx: &ContextBase) -> Result<InputData> {
    let col_table_id = ctx.cube_query().column_hash.clone();
    let row_table_id = ctx.cube_query().row_hash.clone();
    let qt_table_id = ctx.cube_query().qt_hash.clone();

    if col_table_id.is_empty() {
        bail!(
            "operator has no column-facet table (cube_query.column_hash is empty). \
             The QT operator expects column factors carrying grdImageNameUsed, \
             Image, spotRow, spotCol, ID, plus 1-2 documentId columns."
        );
    }
    if row_table_id.is_empty() {
        bail!(
            "operator has no row-facet table (cube_query.row_hash is empty). \
             The QT operator expects a 'variable' row factor from an upstream \
             Gather step on the pamsoft_grid output."
        );
    }
    if qt_table_id.is_empty() {
        bail!("operator has no main data table (cube_query.qt_hash is empty).");
    }

    // --- Resolve all required column-facet factor names by endsWith ---
    let all_cnames = ctx
        .cnames()
        .await
        .map_err(|e| anyhow!("fetch column-facet schema: {e}"))?;
    let document_id_columns: Vec<String> = all_cnames
        .iter()
        .filter(|c| c.contains("documentId"))
        .cloned()
        .collect();
    if document_id_columns.is_empty() || document_id_columns.len() > 2 {
        bail!(
            "expected 1 or 2 documentId columns on the column-facet table, found {} ({:?})",
            document_id_columns.len(),
            document_id_columns,
        );
    }
    let resolved_cnames: Vec<String> = REQUIRED_CNAMES
        .iter()
        .map(|req| {
            all_cnames
                .iter()
                .find(|c| c.ends_with(req) || c.as_str() == *req)
                .cloned()
                .ok_or_else(|| {
                    anyhow!(
                        "required column factor '{}' not found on column-facet \
                         (available: {:?}). Make sure the upstream Gather step \
                         keeps grdImageNameUsed, Image, spotRow, spotCol, and ID \
                         as column factors.",
                        req,
                        all_cnames,
                    )
                })
        })
        .collect::<Result<Vec<_>>>()?;

    // --- Resolve 'variable' on the row facet ---
    let all_rnames = ctx
        .rnames()
        .await
        .map_err(|e| anyhow!("fetch row-facet schema: {e}"))?;
    let variable_col = all_rnames
        .iter()
        .find(|r| r.ends_with("variable") || r.as_str() == "variable")
        .cloned()
        .ok_or_else(|| {
            anyhow!(
                "required row factor 'variable' not found (available: {:?}). \
                 Add a Gather step upstream that turns the grid output's \
                 numeric columns into a 'variable' row factor.",
                all_rnames,
            )
        })?;

    // --- Stream column-facet ---
    let streamer = ctx.streamer();
    let mut col_cols: Vec<String> = resolved_cnames.clone();
    col_cols.extend(document_id_columns.iter().cloned());
    let col_tson = streamer
        .stream_tson(&col_table_id, Some(col_cols.clone()), 0, -1)
        .await
        .map_err(|e| anyhow!("stream column-facet table {col_table_id}: {e}"))?;
    let col_df = tson_to_dataframe(&col_tson).context("parse TSON column-facet payload")?;

    let n_cols = col_df.height();
    if n_cols == 0 {
        bail!("column-facet table is empty — no (image × spot) rows to process.");
    }

    // Materialize all the typed series once. We hold owned `Series`
    // (not borrowed) so the chunked accessors below stay valid through
    // the per-row loop.
    let grd_image_s = owned_str(&col_df, &resolved_cnames[0])?;
    let image_s = owned_str(&col_df, &resolved_cnames[1])?;
    let spot_row_s = owned_i32(&col_df, &resolved_cnames[2])?;
    let spot_col_s = owned_i32(&col_df, &resolved_cnames[3])?;
    let spot_id_s = owned_str(&col_df, &resolved_cnames[4])?;
    let doc_series: Vec<Series> = document_id_columns
        .iter()
        .map(|name| owned_str(&col_df, name))
        .collect::<Result<Vec<_>>>()?;

    // --- Stream row-facet: variable per .ri (positional) ---
    let row_tson = streamer
        .stream_tson(&row_table_id, Some(vec![variable_col.clone()]), 0, -1)
        .await
        .map_err(|e| anyhow!("stream row-facet table {row_table_id}: {e}"))?;
    let row_df = tson_to_dataframe(&row_tson).context("parse TSON row-facet payload")?;
    let n_var_rows = row_df.height();
    if n_var_rows == 0 {
        bail!("row-facet table is empty — no gathered variables to process.");
    }
    let var_s = owned_str(&row_df, &variable_col)?;
    let var_chunked = var_s.str().context("variable is not string")?;
    // Build `variable_name → ri`. Strip namespace prefix (e.g.
    // `"ds1.gridX"` → `"gridX"`) — R main.R:379 does the same via
    // `stri_split_fixed(..., ".", 2)[,2]`.
    let mut variable_to_ri: HashMap<String, i32> = HashMap::with_capacity(n_var_rows);
    for ri in 0..n_var_rows {
        let v = var_chunked
            .get(ri)
            .ok_or_else(|| anyhow!("null variable value at .ri={ri}"))?;
        variable_to_ri.insert(strip_namespace(v).to_string(), ri as i32);
    }
    // Sanity-check that every required variable is present.
    for req in REQUIRED_VARIABLES {
        if !variable_to_ri.contains_key(*req) {
            let mut found: Vec<&str> = variable_to_ri.keys().map(String::as_str).collect();
            found.sort_unstable();
            bail!(
                "required variable '{}' not found in row-facet (got: {:?}). \
                 The upstream Gather step must include all 9 of {:?}.",
                req,
                found,
                REQUIRED_VARIABLES,
            );
        }
    }

    // --- Stream main data table ---
    let qt_tson = streamer
        .stream_tson(
            &qt_table_id,
            Some(vec![".ci".to_string(), ".ri".to_string(), ".y".to_string()]),
            0,
            -1,
        )
        .await
        .map_err(|e| anyhow!("stream main data table {qt_table_id}: {e}"))?;
    // Manual TSON decode: tercen-rs' `tson_to_dataframe` reads
    // `col["data"]` per column, but the Tercen TSON encoding uses
    // `col["values"]` (confirmed by reading rustson's test fixtures +
    // the Python SDK's `tson_to_polars`). The wrong key produces a
    // short / null-padded `.y` column past ~14976 rows on this dataset,
    // which is the bug that made every chip 11+ NaN-fill in the
    // operator's QT output.
    //
    // If the manual decoder fails (e.g. the actual TSON has yet a
    // third structure we haven't covered yet), fall back to the
    // tercen-rs path so the operator still produces output. The
    // diagnostic logs from `decode_main_data_yy` will tell us what
    // shape the TSON actually has so we can fix it.
    let mut tson_diagnostic = String::with_capacity(2048);
    use std::fmt::Write as _;
    let yy = match decode_main_data_yy(&qt_tson, &mut tson_diagnostic) {
        Ok(map) => {
            let _ = writeln!(tson_diagnostic, "OK manual yy_size={}", map.len());
            map
        }
        Err(e) => {
            let _ = writeln!(tson_diagnostic, "MANUAL_ERR {e:#}");
            tracing::error!(
                "manual TSON decode failed: {e:#}. Falling back to tercen-rs \
                 tson_to_dataframe path — chips 11+ will likely NaN-fill in \
                 the QT output, but the operator will at least produce SOMETHING."
            );
            let map = fallback_yy_via_dataframe(&qt_tson, &mut tson_diagnostic)?;
            let _ = writeln!(tson_diagnostic, "FALLBACK yy_size={}", map.len());
            map
        }
    };
    if yy.is_empty() {
        bail!("main data table yielded zero non-null (.ci, .ri, .y) tuples.");
    }
    // Diagnostic: yy map size + per-ri ci coverage + sample lookups.
    tracing::info!(yy_size = yy.len(), "QT main data yy map built");
    // Count distinct .ci per .ri so we can see if some variables only
    // got partial coverage from the TSON stream.
    let mut per_ri: std::collections::HashMap<i32, usize> = std::collections::HashMap::new();
    let mut max_ci_per_ri: std::collections::HashMap<i32, i32> = std::collections::HashMap::new();
    for (&(_ci, ri), _) in &yy {
        *per_ri.entry(ri).or_insert(0) += 1;
        let e = max_ci_per_ri.entry(ri).or_insert(0);
        if _ci > *e { *e = _ci; }
    }
    let mut keys: Vec<i32> = per_ri.keys().copied().collect();
    keys.sort();
    for ri in keys {
        tracing::info!(
            ri,
            n_ci = per_ri[&ri],
            max_ci = max_ci_per_ri[&ri],
            "yy per-ri coverage"
        );
    }
    // Sample lookups for known-finite cells (per MCP export):
    for (ci_target, ri_target) in [(0i32, 6i32), (152, 6), (1520, 6), (1672, 6), (2000, 6), (5000, 6)] {
        match yy.get(&(ci_target, ri_target)) {
            Some(v) => tracing::info!(ci = ci_target, ri = ri_target, value = v, "yy sample"),
            None => tracing::warn!(ci = ci_target, ri = ri_target, "yy sample: MISSING"),
        }
    }

    // --- Denormalize ---
    let grd_image_chunked = grd_image_s.str().context("grdImageNameUsed is not string")?;
    let image_chunked = image_s.str().context("Image is not string")?;
    let spot_row_chunked = spot_row_s.i32().context("spotRow is not int32")?;
    let spot_col_chunked = spot_col_s.i32().context("spotCol is not int32")?;
    let spot_id_chunked = spot_id_s.str().context("ID is not string")?;
    let doc_chunked: Vec<&StringChunked> = doc_series
        .iter()
        .map(|s| s.str().context("documentId is not string"))
        .collect::<Result<Vec<_>>>()?;

    let ri_grid_x = variable_to_ri["gridX"];
    let ri_grid_y = variable_to_ri["gridY"];
    let ri_diameter = variable_to_ri["diameter"];
    let ri_rotation = variable_to_ri["grdRotation"];
    let ri_xfix = variable_to_ri["grdXFixedPosition"];
    let ri_yfix = variable_to_ri["grdYFixedPosition"];
    let ri_bad = variable_to_ri["bad"];
    let ri_empty = variable_to_ri["empty"];
    let ri_manual = variable_to_ri["manual"];

    let mut rows = Vec::with_capacity(n_cols);
    for ci_usize in 0..n_cols {
        let ci = ci_usize as i32;
        let grd_image_name = grd_image_chunked
            .get(ci_usize)
            .ok_or_else(|| anyhow!("null grdImageNameUsed at .ci={ci}"))?
            .to_string();
        let image_label = image_chunked
            .get(ci_usize)
            .ok_or_else(|| anyhow!("null Image at .ci={ci}"))?
            .to_string();
        let spot_row = spot_row_chunked
            .get(ci_usize)
            .ok_or_else(|| anyhow!("null spotRow at .ci={ci}"))?;
        let spot_col = spot_col_chunked
            .get(ci_usize)
            .ok_or_else(|| anyhow!("null spotCol at .ci={ci}"))?;
        let spot_id = spot_id_chunked
            .get(ci_usize)
            .ok_or_else(|| anyhow!("null ID at .ci={ci}"))?
            .to_string();
        let document_ids: Vec<String> = doc_chunked
            .iter()
            .map(|s| {
                s.get(ci_usize)
                    .ok_or_else(|| anyhow!("null documentId at .ci={ci}"))
                    .map(String::from)
            })
            .collect::<Result<Vec<_>>>()?;

        // Sparse cells (null .y) become NaN — matches R's `left_join`
        // behaviour. The upstream grid step legitimately emits NaN for
        // spots it couldn't place (empty/bad/replaced spots), and
        // Tercen materializes those as null cells that `tson_to_dataframe`
        // skips. Don't bail here; downstream consumers (the grid CSV
        // writer + the QT algorithm) handle NaN correctly.
        let lookup = |ri: i32| -> f64 { yy.get(&(ci, ri)).copied().unwrap_or(f64::NAN) };

        rows.push(QtInputRow {
            ci,
            document_ids,
            grd_image_name,
            image_label,
            spot_row,
            spot_col,
            spot_id,
            grid_x: lookup(ri_grid_x),
            grid_y: lookup(ri_grid_y),
            diameter: lookup(ri_diameter),
            rotation: lookup(ri_rotation),
            grd_x_fixed: lookup(ri_xfix),
            grd_y_fixed: lookup(ri_yfix),
            bad: lookup(ri_bad),
            empty: lookup(ri_empty),
            manual: lookup(ri_manual),
        });
    }

    Ok(InputData {
        rows,
        document_id_columns,
        tson_diagnostic,
    })
}

/// Strip the dataset-namespace prefix from a variable name, e.g.
/// `"ds1.gridX"` → `"gridX"`. Mirrors R main.R:379. If no `.` is
/// present, returns the input unchanged (safer than R's behaviour,
/// which silently produces `""`).
fn strip_namespace(s: &str) -> &str {
    s.split_once('.').map(|(_, rest)| rest).unwrap_or(s)
}

/// Pull a string column out of a `DataFrame` as an owned `Series`,
/// casting if necessary. The owned `Series` is needed because polars'
/// `&StringChunked` accessors borrow from a `Series` that has to
/// outlive them.
fn owned_str(df: &DataFrame, name: &str) -> Result<Series> {
    let col = df
        .column(name)
        .map_err(|e| anyhow!("missing column '{}': {}", name, e))?
        .cast(&DataType::String)
        .with_context(|| format!("cast column '{}' to string", name))?;
    Ok(col.take_materialized_series())
}

fn owned_i32(df: &DataFrame, name: &str) -> Result<Series> {
    let col = df
        .column(name)
        .map_err(|e| anyhow!("missing column '{}': {}", name, e))?
        .cast(&DataType::Int32)
        .with_context(|| format!("cast column '{}' to int32", name))?;
    Ok(col.take_materialized_series())
}

/// Decode the QT main-data TSON payload into a `(.ci, .ri) → .y` map
/// manually, reading the **correct** column-values key (`"values"`).
///
/// Why this exists: `tercen_rs::tson_to_dataframe` reads `col["data"]`,
/// but Tercen's TSON main-data tables use `col["values"]` (confirmed
/// by rustson's test fixtures + the Python SDK's `tson_to_polars`).
/// Using the wrong key silently produces a short / null-padded `.y`
/// column past ~14976 rows on production data, and every chip past
/// that row index NaN-fills downstream.
///
/// Structure expected:
///
/// ```text
/// MAP {
///   "cols": LST [
///     MAP { "name": "ci", "values": LSTI32 [...] },
///     MAP { "name": "ri", "values": LSTI32 [...] },
///     MAP { "name": "y",  "values": LSTF64 [...] },
///   ]
/// }
/// ```
///
/// Walks the column list, finds `.ci`/`.ri`/`.y` by their `name`
/// field, validates that all three have the same length, then walks
/// row-by-row to build the lookup map. Logs the per-column lengths
/// + null counts for visibility.
fn decode_main_data_yy(
    tson_bytes: &[u8],
    diag: &mut String,
) -> Result<HashMap<(i32, i32), f64>> {
    use rustson::Value;
    use std::fmt::Write;
    let root = rustson::decode_bytes(tson_bytes)
        .map_err(|e| anyhow!("rustson decode_bytes: {:?}", e))?;
    let _ = writeln!(
        diag,
        "bytes={} root_kind={}",
        tson_bytes.len(),
        root_kind_name(&root)
    );
    tracing::info!(
        bytes = tson_bytes.len(),
        root_kind = root_kind_name(&root),
        "TSON root decoded"
    );

    // Find the column list. Tercen TSON appears in two shapes in the
    // wild: `MAP { "cols": LST [...] }` (what tercen-rs's
    // tson_to_dataframe expects) or `MAP { "columns": LST [...] }`
    // (what Python's tson_to_polars expects). Also handle the case
    // where the top level is the LST itself.
    let cols_owned: Vec<Value> = match &root {
        Value::MAP(m) => {
            let keys: Vec<String> = m.keys().cloned().collect();
            let _ = writeln!(diag, "root_map_keys={:?}", keys);
            tracing::info!(top_keys = ?keys, "TSON root is MAP");
            if let Some(Value::LST(l)) = m.get("cols") {
                let _ = writeln!(diag, "cols_lst_len={}", l.len());
                tracing::info!(n_cols = l.len(), "found `cols` key");
                l.clone()
            } else if let Some(Value::LST(l)) = m.get("columns") {
                let _ = writeln!(diag, "columns_lst_len={}", l.len());
                tracing::info!(n_cols = l.len(), "found `columns` key");
                l.clone()
            } else {
                bail!(
                    "no `cols` or `columns` LST in TSON root MAP (keys: {:?})",
                    keys
                );
            }
        }
        Value::LST(l) => {
            let _ = writeln!(diag, "root_lst_len={}", l.len());
            tracing::info!(n_cols = l.len(), "TSON root is bare LST");
            l.clone()
        }
        other => bail!("expected TSON MAP or LST at root, got {}", root_kind_name(other)),
    };
    tracing::info!(n_cols = cols_owned.len(), "TSON cols list");

    let mut ci: Option<Vec<i32>> = None;
    let mut ri: Option<Vec<i32>> = None;
    let mut y: Option<Vec<f64>> = None;
    for (idx, col) in cols_owned.iter().enumerate() {
        let Value::MAP(col_map) = col else {
            tracing::warn!(idx, kind = root_kind_name(col), "col is not a MAP — skipping");
            continue;
        };
        let col_keys: Vec<String> = col_map.keys().cloned().collect();
        let name = col_map
            .get("name")
            .or_else(|| col_map.get("Name"))
            .and_then(|v| if let Value::STR(s) = v { Some(s.clone()) } else { None });
        // Try the two known keys for the actual data: `values` (Python
        // SDK convention, also what rustson tests use) and `data`
        // (tercen-rs convention). Pick whichever is present.
        let values_val = col_map.get("values");
        let data_val = col_map.get("data");
        let values = values_val.or(data_val);
        let value_kind = values.map(root_kind_name).unwrap_or("None");
        let values_len = values.and_then(typed_data_len).unwrap_or(0);
        // Critical for the bug: if both `values` and `data` exist with
        // DIFFERENT lengths, that's the smoking gun for the tercen-rs
        // tson_to_dataframe truncation. Capture both side by side.
        let values_key_len = values_val.and_then(typed_data_len).unwrap_or(0);
        let data_key_len = data_val.and_then(typed_data_len).unwrap_or(0);
        let _ = writeln!(
            diag,
            "col[{}] name={:?} keys={:?} value_kind={} chosen_len={} values_key_len={} data_key_len={}",
            idx,
            name.as_deref().unwrap_or("<unnamed>"),
            col_keys,
            value_kind,
            values_len,
            values_key_len,
            data_key_len,
        );
        tracing::info!(
            idx,
            name = name.as_deref().unwrap_or("<unnamed>"),
            keys = ?col_keys,
            value_kind,
            values_len,
            "col"
        );
        let Some(name) = name else { continue };
        match name.as_str() {
            ".ci" => ci = extract_i32(values)?,
            ".ri" => ri = extract_i32(values)?,
            ".y" => y = extract_f64(values)?,
            _ => {}
        }
    }
    let ci = ci.ok_or_else(|| anyhow!("`.ci` column missing from TSON"))?;
    let ri = ri.ok_or_else(|| anyhow!("`.ri` column missing from TSON"))?;
    let y = y.ok_or_else(|| anyhow!("`.y` column missing from TSON"))?;
    let _ = writeln!(
        diag,
        "decoded ci_len={} ri_len={} y_len={}",
        ci.len(),
        ri.len(),
        y.len()
    );
    tracing::info!(
        ci_len = ci.len(),
        ri_len = ri.len(),
        y_len = y.len(),
        "QT main-data column lengths (manual TSON decode)"
    );
    if !(ci.len() == ri.len() && ri.len() == y.len()) {
        bail!(
            "main-data column lengths still mismatched after manual decode: \
             ci={}, ri={}, y={}",
            ci.len(), ri.len(), y.len()
        );
    }
    let n = ci.len();
    let mut yy: HashMap<(i32, i32), f64> = HashMap::with_capacity(n);
    let mut n_y_nan = 0usize;
    for i in 0..n {
        if y[i].is_nan() {
            n_y_nan += 1;
        }
        yy.insert((ci[i], ri[i]), y[i]);
    }
    tracing::info!(yy_size = yy.len(), n_y_nan, "QT main-data yy map built");
    Ok(yy)
}

/// Fallback that uses tercen-rs's `tson_to_dataframe` path. Same code
/// as before — kept for graceful degradation if the manual decoder
/// can't find the column data. Produces a `(.ci, .ri) → .y` map with
/// (most likely) the broken short-`.y` behaviour that motivated the
/// manual decode in the first place; the operator's output will then
/// be partially NaN-filled, but at least the rest of the pipeline
/// runs and we get diagnostic information.
fn fallback_yy_via_dataframe(
    tson_bytes: &[u8],
    diag: &mut String,
) -> Result<HashMap<(i32, i32), f64>> {
    use std::fmt::Write;
    let df = tson_to_dataframe(tson_bytes).context("fallback tson_to_dataframe")?;
    let _ = writeln!(
        diag,
        "FALLBACK df_height={} df_width={} cols={:?}",
        df.height(),
        df.width(),
        df.get_column_names()
    );
    tracing::warn!(
        df_height = df.height(),
        df_width = df.width(),
        df_columns = ?df.get_column_names(),
        "fallback DataFrame parsed (likely short)"
    );
    let ci_s = owned_i32(&df, ".ci")?;
    let ri_s = owned_i32(&df, ".ri")?;
    let y_col = df
        .column(".y")
        .map_err(|e| anyhow!("missing .y column on main table: {e}"))?
        .cast(&DataType::Float64)
        .context("cast .y to float64")?;
    let y_s = y_col.take_materialized_series();
    let ci_chunked = ci_s.i32().context(".ci is not int32")?;
    let ri_chunked = ri_s.i32().context(".ri is not int32")?;
    let y_chunked = y_s.f64().context(".y is not float64")?;
    let n = ci_chunked.len().min(ri_chunked.len()).min(y_chunked.len());
    let mut yy: HashMap<(i32, i32), f64> = HashMap::with_capacity(n);
    for i in 0..n {
        let (Some(ci), Some(ri), Some(y)) = (ci_chunked.get(i), ri_chunked.get(i), y_chunked.get(i))
        else { continue };
        yy.insert((ci, ri), y);
    }
    tracing::warn!(yy_size = yy.len(), "fallback yy map built");
    Ok(yy)
}

/// Short debug name for a TSON Value variant — used when logging the
/// structure we got vs what we expected. Avoids dumping multi-MB data
/// from the actual variant.
fn root_kind_name(v: &rustson::Value) -> &'static str {
    use rustson::Value;
    match v {
        Value::NULL => "NULL",
        Value::STR(_) => "STR",
        Value::I32(_) => "I32",
        Value::F64(_) => "F64",
        Value::BOOL(_) => "BOOL",
        Value::LST(_) => "LST",
        Value::MAP(_) => "MAP",
        Value::LSTU8(_) => "LSTU8",
        Value::LSTI8(_) => "LSTI8",
        Value::LSTU16(_) => "LSTU16",
        Value::LSTI16(_) => "LSTI16",
        Value::LSTU32(_) => "LSTU32",
        Value::LSTI32(_) => "LSTI32",
        Value::LSTU64(_) => "LSTU64",
        Value::LSTI64(_) => "LSTI64",
        Value::LSTF32(_) => "LSTF32",
        Value::LSTF64(_) => "LSTF64",
        Value::LSTSTR(_) => "LSTSTR",
    }
}

/// Length of a typed-array TSON value, or 0 for non-array kinds. Used
/// for diagnostics — we want to see whether `values` and `data` have
/// the same length on each column.
fn typed_data_len(v: &rustson::Value) -> Option<usize> {
    use rustson::Value;
    Some(match v {
        Value::LST(l) => l.len(),
        Value::LSTU8(l) => l.len(),
        Value::LSTI8(l) => l.len(),
        Value::LSTU16(l) => l.len(),
        Value::LSTI16(l) => l.len(),
        Value::LSTU32(l) => l.len(),
        Value::LSTI32(l) => l.len(),
        Value::LSTU64(l) => l.len(),
        Value::LSTI64(l) => l.len(),
        Value::LSTF32(l) => l.len(),
        Value::LSTF64(l) => l.len(),
        // StrVec stores bytes — closest analogue to "length" is byte count.
        Value::LSTSTR(l) => l.bytes.len(),
        _ => return None,
    })
}

/// Pull an i32 array out of a TSON column-values field. Accepts a few
/// integer-typed variants since the encoder may pick a smaller width
/// for compact tables.
fn extract_i32(values: Option<&rustson::Value>) -> Result<Option<Vec<i32>>> {
    use rustson::Value;
    match values {
        None => Ok(None),
        Some(Value::LSTI32(v)) => Ok(Some(v.clone())),
        Some(Value::LSTI16(v)) => Ok(Some(v.iter().map(|&x| x as i32).collect())),
        Some(Value::LSTU16(v)) => Ok(Some(v.iter().map(|&x| x as i32).collect())),
        Some(Value::LSTI64(v)) => Ok(Some(v.iter().map(|&x| x as i32).collect())),
        Some(Value::LSTU32(v)) => Ok(Some(v.iter().map(|&x| x as i32).collect())),
        Some(Value::LSTF64(v)) => Ok(Some(v.iter().map(|&x| x as i32).collect())),
        Some(other) => bail!(
            "expected integer-list for column values, got {:?}",
            std::mem::discriminant(other)
        ),
    }
}

/// Pull an f64 array out of a TSON column-values field.
fn extract_f64(values: Option<&rustson::Value>) -> Result<Option<Vec<f64>>> {
    use rustson::Value;
    match values {
        None => Ok(None),
        Some(Value::LSTF64(v)) => Ok(Some(v.clone())),
        Some(Value::LSTF32(v)) => Ok(Some(v.iter().map(|&x| x as f64).collect())),
        Some(Value::LSTI32(v)) => Ok(Some(v.iter().map(|&x| x as f64).collect())),
        Some(Value::LSTI64(v)) => Ok(Some(v.iter().map(|&x| x as f64).collect())),
        Some(other) => bail!(
            "expected float-list for column values, got {:?}",
            std::mem::discriminant(other)
        ),
    }
}
