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
    let col_df = paged_stream_df(&streamer, &col_table_id, col_cols.clone())
        .await
        .context("stream column-facet table")?;

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
    let row_df = paged_stream_df(&streamer, &row_table_id, vec![variable_col.clone()])
        .await
        .context("stream row-facet table")?;
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

    // --- Stream main data table (paginated; Tercen caps -1 reads at 15k rows) ---
    let qt_df = paged_stream_df(
        &streamer,
        &qt_table_id,
        vec![".ci".to_string(), ".ri".to_string(), ".y".to_string()],
    )
    .await
    .context("stream main data table")?;
    let mut tson_diagnostic = String::with_capacity(256);
    use std::fmt::Write as _;
    let _ = writeln!(
        tson_diagnostic,
        "qt_df rows={} cols={:?} (paged 10k/page until short page)",
        qt_df.height(),
        qt_df.get_column_names()
    );
    let ci_s = owned_i32(&qt_df, ".ci")?;
    let ri_s = owned_i32(&qt_df, ".ri")?;
    let y_col = qt_df
        .column(".y")
        .map_err(|e| anyhow!("missing .y on main data: {e}"))?
        .cast(&DataType::Float64)
        .context("cast .y to f64")?;
    let y_s = y_col.take_materialized_series();
    let ci_c = ci_s.i32().context(".ci not int32")?;
    let ri_c = ri_s.i32().context(".ri not int32")?;
    let y_c = y_s.f64().context(".y not f64")?;
    let n = ci_c.len();
    let mut yy: HashMap<(i32, i32), f64> = HashMap::with_capacity(n);
    for i in 0..n {
        let (Some(ci), Some(ri), Some(y)) = (ci_c.get(i), ri_c.get(i), y_c.get(i)) else {
            continue;
        };
        yy.insert((ci, ri), y);
    }
    let _ = writeln!(tson_diagnostic, "yy_size={}", yy.len());
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

/// Tercen caps single `streamTable` responses at 15 000 rows regardless
/// of `limit = -1`, so unlimited reads silently truncate. R operators
/// work because the R SDK pages internally; Rust's tercen-rs leaves
/// pagination to the caller. We loop with explicit `offset` / `limit`
/// and vstack the resulting DataFrames until a short page signals EOF.
async fn paged_stream_df(
    streamer: &tercen_rs::table::TableStreamer<'_>,
    table_id: &str,
    cols: Vec<String>,
) -> Result<DataFrame> {
    const PAGE: i64 = 10_000;
    let mut offset: i64 = 0;
    let mut acc: Option<DataFrame> = None;
    loop {
        let bytes = streamer
            .stream_tson(table_id, Some(cols.clone()), offset, PAGE)
            .await
            .map_err(|e| anyhow!("stream {table_id} @ offset {offset}: {e}"))?;
        if bytes.is_empty() {
            break;
        }
        let df = tson_to_dataframe(&bytes)
            .with_context(|| format!("decode page @ offset {offset} of {table_id}"))?;
        let h = df.height();
        if h == 0 {
            break;
        }
        acc = Some(match acc {
            None => df,
            Some(mut prev) => {
                prev.vstack_mut(&df)
                    .map_err(|e| anyhow!("vstack page @ offset {offset}: {e}"))?;
                prev
            }
        });
        offset += h as i64;
        if (h as i64) < PAGE {
            break;
        }
    }
    Ok(acc.unwrap_or_default())
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

