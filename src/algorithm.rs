//! Stage 5: algorithm invocation.
//!
//! Group input rows by `grdImageNameUsed` — one MATLAB pass per chip,
//! processing every image in that chip against a single grid (matching
//! R main.R::do.quant). Inside each chip:
//!
//! 1. Pick one image's slice (any of the chip's `.ci`s) to recover the
//!    grid. The 9 gathered variables (gridX, gridY, diameter, …) are
//!    identical across images of the same chip for each spot, so any
//!    `.ci` produces the same grid.
//! 2. Write a grid CSV (`pamsoft_grid::types::SpotResult` schema —
//!    same shape `process_single_group_qt` expects via
//!    `gridding_output_file`).
//! 3. Build the chip's `images_list` from the unique `image_label`s.
//! 4. Call `process_single_group_qt` → `Vec<QuantResult>` (one per
//!    `image × spot`).
//! 5. Tag each result with its source `.ci` via a `(image, spotRow,
//!    spotCol)` reverse lookup so stage 6 can emit per-`.ci` output
//!    rows.

use std::collections::BTreeMap;
use std::path::Path;

use anyhow::{anyhow, Context, Result};
use pamsoft_grid::batch::process_single_group_qt;
use pamsoft_grid::io::load_tiff_image;
use pamsoft_grid::types::{GroupConfig, ImageType, QuantResult};

use crate::download::DocumentCatalogue;
use crate::input::{InputData, QtInputRow};
use crate::props::PamsoftProps;

/// One chip's QT output. The chip is identified by its
/// `grd_image_name`; per-image, per-spot quant rows live in `results`.
///
/// `result_to_ci` is a parallel `Vec<i32>` of the original `.ci` for
/// each `results[i]` — stage 6 uses it to tag output rows back to their
/// source crosstab column. The lookup is precomputed here so output
/// construction is O(n).
pub struct ChipResult {
    pub grd_image_name: String,
    pub results: Vec<QuantResult>,
    pub result_to_ci: Vec<i32>,
    /// Effective spot pitch (auto-detected if `Spot Pitch = 0`).
    pub spot_pitch: f64,
}

/// Run QT for every chip in the input. Results are returned in
/// `grd_image_name` order (BTreeMap iteration).
pub fn run_quant_per_chip(
    input: &InputData,
    catalogue: &DocumentCatalogue,
    layout_path: &Path,
    props: &PamsoftProps,
    work_root: &Path,
) -> Result<Vec<ChipResult>> {
    // Group rows by chip (grdImageNameUsed). BTreeMap keeps chips in a
    // stable order; preserving each chip's original .ci ordering is
    // important so the reverse lookup we build below mirrors the input.
    let mut chips: BTreeMap<&str, Vec<&QtInputRow>> = BTreeMap::new();
    for row in &input.rows {
        chips.entry(row.grd_image_name.as_str()).or_default().push(row);
    }

    let mut out = Vec::with_capacity(chips.len());
    for (chip_name, chip_rows) in chips {
        let res = run_one_chip(chip_name, &chip_rows, catalogue, layout_path, props, work_root)
            .with_context(|| format!("chip '{chip_name}'"))?;
        out.push(res);
    }
    Ok(out)
}

fn run_one_chip(
    chip_name: &str,
    chip_rows: &[&QtInputRow],
    catalogue: &DocumentCatalogue,
    layout_path: &Path,
    props: &PamsoftProps,
    work_root: &Path,
) -> Result<ChipResult> {
    // --- 1. Pick a reference image (any image in the chip) for the grid ---
    // All images share the same per-spot grid values, so we pick the
    // first image_label we see and take its slice of the chip's rows.
    let ref_image = &chip_rows[0].image_label;
    let grid_rows: Vec<&QtInputRow> = chip_rows
        .iter()
        .copied()
        .filter(|r| &r.image_label == ref_image)
        .collect();
    if grid_rows.is_empty() {
        // shouldn't happen — `ref_image` came from `chip_rows[0]`.
        return Err(anyhow!(
            "no rows for reference image '{}' in chip '{}'",
            ref_image,
            chip_name
        ));
    }

    // --- 2. Write the grid CSV (SpotResult schema) ---
    let safe_chip = chip_name.replace(['/', '\\', ':', ' '], "_");
    let grid_csv_path = work_root.join(format!("grd_{}.csv", safe_chip));
    write_grid_csv(&grid_csv_path, chip_name, &grid_rows)
        .with_context(|| format!("write grid CSV {}", grid_csv_path.display()))?;

    // --- 3. Resolve TIFF paths for every image in the chip ---
    // Unique image_labels in input order — preserves the natural
    // "cycle ascending" order from the upstream Gather step.
    let mut seen: std::collections::HashSet<&str> = std::collections::HashSet::new();
    let mut chip_images: Vec<&QtInputRow> = Vec::new();
    for r in chip_rows {
        if seen.insert(r.image_label.as_str()) {
            chip_images.push(*r);
        }
    }
    let images_list: Vec<String> = chip_images
        .iter()
        .map(|r| {
            let tiff = resolve_tiff(r, catalogue)?;
            Ok(tiff.to_string_lossy().into_owned())
        })
        .collect::<Result<Vec<_>>>()?;

    // --- 4. Build GroupConfig and run quantification ---
    let spot_pitch = if props.spot_pitch > 0.0 {
        props.spot_pitch
    } else {
        autodetect_spot_pitch(&images_list[0])
            .with_context(|| format!("chip '{}': auto-detect spot pitch", chip_name))?
    };

    let group = GroupConfig {
        group_id: chip_name.to_string(),
        min_diameter: props.min_diameter,
        max_diameter: props.max_diameter,
        edge_sensitivity: props.edge_sensitivity.to_vec(),
        series_mode: 0,
        show_viewer: 0,
        spot_pitch,
        spot_size: props.spot_size,
        rotation: props.rotation.clone(),
        saturation_limit: props.saturation_limit,
        seg_method: props.seg_method.clone(),
        use_image: "Last".to_string(),
        pg_mode: "quantification".to_string(),
        debug_show: 0,
        array_layout_file: layout_path.to_string_lossy().into_owned(),
        images_list: images_list.clone(),
        gridding_output_file: grid_csv_path.to_string_lossy().into_owned(),
    };

    tracing::info!(
        chip = chip_name,
        n_images = group.images_list.len(),
        n_spots = grid_rows.len(),
        spot_pitch,
        "running QT pipeline"
    );
    let started = std::time::Instant::now();
    let results = process_single_group_qt(&group)
        .map_err(|e| anyhow!("chip '{}' QT pipeline: {}", chip_name, e))?;
    let elapsed = started.elapsed();
    tracing::info!(
        chip = chip_name,
        n_results = results.len(),
        elapsed_ms = elapsed.as_millis(),
        "chip done"
    );

    // --- 5. Reverse-lookup: (image_label, spot_row, spot_col) → .ci ---
    // Each QuantResult carries image_name + Row/Column (the grid spot
    // coords). Use these to find the original .ci.
    let lookup: std::collections::HashMap<(String, i32, i32), i32> = chip_rows
        .iter()
        .map(|r| ((r.image_label.clone(), r.spot_row, r.spot_col), r.ci))
        .collect();
    let mut result_to_ci = Vec::with_capacity(results.len());
    for r in &results {
        // QuantResult.row/col are f64 because the algorithm passes them
        // through as the grid CSV stored — but our CSV writes integer
        // spotRow/spotCol values that the algorithm round-trips. Cast
        // back via `as i32` for the lookup.
        let key = (r.image_name.clone(), r.row as i32, r.col as i32);
        let ci = lookup.get(&key).copied().ok_or_else(|| {
            anyhow!(
                "QT result (image='{}', row={}, col={}) for chip '{}' did not \
                 round-trip back to any input .ci. The grid CSV may have been \
                 written with the wrong key — check write_grid_csv.",
                r.image_name,
                r.row,
                r.col,
                chip_name,
            )
        })?;
        result_to_ci.push(ci);
    }

    Ok(ChipResult {
        grd_image_name: chip_name.to_string(),
        results,
        result_to_ci,
        spot_pitch,
    })
}

/// Write a chip's per-spot grid as a `SpotResult`-schema CSV. The QT
/// algorithm (`process_single_group_qt`) reads this file via
/// `load_grid_results_csv` and treats each row as a `Spot`. Columns
/// must match the header at `pamsoft_grid::batch::write_grid_csv`:
///
/// ```text
/// groupId,qntSpotID,grdIsReference,grdRow,grdCol,
/// grdXFixedPosition,grdYFixedPosition,gridX,gridY,
/// diameter,isManual,segIsBad,segIsEmpty,grdRotation,grdImageNameUsed
/// ```
fn write_grid_csv(path: &Path, group_id: &str, grid_rows: &[&QtInputRow]) -> Result<()> {
    use std::io::Write;
    let mut f = std::fs::File::create(path)?;
    writeln!(
        f,
        "groupId,qntSpotID,grdIsReference,grdRow,grdCol,\
         grdXFixedPosition,grdYFixedPosition,gridX,gridY,\
         diameter,isManual,segIsBad,segIsEmpty,grdRotation,grdImageNameUsed"
    )?;
    for r in grid_rows {
        // `grdIsReference` mirrors R's `qntSpotID == "#REF"` derivation
        // (main.R:79-84) — the upstream grid output's column wasn't
        // gathered, so we recompute it from the spot ID here.
        let is_reference = if r.spot_id == "#REF" { 1 } else { 0 };
        writeln!(
            f,
            "{group_id},{spot_id},{is_ref},{row},{col},\
             {xfix},{yfix},{gx},{gy},{diam},{manual},{bad},{empty},{rot},{img}",
            spot_id = r.spot_id,
            is_ref = is_reference,
            row = r.spot_row,
            col = r.spot_col,
            xfix = r.grd_x_fixed,
            yfix = r.grd_y_fixed,
            gx = r.grid_x,
            gy = r.grid_y,
            diam = r.diameter,
            // R coerces the manual/bad/empty fields to as.integer(as.logical(...)),
            // i.e. 0 or 1. The .y values we received are already 0.0/1.0 doubles;
            // round to int explicitly to avoid e.g. "1.0" sneaking into the CSV.
            manual = r.manual as i32,
            bad = r.bad as i32,
            empty = r.empty as i32,
            rot = r.rotation,
            img = r.image_label,
        )?;
    }
    Ok(())
}

/// Resolve the on-disk TIFF for an input row by looking up the row's
/// image_label in the primary documentId's TIFF index.
fn resolve_tiff(row: &QtInputRow, catalogue: &DocumentCatalogue) -> Result<std::path::PathBuf> {
    let primary = row
        .document_ids
        .first()
        .ok_or_else(|| anyhow!(".ci={} has no documentId", row.ci))?;
    let doc = catalogue
        .get(primary)
        .ok_or_else(|| anyhow!(".ci={}: documentId {} not in catalogue", row.ci, primary))?;
    doc.tiff_index
        .get(&row.image_label)
        .cloned()
        .ok_or_else(|| {
            let sample = doc
                .tiff_index
                .keys()
                .take(5)
                .cloned()
                .collect::<Vec<_>>()
                .join(", ");
            anyhow!(
                ".ci={} ('{}'): no TIFF with stem '{}' in documentId {}. \
                 Available stems (first 5): {}",
                row.ci,
                row.image_label,
                row.image_label,
                primary,
                sample,
            )
        })
}

fn autodetect_spot_pitch(first_image: &str) -> Result<f64> {
    let img = load_tiff_image(first_image).context("load first image for pitch detect")?;
    let kind = ImageType::detect(img.width, img.height);
    kind.default_spot_pitch().ok_or_else(|| {
        anyhow!(
            "cannot auto-detect spot pitch from image dimensions {}×{} \
             ({:?}). Set the 'Spot Pitch' operator property explicitly.",
            img.width,
            img.height,
            kind
        )
    })
}
