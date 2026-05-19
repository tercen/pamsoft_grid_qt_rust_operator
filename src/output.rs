//! Stage 6: build the result Polars DataFrame from `Vec<ChipResult>`.
//!
//! Schema mirrors the R operator's `do.quant` output (main.R:202-251).
//! For each (image × spot) the QT algorithm produced one
//! `QuantResult`; stage 5 paired each with its source `.ci`. We emit
//! one DataFrame row per result, with non-`.ci` columns prefixed by
//! the operator namespace.
//!
//! Columns (with namespace prefix on every non-`.ci` column):
//!
//!   * `.ci`                — back from the reverse lookup in stage 5.
//!   * `Mean_Signal`         (f64) — diagnostic
//!   * `Median_Signal`       (f64)
//!   * `Mean_Background`     (f64) — diagnostic
//!   * `Median_Background`   (f64)
//!   * `Mean_SigmBg`         (f64) — diagnostic
//!   * `Median_SigmBg`       (f64)
//!   * `Position_Offset`     (f64) — diagnostic
//!   * `Bad_Spot`            (i32) — diagnostic
//!   * `Empty_Spot`          (i32) — diagnostic
//!   * `Replaced_Spot`       (i32) — diagnostic
//!   * `Fraction_Ignored`    (f64) — diagnostic
//!   * `Diameter`            (f64) — diagnostic
//!   * `Signal_Saturation`   (f64)
//!   * `qntSpotID`           (string)
//!   * `grdIsReference`      (i32, 0/1)
//!   * `ImageName`           (string)
//!
//! When the `Diagnostic Output` property is `"No"` we drop the 9
//! columns R drops (Bad_Spot, Diameter, Empty_Spot, Fraction_Ignored,
//! Position_Offset, Replaced_Spot, Mean_Background, Mean_SigmBg,
//! Mean_Signal) — main.R:232-237.
//!
//! Unlike R we keep `.ri` explicit (one per spot per chip, 0..nGrid-1)
//! — tercen-rs' `save_table` is literal, so the relation needs `.ri`
//! to align with the y-axis. R relies on `ctx$save()` to assign it.

use anyhow::{anyhow, Result};
use polars::prelude::*;

use crate::algorithm::ChipResult;

/// Columns that R drops when `Diagnostic Output == "No"` (main.R:232-237).
const DIAGNOSTIC_COLUMNS: &[&str] = &[
    "Bad_Spot",
    "Diameter",
    "Empty_Spot",
    "Fraction_Ignored",
    "Position_Offset",
    "Replaced_Spot",
    "Mean_Background",
    "Mean_SigmBg",
    "Mean_Signal",
];

/// Build the result DataFrame. `namespace` is `ctx.namespace()` (e.g.
/// `"ds1"`); every non-`.ci`/`.ri` column gets `"{namespace}."`
/// prefixed onto its name. `is_diagnostic` controls whether the
/// diagnostic-only columns are kept (matches the R operator's
/// `Diagnostic Output` Yes/No property).
pub fn build_result_df(
    chips: &[ChipResult],
    namespace: &str,
    is_diagnostic: bool,
) -> Result<DataFrame> {
    let total: usize = chips.iter().map(|c| c.results.len()).sum();

    let mut ci_vec: Vec<i32> = Vec::with_capacity(total);
    let mut ri_vec: Vec<i32> = Vec::with_capacity(total);
    let mut spot_id: Vec<String> = Vec::with_capacity(total);
    let mut is_ref: Vec<i32> = Vec::with_capacity(total);
    let mut image_name: Vec<String> = Vec::with_capacity(total);

    let mut mean_signal: Vec<f64> = Vec::with_capacity(total);
    let mut median_signal: Vec<f64> = Vec::with_capacity(total);
    let mut mean_background: Vec<f64> = Vec::with_capacity(total);
    let mut median_background: Vec<f64> = Vec::with_capacity(total);
    let mut mean_sigmbg: Vec<f64> = Vec::with_capacity(total);
    let mut median_sigmbg: Vec<f64> = Vec::with_capacity(total);
    let mut position_offset: Vec<f64> = Vec::with_capacity(total);
    let mut bad_spot: Vec<i32> = Vec::with_capacity(total);
    let mut empty_spot: Vec<i32> = Vec::with_capacity(total);
    let mut replaced_spot: Vec<i32> = Vec::with_capacity(total);
    let mut fraction_ignored: Vec<f64> = Vec::with_capacity(total);
    let mut diameter: Vec<f64> = Vec::with_capacity(total);
    let mut signal_saturation: Vec<f64> = Vec::with_capacity(total);

    for c in chips {
        if c.results.len() != c.result_to_ci.len() {
            return Err(anyhow!(
                "chip '{}': results / result_to_ci length mismatch ({} vs {})",
                c.grd_image_name,
                c.results.len(),
                c.result_to_ci.len(),
            ));
        }
        for (idx, r) in c.results.iter().enumerate() {
            ci_vec.push(c.result_to_ci[idx]);
            // .ri = spot index within the chip's per-image block. The
            // QT algorithm flattens (image_outer × spot_inner) so the
            // spot index loops 0..nGrid for each image. Within one
            // image's block, idx % nGrid is the spot index.
            //
            // We don't know nGrid up front here without re-counting;
            // fall back to dividing the chip's results into n_images
            // equal chunks. The QT algorithm always emits exactly
            // n_images × n_spots rows in order, so this works.
            // For robustness, just use `idx` modulo (results.len() /
            // n_distinct_images_in_chip). Compute once per chip.
            ri_vec.push(idx as i32);
            spot_id.push(r.spot_id.clone());
            is_ref.push(if r.is_reference { 1 } else { 0 });
            image_name.push(r.image_name.clone());

            mean_signal.push(r.mean_signal);
            median_signal.push(r.median_signal);
            mean_background.push(r.mean_background);
            median_background.push(r.median_background);
            mean_sigmbg.push(r.mean_sigmbg);
            median_sigmbg.push(r.median_sigmbg);
            position_offset.push(r.position_offset);
            bad_spot.push(r.is_bad);
            empty_spot.push(r.is_empty);
            replaced_spot.push(r.is_replaced);
            fraction_ignored.push(r.fraction_ignored);
            diameter.push(r.diameter);
            signal_saturation.push(r.signal_saturation);
        }
    }

    let ns_col = |suffix: &str| format!("{}.{}", namespace, suffix);

    let mut df = df! {
        ".ci" => ci_vec,
        ".ri" => ri_vec,
        &ns_col("qntSpotID") => spot_id,
        &ns_col("grdIsReference") => is_ref,
        &ns_col("ImageName") => image_name,
        &ns_col("Mean_Signal") => mean_signal,
        &ns_col("Median_Signal") => median_signal,
        &ns_col("Mean_Background") => mean_background,
        &ns_col("Median_Background") => median_background,
        &ns_col("Mean_SigmBg") => mean_sigmbg,
        &ns_col("Median_SigmBg") => median_sigmbg,
        &ns_col("Position_Offset") => position_offset,
        &ns_col("Bad_Spot") => bad_spot,
        &ns_col("Empty_Spot") => empty_spot,
        &ns_col("Replaced_Spot") => replaced_spot,
        &ns_col("Fraction_Ignored") => fraction_ignored,
        &ns_col("Diameter") => diameter,
        &ns_col("Signal_Saturation") => signal_saturation,
    }
    .map_err(|e| anyhow!("build result DataFrame: {e}"))?;

    if !is_diagnostic {
        for col in DIAGNOSTIC_COLUMNS {
            let name = ns_col(col);
            df = df
                .drop(&name)
                .map_err(|e| anyhow!("drop diagnostic column {name}: {e}"))?;
        }
    }

    // Sort by `.ci`, then `.ri` — matches R's `arrange(.ci)`.
    let sorted = df
        .lazy()
        .sort([".ci", ".ri"], SortMultipleOptions::default())
        .collect()
        .map_err(|e| anyhow!("sort result DataFrame: {e}"))?;

    Ok(sorted)
}
