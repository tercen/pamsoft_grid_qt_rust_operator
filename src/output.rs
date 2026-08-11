//! Stage 6: build the result Polars DataFrame from `Vec<ChipResult>`.
//!
//! Schema mirrors the R operator's `do.quant` output (main.R:202-251)
//! one-for-one. R does:
//!
//! ```r
//! quantOutput %>%
//!   left_join(inTable, by = c("spotCol", "spotRow", "Image")) %>%
//!   select(-spotCol, -spotRow, -Image, -.ri)
//! ```
//!
//! …which drops the join-key columns (spotRow/spotCol/Image) **and**
//! `.ri`, leaving the table keyed solely by `.ci`. `qntSpotID`,
//! `grdIsReference`, and `ImageName` (raw MATLAB CSV columns that
//! aren't carried through the join) also don't make it to the output.
//! All numeric columns get cast to `double` via `mutate(across(
//! where(is.numeric), as.double))`.
//!
//! Columns emitted (with `{namespace}.` prefix on everything except `.ci`):
//!
//!   * `.ci`                — back from the reverse lookup in stage 5.
//!   * `Mean_Signal`         (f64) — diagnostic
//!   * `Median_Signal`       (f64)
//!   * `Mean_Background`     (f64) — diagnostic
//!   * `Median_Background`   (f64)
//!   * `Mean_SigmBg`         (f64) — diagnostic
//!   * `Median_SigmBg`       (f64)
//!   * `Position_Offset`     (f64) — diagnostic
//!   * `Bad_Spot`            (f64) — diagnostic, 0.0 / 1.0
//!   * `Empty_Spot`          (f64) — 0.0 / 1.0
//!   * `Replaced_Spot`       (f64) — 0.0 / 1.0
//!   * `Fraction_Ignored`    (f64) — diagnostic
//!   * `Diameter`            (f64) — diagnostic
//!   * `Signal_Saturation`   (f64)
//!   * `gridX`               (f64)
//!   * `gridY`               (f64)
//!
//! When the `Diagnostic Output` property is `"No"` we drop the 9
//! columns R drops (Bad_Spot, Diameter, Empty_Spot, Fraction_Ignored,
//! Position_Offset, Replaced_Spot, Mean_Background, Mean_SigmBg,
//! Mean_Signal) — main.R:232-237.
//!
//! No `.ri` column — R's `select(-.ri)` drops it and the relation works
//! fine without it (Tercen keys rows positionally within each `.ci`).

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
/// `"ds1"`); every non-`.ci` column gets `"{namespace}."` prefixed
/// onto its name. `is_diagnostic` controls whether the
/// diagnostic-only columns are kept (matches the R operator's
/// `Diagnostic Output` Yes/No property).
pub fn build_result_df(
    chips: &[ChipResult],
    namespace: &str,
    is_diagnostic: bool,
) -> Result<DataFrame> {
    let total: usize = chips.iter().map(|c| c.results.len()).sum();

    let mut ci_vec: Vec<i32> = Vec::with_capacity(total);

    let mut mean_signal: Vec<f64> = Vec::with_capacity(total);
    let mut median_signal: Vec<f64> = Vec::with_capacity(total);
    let mut mean_background: Vec<f64> = Vec::with_capacity(total);
    let mut median_background: Vec<f64> = Vec::with_capacity(total);
    let mut mean_sigmbg: Vec<f64> = Vec::with_capacity(total);
    let mut median_sigmbg: Vec<f64> = Vec::with_capacity(total);
    let mut position_offset: Vec<f64> = Vec::with_capacity(total);
    // The bool-style flags are emitted as f64 0.0/1.0 to match R's
    // `as.double(as.logical(...))` cast — int32 would be loud-but-wrong
    // for downstream consumers that expect doubles (the R operator's
    // schema is the contract here).
    let mut bad_spot: Vec<f64> = Vec::with_capacity(total);
    let mut empty_spot: Vec<f64> = Vec::with_capacity(total);
    let mut replaced_spot: Vec<f64> = Vec::with_capacity(total);
    let mut fraction_ignored: Vec<f64> = Vec::with_capacity(total);
    let mut diameter: Vec<f64> = Vec::with_capacity(total);
    let mut signal_saturation: Vec<f64> = Vec::with_capacity(total);
    let mut grid_x: Vec<f64> = Vec::with_capacity(total);
    let mut grid_y: Vec<f64> = Vec::with_capacity(total);

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
            mean_signal.push(r.mean_signal);
            median_signal.push(r.median_signal);
            mean_background.push(r.mean_background);
            median_background.push(r.median_background);
            mean_sigmbg.push(r.mean_sigmbg);
            median_sigmbg.push(r.median_sigmbg);
            position_offset.push(r.position_offset);
            bad_spot.push(if r.is_bad != 0 { 1.0 } else { 0.0 });
            empty_spot.push(if r.is_empty != 0 { 1.0 } else { 0.0 });
            replaced_spot.push(if r.is_replaced != 0 { 1.0 } else { 0.0 });
            fraction_ignored.push(r.fraction_ignored);
            diameter.push(r.diameter);
            signal_saturation.push(r.signal_saturation);
            grid_x.push(r.grid_x);
            grid_y.push(r.grid_y);
        }
    }

    let ns_col = |suffix: &str| format!("{}.{}", namespace, suffix);

    let mut df = df! {
        ".ci" => ci_vec,
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
        &ns_col("gridX") => grid_x,
        &ns_col("gridY") => grid_y,
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

    // Sort by `.ci` only — matches R's `arrange(.ci)`. Within a `.ci`,
    // row order is whatever the algorithm emitted (one row per spot
    // per image; spot index ascending).
    let sorted = df
        .lazy()
        .sort([".ci"], SortMultipleOptions::default())
        .collect()
        .map_err(|e| anyhow!("sort result DataFrame: {e}"))?;

    Ok(sorted)
}
