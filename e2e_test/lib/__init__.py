"""End-to-end Rust vs R operator comparison driver.

Submodules:

- ``client``: connect to Tercen, list projects/workflows.
- ``workflow``: walk a workflow's steps, find the grid/QT steps and
  their relation outputs.
- ``runner``: shell-out to the local Rust ``dev`` binary with
  ``OUTPUT_CSV`` set.
- ``results``: download the R operator's output table from Tercen as
  a CSV (matching the format the Rust dev binary dumps).
- ``diff``: column-wise diff between R and Rust CSVs.
"""
