# inspect_parquet_sample.R
#
# Diagnostic pass over the parquet dataset written by process_mrf.R. Picks
# ~N_SAMPLE files spread across the FULL size distribution (not just random),
# so you see the smallest, largest, and everything in between -- exactly what
# you want when the question is "are the tiny files tiny because they're
# legitimately small filings, or because something parsed wrong."
#
# For each sampled file, prints:
#   - file size, row/col count, first 100 rows
#   - the matching row(s) from data/manifest/processing_log.csv (by content
#     hash parsed out of the filename), which already recorded the detected
#     format and row count at parse time -- a fast cross-check without having
#     to eyeball raw data to guess whether "small" means "wrong."
#
# Run from the project root (or let it find its own root, same convention as
# the other pipeline scripts).

find_project_root <- function() {
  a <- commandArgs(trailingOnly = FALSE)
  hit <- grep("--file=", a)
  script_path <- if (length(hit)) normalizePath(sub("--file=", "", a[hit[1]])) else NA_character_
  if (is.na(script_path) && requireNamespace("rstudioapi", quietly = TRUE) && rstudioapi::isAvailable()) {
    script_path <- rstudioapi::getSourceEditorContext()$path
  }
  if (is.na(script_path)) return(getwd())
  normalizePath(file.path(dirname(script_path), ".."))
}
setwd(find_project_root())

library(data.table)
library(arrow)
library(stringr)

DATA_DIR       <- "./data"
PARQUET_DIR    <- file.path(DATA_DIR, "parquet")
PROCESSING_LOG <- file.path(DATA_DIR, "manifest", "processing_log.csv")

N_SAMPLE  <- 10L
N_PREVIEW <- 100L

# ---- gather every parquet file + its size ---------------------------------

files <- list.files(PARQUET_DIR, pattern = "\\.parquet$", recursive = TRUE, full.names = TRUE)
stopifnot("No parquet files found -- has process_mrf.R run yet?" = length(files) > 0)

sizes <- file.size(files)
info  <- data.table(path = files, size_bytes = sizes)[order(size_bytes)]
message(sprintf("Found %d parquet files. Size range: %.1f KB to %.1f MB.",
                 nrow(info), min(sizes) / 1024, max(sizes) / 1e6))

# ---- pick N_SAMPLE files evenly spaced across the size distribution -------
# (by rank position, so this is robust to a distribution that's heavily
# skewed -- e.g. mostly-tiny with a few huge outliers -- rather than assuming
# anything about its shape.)

n_files <- nrow(info)
pick_idx <- if (n_files <= N_SAMPLE) seq_len(n_files) else {
  unique(round(seq(1, n_files, length.out = N_SAMPLE)))
}
sample_files <- info[pick_idx]

# ---- pull content_hash out of the filename for cross-referencing ---------
# filenames are written as "<content_hash>-part-{i}.parquet" by process_mrf.R
sample_files[, content_hash := str_extract(basename(path), "^[a-f0-9]{64}")]

proc_log <- if (file.exists(PROCESSING_LOG)) fread(PROCESSING_LOG) else NULL

# ---- preview each sampled file ---------------------------------------------

for (i in seq_len(nrow(sample_files))) {
  f <- sample_files[i]
  cat(strrep("=", 80), "\n")
  cat(sprintf("[%d/%d] %s\n", i, nrow(sample_files), f$path))
  cat(sprintf("Size: %.1f KB\n", f$size_bytes / 1024))

  if (!is.null(proc_log) && !is.na(f$content_hash)) {
    matches <- proc_log[content_hash == f$content_hash]
    if (nrow(matches)) {
      cat("Processing log entry (recorded at parse time):\n")
      print(matches[, .(storage_path, member, detected, n_rows, reason)])
    } else {
      cat("(no matching processing_log entry found for this hash)\n")
    }
  }

  dt <- tryCatch(as.data.table(read_parquet(f$path)), error = function(e) {
    cat("FAILED TO READ:", conditionMessage(e), "\n"); NULL
  })

  if (!is.null(dt)) {
    cat(sprintf("Rows: %d, Cols: %d\n", nrow(dt), ncol(dt)))
    if (nrow(dt)) {
      cat(sprintf("hospital_name: %s | source_format: %s | parse_ok rate: %.0f%%\n",
                   dt$hospital_name[1], dt$source_format[1], 100 * mean(dt$parse_ok, na.rm = TRUE)))
      cat(sprintf("distinct payer_name: %d | distinct description: %d\n",
                   uniqueN(dt$payer_name), uniqueN(dt$description)))
      cat("---- first", min(N_PREVIEW, nrow(dt)), "rows ----\n")
      print(head(dt, N_PREVIEW))
    } else {
      cat("(zero rows in this file)\n")
    }
  }
  cat("\n")
}

cat(strrep("=", 80), "\n")
cat("Sampled", nrow(sample_files), "of", n_files, "total parquet files.\n")
cat("Re-run with a larger N_SAMPLE at the top of this script for a denser look.\n")
