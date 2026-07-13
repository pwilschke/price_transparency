# process_mrf.R
#
# Step 2 of the pipeline. Takes the content-addressed blobs produced by
# mrf_fetch_and_store.R and turns the three CMS v3.0 layouts (tall CSV, wide
# CSV, JSON) into ONE unified long-format table -- one row per
# (item/service x payer-plan) -- written to a hive-partitioned parquet dataset
# that can be queried across all hospitals with arrow / DuckDB.
#
#   detect_type()   -> classify a blob: tall_csv / wide_csv / json / noncompliant
#   parse_tall_csv() -> already long; rename + gather codes + coerce
#   parse_wide_csv() -> melt the repeating payer-plan column groups to long
#   parse_json()     -> flatten standard_charge_information[] -> standard_charges[]
#                       -> payers_information[]
#
# All three emit the same unified schema (see UNIFIED_COLS). Metadata from the
# top of each file (hospital name, NPI, last_updated_on, ...) is attached as
# columns. Numeric charges are coerced ($ , stripped); on failure the original
# string is kept in a *_raw column and parse_ok is set FALSE.
#
# Re-run monthly after the fetch script. It only converts blobs (content
# hashes) not already recorded in data/manifest/parquet_index.csv, so a
# "nothing changed" month is cheap.
#
# ---------------------------------------------------------------------------

# ---- working directory -------------------------------------------------------
# Resolve the project root regardless of how this script is invoked: RStudio
# "Source", `Rscript process_mrf.R` from any working directory (e.g. Task
# Scheduler's default, which is NOT this folder), or sourced from run_pipeline.R.
# Assumes this file lives in <project_root>/code/.
find_project_root <- function() {
  a <- commandArgs(trailingOnly = FALSE)
  hit <- grep("--file=", a)
  script_path <- if (length(hit)) normalizePath(sub("--file=", "", a[hit[1]])) else NA_character_
  if (is.na(script_path) && requireNamespace("rstudioapi", quietly = TRUE) && rstudioapi::isAvailable()) {
    script_path <- rstudioapi::getSourceEditorContext()$path
  }
  if (is.na(script_path)) return(getwd())  # last resort
  normalizePath(file.path(dirname(script_path), ".."))
}
setwd(find_project_root())
if (!dir.exists("data")) stop("Run from project root (a 'data' dir must exist).")

library(data.table)
library(stringr)
library(arrow)
library(jsonlite)

# ---- parsing functions ----------------------------------------------------
# All the pure parsing logic (detect_type, parse_tall_csv, parse_wide_csv,
# parse_json, gather_codes, ensure_schema, process_blob, ...) lives in
# 03_parsers.R so it can also be source()d by parallel workers WITHOUT running
# this driver. That file defines DATA_DIR/BLOB_DIR/PARQUET_DIR/TMP_DIR,
# log_msg(), and UNIFIED_COLS as well. It creates TMP_DIR but never wipes it.
source(file.path("code", "03_parsers.R"))
source(file.path("code", "_parallel_utils.R"))

# ---- config (driver-only) -------------------------------------------------

MANIFEST_DIR  <- file.path(DATA_DIR, "manifest")

# Clear scratch space once, here in the driver only (never in 03_parsers.R:
# a parallel worker wiping TMP_DIR would destroy other workers' scratch files).
if (dir.exists(TMP_DIR)) unlink(TMP_DIR, recursive = TRUE)
dir.create(TMP_DIR, recursive = TRUE)

SNAPSHOTS_CSV     <- file.path(MANIFEST_DIR, "mrf_snapshots.csv")
URL_MAP_CSV       <- file.path(MANIFEST_DIR, "url_hospital_map.csv")
PARQUET_INDEX_CSV <- file.path(MANIFEST_DIR, "parquet_index.csv")
PROCESSING_LOG    <- file.path(MANIFEST_DIR, "processing_log.csv")

# Canonical manifest schema written by 02_mrf_fetch_and_store.R. Kept in sync
# with SNAPSHOT_COLS there. Used only to give the defensive reader below the
# full (widest) column set so a plain fread never truncates the manifest at a
# schema-width change -- see read_snapshots() for the full explanation.
SNAPSHOT_COLS <- c(
  "run_date", "url", "http_status", "outcome", "content_hash",
  "storage_path", "bytes_raw", "detected_format", "content_type"
)

# Read the append-only manifest defensively. If the file was written across a
# schema change it may hold rows with different column counts; a plain fread()
# locks onto the header width and STOPS EARLY at the first wider row, silently
# dropping most of the history (this is what made this script report "no new
# blobs" despite a full manifest). We force the full canonical width by giving
# fread our own header, then coerce to SNAPSHOT_COLS. Short (old-schema) rows
# get NA in trailing columns; the numeric columns are restored to numeric.
read_snapshots <- function(path = SNAPSHOTS_CSV) {
  if (!file.exists(path))
    return(setNames(
      data.table(matrix(character(0), ncol = length(SNAPSHOT_COLS))),
      SNAPSHOT_COLS
    ))
  lines <- readLines(path, warn = FALSE)
  lines <- lines[nzchar(lines)]
  if (length(lines) <= 1L)
    return(setNames(
      data.table(matrix(character(0), ncol = length(SNAPSHOT_COLS))),
      SNAPSHOT_COLS
    ))
  dt <- fread(
    text = c(paste(SNAPSHOT_COLS, collapse = ","), lines[-1L]),
    header = TRUE, fill = TRUE, colClasses = "character"
  )
  for (col in SNAPSHOT_COLS)
    if (!col %in% names(dt))
      dt[, (col) := NA_character_]
  dt <- dt[, ..SNAPSHOT_COLS]
  suppressWarnings({
    dt[, http_status := as.integer(http_status)]
    dt[, bytes_raw   := as.numeric(bytes_raw)]
  })
  dt
}

# ---- driver ---------------------------------------------------------------

load_blobs_to_process <- function() {
  stopifnot(file.exists(SNAPSHOTS_CSV))
  snap <- read_snapshots()
  snap <- snap[!is.na(storage_path) & storage_path != "" & !is.na(content_hash)]
  setorder(snap, content_hash, run_date)
  latest <- snap[, .SD[.N], by = content_hash]   # most recent sighting per blob
  # a readable source_url for provenance (first url mapped to this hash)
  if (file.exists(URL_MAP_CSV)) {
    umap <- unique(fread(URL_MAP_CSV)[, .(url, name)])
    # A url can be shared by several hospitals (e.g. one system-wide file
    # covering multiple facilities). Collapse to one representative name per
    # url BEFORE merging, so this stays a one-to-one join -- otherwise each
    # blob gets fanned out into one row per hospital sharing it, and ends up
    # reprocessed that many times for no benefit (the output file is keyed
    # purely on content_hash, so the repeats just overwrite each other).
    umap <- umap[, .(name = name[1]), by = url]
    latest <- merge(latest, umap, by = "url", all.x = TRUE)
  }
  setnames(latest, "url", "source_url")
  latest
}

STAMP_START <- format(Sys.time(), "%Y-%m-%dT%H:%M:%S")
for (d in c(PARQUET_DIR, MANIFEST_DIR, TMP_DIR)) if (!dir.exists(d)) dir.create(d, recursive = TRUE)

blobs <- load_blobs_to_process()
# Force character: fread would otherwise read an all-digit hash column as
# integer, breaking the %in% match below (real sha256 hashes contain hex
# letters, but don't rely on that). unique() guards against duplicate index
# rows accumulated across appends.
done <- if (file.exists(PARQUET_INDEX_CSV)) {
  unique(as.character(fread(PARQUET_INDEX_CSV,
                            colClasses = list(character = "content_hash"))$content_hash))
} else {
  character(0)
}
todo  <- blobs[!content_hash %in% done]
log_msg("Blobs total=%d, already converted=%d, to process=%d",
        nrow(blobs), length(done), nrow(todo))

all_logs <- list(); newly_done <- character(0); total_rows <- 0L
n_converted <- 0L   # cumulative across chunks (newly_done is reset each flush)
CHECKPOINT_EVERY <- 25L   # flush progress this often, not just at the very end

flush_progress <- function(logs_acc, done_acc) {
  if (length(logs_acc)) {
    logdt <- rbindlist(logs_acc, fill = TRUE)
    logdt[, run_at := format(Sys.time(), "%Y-%m-%dT%H:%M:%S")]
    fwrite(logdt, PROCESSING_LOG, append = file.exists(PROCESSING_LOG))
  }
  if (length(done_acc)) {
    idx <- data.table(content_hash = unique(done_acc),
                      converted_at = format(Sys.time(), "%Y-%m-%dT%H:%M:%S"))
    fwrite(idx, PARQUET_INDEX_CSV, append = file.exists(PARQUET_INDEX_CSV))
  }
}

# Blobs are independent and each writes its own content_hash-prefixed parquet
# file, so there is no write contention -- they parallelize cleanly. We process
# `todo` in chunks and flush the index/log after each chunk, so a crash loses at
# most one chunk's progress (same resumability as the old per-25 checkpoint).
n_workers <- min(detect_workers(), max(1L, nrow(todo)))
log_msg("Processing %d blobs with %d worker(s) (PIPELINE_WORKERS overrides).",
        nrow(todo), n_workers)

# Each PSOCK worker is a fresh R process: point it at the project root and load
# the parsing functions + arrow. (No-op when running serially with n_workers=1.)
# Built with local() so `root` lives in the closure's OWN environment and gets
# serialized to the workers -- a top-level function would instead reference the
# worker's (empty) global env and fail to find the project root.
worker_init <- local({
  root <- getwd()
  function() {
    setwd(root)
    source(file.path("code", "03_parsers.R"))
  }
})

if (nrow(todo)) {
  # split todo row indices into contiguous chunks of CHECKPOINT_EVERY
  chunks <- split(seq_len(nrow(todo)),
                  ceiling(seq_len(nrow(todo)) / CHECKPOINT_EVERY))
  done_count <- 0L

  with_cluster(n_workers, worker_init = worker_init, FUN = function(cl) {
    for (ch in chunks) {
      mrows <- lapply(ch, function(i) as.list(todo[i]))
      results <- par_lapply(cl, mrows, process_and_write_blob)
      for (res in results) {
        all_logs[[length(all_logs) + 1]] <<- res$log
        if (isTRUE(res$wrote)) {
          total_rows <<- total_rows + res$n_rows
          newly_done <<- c(newly_done, res$content_hash)
          n_converted <<- n_converted + 1L
        }
      }
      done_count <- done_count + length(ch)
      log_msg("...%d / %d blobs processed", done_count, nrow(todo))
      flush_progress(all_logs, newly_done)
      all_logs <<- list(); newly_done <<- character(0)   # already flushed
    }
  })
}
# (any remaining unflushed progress was flushed at the end of the last chunk)
flush_progress(all_logs, newly_done)

logdt <- if (file.exists(PROCESSING_LOG)) {
  full <- fread(PROCESSING_LOG)
  full[run_at >= STAMP_START]   # just this run's rows, not the whole history
} else data.table(detected = character(0))
tab <- table(logdt$detected)
log_msg("Run complete. Detected: %s", paste(sprintf("%s=%d", names(tab), tab), collapse = ", "))
log_msg("Rows written this run: %d; blobs newly converted: %d", total_rows, n_converted)
