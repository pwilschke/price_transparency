# 03_process_mrf.R
#
# Step 2 of the pipeline. Takes the content-addressed blobs produced by
# mrf_fetch_and_store.R and turns the three CMS v3.0 layouts (tall CSV, wide
# CSV, JSON) into ONE unified long-format table -- one row per
# (item/service x payer-plan) -- written to a hive-partitioned parquet dataset
# that can be queried across all hospitals with arrow / DuckDB.
#
# Re-run monthly after the fetch script. It only converts blobs (content
# hashes) not already recorded in data/manifest/parquet_index.csv, so a
# "nothing changed" month is cheap. Blobs estimated too large to safely parse
# in R (see MAX_ESTIMATED_MB in 03_parsers.R) are recorded separately in
# data/manifest/skipped_index.csv and permanently excluded from future runs --
# to revisit one later (e.g. after raising MAX_ESTIMATED_MB), delete its row
# from that file.
# ---------------------------------------------------------------------------

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
if (!dir.exists("data")) stop("Run from project root (a 'data' dir must exist).")

library(data.table)
library(stringr)
library(arrow)
library(jsonlite)

source(file.path("code", "03_parsers.R"))
source(file.path("code", "_parallel_utils.R"))

# ---- config (driver-only) -------------------------------------------------

MANIFEST_DIR  <- file.path(DATA_DIR, "manifest")

if (dir.exists(TMP_DIR)) unlink(TMP_DIR, recursive = TRUE)
dir.create(TMP_DIR, recursive = TRUE)

SNAPSHOTS_CSV     <- file.path(MANIFEST_DIR, "mrf_snapshots.csv")
URL_MAP_CSV       <- file.path(MANIFEST_DIR, "url_hospital_map.csv")
PARQUET_INDEX_CSV <- file.path(MANIFEST_DIR, "parquet_index.csv")
SKIPPED_INDEX_CSV <- file.path(MANIFEST_DIR, "skipped_index.csv")
PROCESSING_LOG    <- file.path(MANIFEST_DIR, "processing_log.csv")

SNAPSHOT_COLS <- c(
  "run_date", "url", "http_status", "outcome", "content_hash",
  "storage_path", "bytes_raw", "detected_format", "content_type"
)

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
  latest <- snap[, .SD[.N], by = content_hash]
  if (file.exists(URL_MAP_CSV)) {
    umap <- unique(fread(URL_MAP_CSV)[, .(url, name)])
    umap <- umap[, .(name = name[1]), by = url]
    latest <- merge(latest, umap, by = "url", all.x = TRUE)
  }
  setnames(latest, "url", "source_url")
  latest
}

# read a simple content_hash index file (parquet_index.csv / skipped_index.csv),
# forcing character so an all-digit hash never gets silently read as integer.
read_hash_index <- function(path) {
  if (!file.exists(path)) return(character(0))
  unique(as.character(fread(path, colClasses = list(character = "content_hash"))$content_hash))
}

STAMP_START <- format(Sys.time(), "%Y-%m-%dT%H:%M:%S")
for (d in c(PARQUET_DIR, MANIFEST_DIR, TMP_DIR)) if (!dir.exists(d)) dir.create(d, recursive = TRUE)

blobs <- load_blobs_to_process()
done    <- read_hash_index(PARQUET_INDEX_CSV)
skipped <- read_hash_index(SKIPPED_INDEX_CSV)
todo  <- blobs[!content_hash %in% union(done, skipped)]
log_msg("Blobs total=%d, already converted=%d, already skipped-too-large=%d, to process=%d",
        nrow(blobs), length(done), length(skipped), nrow(todo))

all_logs <- list(); newly_done <- character(0); newly_skipped <- character(0); total_rows <- 0L
n_converted <- 0L
CHECKPOINT_EVERY <- 25L

flush_progress <- function(logs_acc, done_acc, skipped_acc) {
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
  if (length(skipped_acc)) {
    idx <- data.table(content_hash = unique(skipped_acc),
                      skipped_at = format(Sys.time(), "%Y-%m-%dT%H:%M:%S"))
    fwrite(idx, SKIPPED_INDEX_CSV, append = file.exists(SKIPPED_INDEX_CSV))
  }
}

# n_workers <- min(detect_workers(), max(1L, nrow(todo)))
n_workers <- 1

log_msg("Processing %d blobs with %d worker(s) (PIPELINE_WORKERS overrides).",
        nrow(todo), n_workers)

worker_init <- local({
  root <- getwd()
  function() {
    setwd(root)
    source(file.path("code", "03_parsers.R"))
  }
})

# Capture root the same way worker_init does, so this survives serialization
# to a parallel worker if n_workers is ever raised above 1. Note: if a real
# PSOCK worker (n_workers > 1) also runs this, each blob's parse spawns a
# FURTHER nested subprocess via callr -- fine functionally (just adds spawn
# overhead per blob), worth knowing if profiling throughput later.
process_fn <- local({
  root <- getwd()
  function(mrow) process_and_write_blob_isolated(mrow, root = root)
})

if (nrow(todo)) {
  chunks <- split(seq_len(nrow(todo)),
                  ceiling(seq_len(nrow(todo)) / CHECKPOINT_EVERY))
  done_count <- 0L
  
  with_cluster(n_workers, worker_init = worker_init, FUN = function(cl) {
    for (ch in chunks) {
      mrows <- lapply(ch, function(i) as.list(todo[i]))
      results <- par_lapply(cl, mrows, process_fn)
      for (res in results) {
        all_logs[[length(all_logs) + 1]] <<- res$log
        if (isTRUE(res$wrote)) {
          total_rows <<- total_rows + res$n_rows
          newly_done <<- c(newly_done, res$content_hash)
          n_converted <<- n_converted + 1L
        } else if (isTRUE(res$skipped_too_large)) {
          newly_skipped <<- c(newly_skipped, res$content_hash)
        }
      }
      done_count <- done_count + length(ch)
      log_msg("...%d / %d blobs processed", done_count, nrow(todo))
      flush_progress(all_logs, newly_done, newly_skipped)
      all_logs <<- list(); newly_done <<- character(0); newly_skipped <<- character(0)
    }
  })
}
flush_progress(all_logs, newly_done, newly_skipped)

logdt <- if (file.exists(PROCESSING_LOG)) {
  full <- fread(PROCESSING_LOG)
  full[run_at >= STAMP_START]
} else data.table(detected = character(0))
tab <- table(logdt$detected)
log_msg("Run complete. Detected: %s", paste(sprintf("%s=%d", names(tab), tab), collapse = ", "))
log_msg("Rows written this run: %d; blobs newly converted: %d", total_rows, n_converted)

skipped_this_run <- if (file.exists(SKIPPED_INDEX_CSV)) {
  s <- fread(SKIPPED_INDEX_CSV)
  nrow(s[skipped_at >= STAMP_START])
} else 0L
if (skipped_this_run > 0L) {
  log_msg("%d blob(s) skipped as too-large this run; see %s for the list (permanently excluded until you remove them from that file).",
          skipped_this_run, SKIPPED_INDEX_CSV)
}