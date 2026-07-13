# mrf_fetch_and_store.R
#
# Takes the output of the baseline beacon pipeline (hpt_results.csv, which has
# a pipe-delimited `mrf_urls` column per hospital) and:
#   1. Explodes it into one row per (hospital, url) pair.
#   2. De-duplicates to the set of *unique URLs* (many hospitals can share
#      one file, e.g. system-wide beacons).
#   3. For each unique URL, does a conditional fetch (curl's --etag-save /
#      --etag-compare, so unchanged files aren't re-downloaded at all on
#      subsequent monthly runs).
#   4. Hashes whatever bytes were actually retrieved (sha256) and stores them
#      in a content-addressed blob store, so identical content is only ever
#      stored once -- whether that's the SAME url being unchanged over time,
#      or two DIFFERENT urls that happen to serve the same file.
#   5. Appends one row per (url, run) to an append-only manifest log, which
#      becomes your full change-history over time.
#
# Re-run this monthly. It is designed to be cheap on a "nothing changed" month:
# curl skips the download entirely (304) for any url whose ETag matches.
#
# Run this on a machine with real internet access (not the Board device).
#
# ------------------------------------------------------------------------------
# Layout this script maintains under DATA_DIR:
#   data/
#     blobs/<hash[1:2]>/<hash>.<ext>[.gz]   content-addressed store
#     etags/<url_key>.etag                  curl's saved ETag per url (persists)
#     manifest/mrf_snapshots.csv            append-only log, one row per (url, run)
#     manifest/url_hospital_map.csv         rebuilt each run: url <-> hospital
#     tmp/                                  scratch space, cleared per url
# ---------------------------------------------------------------------------

library(data.table)
library(stringr)
library(digest)

# ---- working directory -------------------------------------------------------
find_project_root <- function() {
  a <- commandArgs(trailingOnly = FALSE)
  hit <- grep("--file=", a)
  script_path <-
    if (length(hit))
      normalizePath(sub("--file=", "", a[hit[1]]))
  else
    NA_character_
  if (is.na(script_path) &&
      requireNamespace("rstudioapi", quietly = TRUE) &&
      rstudioapi::isAvailable()) {
    script_path <- rstudioapi::getSourceEditorContext()$path
  }
  if (is.na(script_path))
    return(getwd())
  normalizePath(file.path(dirname(script_path), ".."))
}
setwd(find_project_root())
if (!dir.exists("data"))
  stop("Run from project root (a 'data' dir must exist).")

# Parallelism helpers. Sourcing this file only defines functions; the driver
# (under `# ---- main`) is guarded by `if (sys.nframe() == 0)` so parallel
# workers can source THIS script to get fetch_and_store_one() without launching
# a second run.
source(file.path("code", "_parallel_utils.R"))

# ---- config -------------------------------------------------------------

INPUT_RESULTS_CSV <- "./data/hpt_results.csv"
DATA_DIR          <- "./data"
BLOB_DIR          <- file.path(DATA_DIR, "blobs")
ETAG_DIR          <- file.path(DATA_DIR, "etags")
MANIFEST_DIR      <- file.path(DATA_DIR, "manifest")
TMP_DIR           <- file.path(DATA_DIR, "tmp")

SNAPSHOTS_CSV     <- file.path(MANIFEST_DIR, "mrf_snapshots.csv")
URL_MAP_CSV       <- file.path(MANIFEST_DIR, "url_hospital_map.csv")

# Canonical manifest schema. EVERY row ever written to SNAPSHOTS_CSV must have
# exactly these columns, in this order. `fwrite(append=TRUE)` does NOT verify
# that new rows match the existing header -- so if this vector ever changes,
# appending would leave the file with a mix of column counts. A reader like
# data.table::fread then locks onto the (narrower) header width and STOPS EARLY
# at the first wider row, silently truncating the manifest. `read_snapshots()`
# below reads defensively against that, and the writer migrates the file to
# this schema before appending. If you add a column, add it HERE (and to the
# `base_row` in fetch_and_store_one) and old files auto-migrate on next run.
SNAPSHOT_COLS <- c(
  "run_date", "url", "http_status", "outcome", "content_hash",
  "storage_path", "bytes_raw", "detected_format", "content_type"
)

USER_AGENT   <-
  "price-transparency-research/0.1 (contact: research)"
TIMEOUT_SECS <- 1800
MAX_MB       <- 3000
MIN_PLAUSIBLE_BYTES <-
  200  # a real MRF/beacon response should be at least this
# big; anything smaller is very likely an error or
# redirect page served back with a 200 status
POLITE_SLEEP <- 0.5

RUN_DATE <- format(Sys.time(), "%Y-%m-%dT%H:%M:%S")

for (d in c(BLOB_DIR, ETAG_DIR, MANIFEST_DIR, TMP_DIR)) {
  if (!dir.exists(d))
    dir.create(d, recursive = TRUE)
}

# ---- small helpers --------------------------------------------------------

log_msg <-
  function(...)
    message(sprintf("[%s] %s", format(Sys.time(), "%H:%M:%S"), sprintf(...)))

# Read the append-only manifest defensively. The file may contain rows with
# FEWER columns than SNAPSHOT_COLS (written before a column was added). A plain
# fread() stops early at the first row wider than the header, silently dropping
# most of the history; we instead force fread to use the full canonical width by
# supplying our own header, then coerce each row to SNAPSHOT_COLS. Rows that are
# short (old schema) get NA in the trailing columns; rows that are long (should
# not happen, but be safe) keep only the leading canonical columns. Returns an
# empty, correctly-typed data.table when the file is absent.
read_snapshots <- function(path = SNAPSHOTS_CSV) {
  if (!file.exists(path))
    return(setNames(
      data.table(matrix(character(0), ncol = length(SNAPSHOT_COLS))),
      SNAPSHOT_COLS
    ))
  lines <- readLines(path, warn = FALSE)
  lines <- lines[nzchar(lines)]
  if (length(lines) <= 1L)  # header only (or empty)
    return(setNames(
      data.table(matrix(character(0), ncol = length(SNAPSHOT_COLS))),
      SNAPSHOT_COLS
    ))
  # Replace whatever header is on disk with the canonical (widest) one so fread
  # allocates all columns and never truncates at a wider data row.
  body <- lines[-1L]
  dt <- fread(
    text = c(paste(SNAPSHOT_COLS, collapse = ","), body),
    header = TRUE,
    fill = TRUE,
    colClasses = "character"
  )
  # Guarantee exactly the canonical columns in order (add missing as NA).
  for (col in SNAPSHOT_COLS)
    if (!col %in% names(dt))
      dt[, (col) := NA_character_]
  dt <- dt[, ..SNAPSHOT_COLS]
  # restore the numeric type of columns that downstream code treats as numbers
  suppressWarnings({
    dt[, http_status := as.integer(http_status)]
    dt[, bytes_raw   := as.numeric(bytes_raw)]
  })
  dt
}

# Append run rows to the manifest, keeping the on-disk file at the canonical
# schema. If the existing file has a stale (narrower) header, migrate it in
# place first -- re-read every row through read_snapshots(), rewrite the whole
# file with the canonical header, THEN append -- so the manifest never contains
# a mix of column widths again.
write_snapshots <- function(new_rows, path = SNAPSHOTS_CSV) {
  # normalize the new rows to the canonical column set/order
  new_rows <- as.data.table(new_rows)
  for (col in SNAPSHOT_COLS)
    if (!col %in% names(new_rows))
      new_rows[, (col) := NA]
  new_rows <- new_rows[, ..SNAPSHOT_COLS]

  if (!file.exists(path)) {
    fwrite(new_rows, path)
    return(invisible())
  }
  hdr <- tryCatch(readLines(path, n = 1L, warn = FALSE), error = function(e) "")
  hdr_cols <- if (length(hdr) && nzchar(hdr))
    strsplit(hdr, ",", fixed = TRUE)[[1]]
  else
    character(0)
  header_is_canonical <- identical(hdr_cols, SNAPSHOT_COLS)

  if (header_is_canonical) {
    fwrite(new_rows, path, append = TRUE)
  } else {
    # migrate: rebuild the whole file at the canonical schema, then append
    log_msg("Manifest header is stale (%d cols); migrating to canonical %d-col schema.",
            length(hdr_cols), length(SNAPSHOT_COLS))
    existing <- read_snapshots(path)
    combined <- rbindlist(list(existing, new_rows), fill = TRUE)[, ..SNAPSHOT_COLS]
    fwrite(combined, path)  # overwrite with canonical header + all rows
  }
  invisible()
}

url_key <- function(url)
  digest::digest(url, algo = "sha256")

guess_ext <- function(url) {
  path <- str_split(url, "\\?")[[1]][1]
  ext  <- str_extract(tolower(path), "\\.[a-z0-9]+(\\.gz)?$")
  if (is.na(ext) || !nzchar(ext))
    ext <- ".bin"
  ext
}

sniff_format <- function(path) {
  con   <- file(path, "rb")
  on.exit(close(con), add = TRUE)
  magic <- readBin(con, "raw", n = 16L)
  if (length(magic) == 0)
    return("unknown")
  b <- as.integer(magic)
  if (length(b) >= 4 &&
      b[1] == 0x50 &&
      b[2] == 0x4B && b[3] %in% c(0x03, 0x05, 0x07))
    return("zip")
  if (length(b) >= 2 && b[1] == 0x1f && b[2] == 0x8b)
    return("gzip")
  i <- 1L
  if (length(b) >= 3 &&
      b[1] == 0xEF && b[2] == 0xBB && b[3] == 0xBF)
    i <- 4L
  while (i <= length(b) &&
         b[i] %in% c(0x20, 0x09, 0x0A, 0x0D))
    i <- i + 1L
  if (i <= length(b) && b[i] %in% c(0x7B, 0x5B))
    return("json")
  "csv"
}

is_compressed_format <- function(fmt)
  fmt %in% c("zip", "gzip")

ext_for_format <- function(fmt, url_ext) {
  switch(
    fmt,
    zip  = ".zip",
    gzip = ".gz",
    json = ".json",
    csv  = ".csv",
    if (nzchar(url_ext) &&
        url_ext != ".bin")
      url_ext
    else
      ".bin"
  )
}

blob_path_for_hash <- function(hash, ext) {
  file.path(BLOB_DIR, substr(hash, 1, 2), paste0(hash, ext))
}

sha256_file <-
  function(path)
    digest::digest(file = path, algo = "sha256")

compress_to_gz <- function(src, dst) {
  con_in  <- file(src, "rb")
  con_out <- gzfile(dst, "wb")
  on.exit({
    close(con_in)
    close(con_out)
  }, add = TRUE)
  repeat {
    buf <- readBin(con_in, "raw", n = 10 * 1024 * 1024)
    if (length(buf) == 0)
      break
    writeBin(buf, con_out)
  }
}

store_blob <- function(tmp_path, hash, fmt, url_ext) {
  base_ext    <- ext_for_format(fmt, url_ext)
  should_gzip <- !is_compressed_format(fmt)
  final_ext   <-
    if (should_gzip)
      paste0(base_ext, ".gz")
  else
    base_ext
  dest        <- blob_path_for_hash(hash, final_ext)
  if (file.exists(dest))
    return(list(path = dest, newly_stored = FALSE))
  if (!dir.exists(dirname(dest)))
    dir.create(dirname(dest), recursive = TRUE)
  if (should_gzip)
    compress_to_gz(tmp_path, dest)
  else
    file.copy(tmp_path, dest, overwrite = TRUE)
  list(path = dest, newly_stored = TRUE)
}

# ---- step 1: explode hpt_results.csv into (hospital, url) pairs -----------

load_url_hospital_map <- function(results_csv) {
  res <- fread(results_csv)
  res <- res[!is.na(mrf_urls) & mrf_urls != ""]
  if (nrow(res) == 0) {
    return(data.table(
      id = integer(),
      homepage = character(),
      name = character(),
      url = character()
    ))
  }
  name_col <- if ("name" %in% names(res))
    "name"
  else
    NA_character_
  res[, `:=`(.tmp_name = if (!is.na(name_col))
    get(name_col)
    else
      NA_character_)]
  long <-
    res[, .(url = str_trim(str_split(mrf_urls, " \\| ")[[1]])),
        by = .(id, homepage, name = .tmp_name)]
  long <- long[nzchar(url)]
  unique(long)
}

# ---- step 2: read prior manifest state (latest row per url, if any) ------

load_latest_state <- function() {
  empty <- data.table(
    url = character(),
    content_hash = character(),
    storage_path = character(),
    bytes_raw = numeric(),
    detected_format = character()
  )
  # read_snapshots() reads the full manifest defensively (never truncates at a
  # schema-width change) and always returns the canonical columns.
  hist <- read_snapshots()
  if (nrow(hist) == 0)
    return(empty)
  setorder(hist, url, run_date)
  hist[, .SD[.N], by = url][, .(url, content_hash, storage_path, bytes_raw, detected_format)]
}

# ---- step 3: conditional fetch + hash + store for one URL -----------------

fetch_and_store_one <- function(url, prior) {
  key      <- url_key(url)
  ext      <- guess_ext(url)
  etag_path <- file.path(ETAG_DIR, paste0(key, ".etag"))
  tmp_path <- file.path(TMP_DIR, paste0(key, ext))
  if (file.exists(tmp_path))
    unlink(tmp_path)
  
  fmt <- "%{http_code}\t%{size_download}\t%{content_type}"
  args <-
    c(
      "-sL",
      "--max-time",
      TIMEOUT_SECS,
      "-A",
      shQuote(USER_AGENT),
      "--max-filesize",
      as.character(MAX_MB * 1024 * 1024),
      "--etag-compare",
      shQuote(etag_path),
      "--etag-save",
      shQuote(etag_path),
      "-o",
      shQuote(tmp_path),
      "-w",
      shQuote(fmt),
      shQuote(url)
    )
  out <-
    tryCatch(
      system2("curl", args, stdout = TRUE, stderr = FALSE),
      error = function(e)
        character(0)
    )
  parts <-
    if (length(out) > 0 &&
        nzchar(out[length(out)]))
      str_split(out[length(out)], "\t")[[1]]
  else
    character(0)
  http_code    <- suppressWarnings(as.integer(parts[1]))
  content_type <-
    if (length(parts) >= 3 &&
        nzchar(parts[3]))
      parts[3]
  else
    NA_character_
  
  base_row <- data.table(
    run_date = RUN_DATE,
    url = url,
    http_status = http_code,
    outcome = NA_character_,
    content_hash = NA_character_,
    storage_path = NA_character_,
    bytes_raw = NA_real_,
    detected_format = NA_character_,
    content_type = content_type
  )
  
  if (is.na(http_code) || http_code == 0) {
    base_row$outcome <- "fetch_failed_or_too_large"
    unlink(tmp_path)
    return(base_row)
  }
  
  if (http_code == 304) {
    unlink(tmp_path)
    if (!is.null(prior)) {
      base_row$outcome         <- "unchanged"
      base_row$content_hash    <- prior$content_hash
      base_row$storage_path    <- prior$storage_path
      base_row$bytes_raw       <- prior$bytes_raw
      base_row$detected_format <- prior$detected_format
    } else {
      base_row$outcome <- "unchanged_no_prior_record"
    }
    return(base_row)
  }
  
  if (http_code < 200 || http_code >= 400) {
    base_row$outcome <- paste0("http_", http_code)
    unlink(tmp_path)
    return(base_row)
  }
  
  if (!file.exists(tmp_path) || file.size(tmp_path) == 0) {
    base_row$outcome <- "empty_response"
    unlink(tmp_path)
    return(base_row)
  }
  
  detected <- sniff_format(tmp_path)
  hash     <- sha256_file(tmp_path)
  bytes    <- file.size(tmp_path)
  base_row$detected_format <- detected
  
  if (!is.null(prior) && identical(prior$content_hash, hash)) {
    base_row$outcome         <- "unchanged_by_hash"
    base_row$content_hash    <- hash
    base_row$storage_path    <- prior$storage_path
    base_row$bytes_raw       <- bytes
    base_row$detected_format <- detected
    unlink(tmp_path)
    return(base_row)
  }
  
  stored <- store_blob(tmp_path, hash, detected, ext)
  unlink(tmp_path)
  
  base_row$outcome <- if (is.null(prior))
    "new_url"
  else
    "updated"
  # Cheap red flag, not a filter: a real MRF/beacon response is essentially
  # never this small. Most likely cause is a server returning an error page
  # or a login/redirect page with a 200 status instead of real content. Still
  # stored (that's itself useful evidence, e.g. for a compliance angle), just
  # tagged so it's visible without waiting for process_mrf.R's deeper check.
  if (bytes < MIN_PLAUSIBLE_BYTES) {
    base_row$outcome <- paste0(base_row$outcome, "_suspiciously_small")
  }
  base_row$content_hash <- hash
  base_row$storage_path <- stored$path
  base_row$bytes_raw    <- bytes
  base_row
}

# ---- main -----------------------------------------------------------------

if (sys.nframe() == 0) {   # driver: skipped when sourced by a parallel worker

stopifnot(file.exists(INPUT_RESULTS_CSV))
url_map <- load_url_hospital_map(INPUT_RESULTS_CSV)
fwrite(url_map, URL_MAP_CSV)

unique_urls <- unique(url_map$url)
log_msg(
  "Loaded %d hospital-url pairs -> %d unique URLs to check.",
  nrow(url_map),
  length(unique_urls)
)

latest_state <- load_latest_state()
setkey(latest_state, url)

# Precompute each URL's prior state HERE (latest_state lives only in the main
# process); hand each worker a self-contained (url, prior) pair. Fetches
# parallelize across hosts, stay serial + POLITE_SLEEP-spaced within a host --
# so any single hospital server sees at most one in-flight request. The blob
# store and etag/tmp paths are content-/url-hash addressed, so concurrent
# workers never collide on a write.
work <- lapply(unique_urls, function(u) {
  prior <- if (u %in% latest_state$url) as.list(latest_state[u]) else NULL
  list(url = u, prior = prior)
})

n_workers <- min(detect_workers(), max(1L, length(unique_urls)))
log_msg("Fetching %d unique URLs with %d worker(s) (PIPELINE_WORKERS overrides).",
        length(unique_urls), n_workers)
worker_init <- local({
  root <- getwd()
  function() { setwd(root); source(file.path("code", "02_mrf_fetch_and_store.R")) }
})
rows <- with_cluster(n_workers, worker_init = worker_init, FUN = function(cl) {
  par_by_host(cl, work, unique_urls,
              function(w) fetch_and_store_one(w$url, w$prior), sleep = POLITE_SLEEP)
})
run_results <- rbindlist(rows, fill = TRUE)

# Append via write_snapshots() (not a bare fwrite append): it normalizes rows to
# the canonical schema and, if the on-disk file has a stale/narrower header,
# migrates the whole file to the canonical schema before appending -- so the
# manifest never ends up with a mix of column widths that a later fread would
# truncate at.
write_snapshots(run_results, SNAPSHOTS_CSV)

tab <- table(run_results$outcome)
log_msg("Run complete. Outcomes: %s", paste(sprintf("%s=%d", names(tab), tab), collapse = ", "))

new_bytes <-
  run_results[outcome %in% c("new_url", "updated"), sum(bytes_raw, na.rm = TRUE)]
blob_bytes <-
  sum(file.size(list.files(
    BLOB_DIR, recursive = TRUE, full.names = TRUE
  )))
log_msg("Bytes freshly downloaded this run: %.1f MB", new_bytes / 1e6)
log_msg(
  "Total blob store size on disk:     %.1f MB across %d unique files",
  blob_bytes / 1e6,
  length(list.files(BLOB_DIR, recursive = TRUE))
)
log_msg("Total (url x run) manifest rows so far: %d", nrow(read_snapshots()))

}  # end driver guard: if (sys.nframe() == 0)