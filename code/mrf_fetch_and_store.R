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
# ---------------------------------------------------------------------------
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

# ---- config -------------------------------------------------------------

INPUT_RESULTS_CSV <- "./data/hpt_results.csv"   # output of the baseline beacon script
DATA_DIR          <- "./data"
BLOB_DIR          <- file.path(DATA_DIR, "blobs")
ETAG_DIR          <- file.path(DATA_DIR, "etags")
MANIFEST_DIR      <- file.path(DATA_DIR, "manifest")
TMP_DIR           <- file.path(DATA_DIR, "tmp")

SNAPSHOTS_CSV     <- file.path(MANIFEST_DIR, "mrf_snapshots.csv")
URL_MAP_CSV       <- file.path(MANIFEST_DIR, "url_hospital_map.csv")

USER_AGENT   <- "price-transparency-research/0.1 (contact: research)"
TIMEOUT_SECS <- 1800     # 30 min; some standard-charges files are enormous
MAX_MB       <- 3000     # skip (log, don't fetch) anything bigger than this
POLITE_SLEEP <- 0.5

RUN_DATE <- format(Sys.time(), "%Y-%m-%dT%H:%M:%S")

for (d in c(BLOB_DIR, ETAG_DIR, MANIFEST_DIR, TMP_DIR)) {
  if (!dir.exists(d)) dir.create(d, recursive = TRUE)
}

# ---- small helpers --------------------------------------------------------

log_msg <- function(...) message(sprintf("[%s] %s", format(Sys.time(), "%H:%M:%S"), sprintf(...)))

# Stable, filesystem-safe key for a URL (used for etag files / tmp files).
url_key <- function(url) digest::digest(url, algo = "sha256")

# Best-effort file extension from the URL path (ignoring query string).
guess_ext <- function(url) {
  path <- str_split(url, "\\?")[[1]][1]
  ext  <- str_extract(tolower(path), "\\.[a-z0-9]+(\\.gz)?$")
  if (is.na(ext) || !nzchar(ext)) ext <- ".bin"
  ext
}

# Detect the true format from the leading bytes of the downloaded file, rather
# than trusting the URL extension (which lies: .ashx serves a zip, query-only
# URLs have no extension at all, etc.). Returns one of:
#   "zip"  -> PK\x03\x04 (and empty/spanned variants)
#   "gzip" -> \x1f\x8b
#   "json" -> first non-whitespace char (after any UTF-8 BOM) is { or [
#   "csv"  -> anything else that has bytes (treated as delimited text)
#   "unknown" -> empty file
sniff_format <- function(path) {
  con   <- file(path, "rb")
  on.exit(close(con), add = TRUE)
  magic <- readBin(con, "raw", n = 16L)
  if (length(magic) == 0) return("unknown")
  b <- as.integer(magic)
  if (length(b) >= 4 && b[1] == 0x50 && b[2] == 0x4B &&
      b[3] %in% c(0x03, 0x05, 0x07)) return("zip")
  if (length(b) >= 2 && b[1] == 0x1f && b[2] == 0x8b) return("gzip")
  i <- 1L
  if (length(b) >= 3 && b[1] == 0xEF && b[2] == 0xBB && b[3] == 0xBF) i <- 4L  # UTF-8 BOM
  while (i <= length(b) && b[i] %in% c(0x20, 0x09, 0x0A, 0x0D)) i <- i + 1L    # whitespace
  if (i <= length(b) && b[i] %in% c(0x7B, 0x5B)) return("json")               # { or [
  "csv"
}

# A file already in a compressed container gains nothing from our gzip pass
# (and .ashx-that-is-really-a-zip would otherwise get double-compressed).
is_compressed_format <- function(fmt) fmt %in% c("zip", "gzip")

# Blob storage extension chosen from the *detected* format, so the filename no
# longer lies. Falls back to the URL-guessed extension only when we can't tell.
ext_for_format <- function(fmt, url_ext) {
  switch(fmt,
         zip  = ".zip",
         gzip = ".gz",
         json = ".json",
         csv  = ".csv",
         if (nzchar(url_ext) && url_ext != ".bin") url_ext else ".bin")
}

blob_path_for_hash <- function(hash, ext) {
  file.path(BLOB_DIR, substr(hash, 1, 2), paste0(hash, ext))
}

sha256_file <- function(path) digest::digest(file = path, algo = "sha256")

# Streaming gzip so we don't need the whole file in memory.
compress_to_gz <- function(src, dst) {
  con_in  <- file(src, "rb")
  con_out <- gzfile(dst, "wb")
  on.exit({ close(con_in); close(con_out) }, add = TRUE)
  repeat {
    buf <- readBin(con_in, "raw", n = 10 * 1024 * 1024)
    if (length(buf) == 0) break
    writeBin(buf, con_out)
  }
}

# Move a freshly-downloaded tmp file into the blob store under its content
# hash. If that hash is already stored (same content seen before, from this
# url or any other), just discard the new copy -- it's a duplicate.
# Gzip only formats that aren't already in a compressed container.
store_blob <- function(tmp_path, hash, fmt, url_ext) {
  base_ext    <- ext_for_format(fmt, url_ext)
  should_gzip <- !is_compressed_format(fmt)
  final_ext   <- if (should_gzip) paste0(base_ext, ".gz") else base_ext
  dest        <- blob_path_for_hash(hash, final_ext)
  if (file.exists(dest)) return(list(path = dest, newly_stored = FALSE))
  if (!dir.exists(dirname(dest))) dir.create(dirname(dest), recursive = TRUE)
  if (should_gzip) compress_to_gz(tmp_path, dest) else file.copy(tmp_path, dest, overwrite = TRUE)
  list(path = dest, newly_stored = TRUE)
}

# ---- step 1: explode hpt_results.csv into (hospital, url) pairs -----------

load_url_hospital_map <- function(results_csv) {
  res <- fread(results_csv)
  res <- res[!is.na(mrf_urls) & mrf_urls != ""]
  if (nrow(res) == 0) return(data.table(id = integer(), homepage = character(),
                                         name = character(), url = character()))
  name_col <- if ("name" %in% names(res)) "name" else NA_character_
  res[, `:=`(.tmp_name = if (!is.na(name_col)) get(name_col) else NA_character_)]
  long <- res[, .(url = str_trim(str_split(mrf_urls, " \\| ")[[1]])),
              by = .(id, homepage, name = .tmp_name)]
  long <- long[nzchar(url)]
  unique(long)
}

# ---- step 2: read prior manifest state (latest row per url, if any) ------

load_latest_state <- function() {
  empty <- data.table(url = character(), content_hash = character(),
                      storage_path = character(), bytes_raw = numeric(),
                      detected_format = character())
  if (!file.exists(SNAPSHOTS_CSV)) return(empty)
  hist <- fread(SNAPSHOTS_CSV)
  # tolerate manifests written before detected_format existed
  if (!"detected_format" %in% names(hist)) hist[, detected_format := NA_character_]
  setorder(hist, url, run_date)
  hist[, .SD[.N], by = url][, .(url, content_hash, storage_path, bytes_raw, detected_format)]
}

# ---- step 3: conditional fetch + hash + store for one URL -----------------

fetch_and_store_one <- function(url, prior) {
  key      <- url_key(url)
  ext      <- guess_ext(url)
  etag_path<- file.path(ETAG_DIR, paste0(key, ".etag"))
  tmp_path <- file.path(TMP_DIR, paste0(key, ext))
  if (file.exists(tmp_path)) unlink(tmp_path)

  fmt <- "%{http_code}\t%{size_download}\t%{content_type}"
  args <- c("-sL", "--max-time", TIMEOUT_SECS, "-A", shQuote(USER_AGENT),
            "--max-filesize", as.character(MAX_MB * 1024 * 1024),
            "--etag-compare", shQuote(etag_path),
            "--etag-save", shQuote(etag_path),
            "-o", shQuote(tmp_path),
            "-w", shQuote(fmt),
            shQuote(url))
  out <- tryCatch(
    system2("curl", args, stdout = TRUE, stderr = FALSE),
    error = function(e) character(0)
  )
  parts <- if (length(out) > 0 && nzchar(out[length(out)])) str_split(out[length(out)], "\t")[[1]] else character(0)
  http_code    <- suppressWarnings(as.integer(parts[1]))
  content_type <- if (length(parts) >= 3 && nzchar(parts[3])) parts[3] else NA_character_

  base_row <- data.table(
    run_date = RUN_DATE, url = url, http_status = http_code,
    outcome = NA_character_, content_hash = NA_character_,
    storage_path = NA_character_, bytes_raw = NA_real_,
    detected_format = NA_character_, content_type = content_type
  )

  # curl aborted due to --max-filesize, or genuinely failed to connect
  if (is.na(http_code) || http_code == 0) {
    base_row$outcome <- "fetch_failed_or_too_large"
    unlink(tmp_path)
    return(base_row)
  }

  # 304 Not Modified: server confirms unchanged. curl writes no new body.
  if (http_code == 304) {
    unlink(tmp_path)
    if (!is.null(prior)) {
      base_row$outcome         <- "unchanged"
      base_row$content_hash    <- prior$content_hash
      base_row$storage_path    <- prior$storage_path
      base_row$bytes_raw       <- prior$bytes_raw
      base_row$detected_format <- prior$detected_format
    } else {
      # 304 but we have no prior record -- shouldn't normally happen; treat as failure
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

  # We have real bytes: sniff their true format, then hash them.
  detected <- sniff_format(tmp_path)
  hash     <- sha256_file(tmp_path)
  bytes    <- file.size(tmp_path)
  base_row$detected_format <- detected

  if (!is.null(prior) && identical(prior$content_hash, hash)) {
    # Content identical to what we had, even though server didn't 304 us
    # (some servers don't honor If-None-Match reliably).
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

  base_row$outcome      <- if (is.null(prior)) "new_url" else "updated"
  base_row$content_hash <- hash
  base_row$storage_path <- stored$path
  base_row$bytes_raw    <- bytes
  base_row
}

# ---- main -----------------------------------------------------------------

if (sys.nframe() == 0) {

  stopifnot(file.exists(INPUT_RESULTS_CSV))
  url_map <- load_url_hospital_map(INPUT_RESULTS_CSV)
  fwrite(url_map, URL_MAP_CSV)   # rebuilt fresh each run; small and cheap

  unique_urls <- unique(url_map$url)
  log_msg("Loaded %d hospital-url pairs -> %d unique URLs to check.",
          nrow(url_map), length(unique_urls))

  latest_state <- load_latest_state()
  setkey(latest_state, url)

  rows <- vector("list", length(unique_urls))
  for (i in seq_along(unique_urls)) {
    u <- unique_urls[i]
    prior <- if (u %in% latest_state$url) as.list(latest_state[u]) else NULL
    rows[[i]] <- fetch_and_store_one(u, prior)
    if (i %% 25 == 0) log_msg("...%d / %d urls checked", i, length(unique_urls))
    Sys.sleep(POLITE_SLEEP)
  }
  run_results <- rbindlist(rows, fill = TRUE)

  fwrite(run_results, SNAPSHOTS_CSV, append = file.exists(SNAPSHOTS_CSV))

  # ---- summary ----
  tab <- table(run_results$outcome)
  log_msg("Run complete. Outcomes: %s",
          paste(sprintf("%s=%d", names(tab), tab), collapse = ", "))

  new_bytes <- run_results[outcome %in% c("new_url", "updated"), sum(bytes_raw, na.rm = TRUE)]
  blob_bytes <- sum(file.size(list.files(BLOB_DIR, recursive = TRUE, full.names = TRUE)))
  log_msg("Bytes freshly downloaded this run: %.1f MB", new_bytes / 1e6)
  log_msg("Total blob store size on disk:     %.1f MB across %d unique files",
          blob_bytes / 1e6, length(list.files(BLOB_DIR, recursive = TRUE)))
  log_msg("Total (url x run) manifest rows so far: %d", nrow(fread(SNAPSHOTS_CSV)))
}
