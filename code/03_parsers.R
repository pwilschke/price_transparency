# 03_parsers.R
#
# Pure parsing functions for the MRF pipeline, factored out of
# 03_process_mrf.R so they can be source()d by parallel PSOCK workers WITHOUT
# re-running the driver (PSOCK workers are fresh R sessions with none of the
# parent's functions loaded). Everything here is side-effect-free at load time
# except creating TMP_DIR if absent -- crucially it does NOT wipe TMP_DIR (that
# is the driver's job; a worker wiping it would destroy other workers' scratch
# files mid-run).
#
# Loaded by: 03_process_mrf.R (the driver) and each parallel worker at init.
# Callers are responsible for setwd()-ing to the project root first, so the
# relative paths below resolve correctly.
#
# Contains: coerce_numeric_vec / coerce_date / split_pipe, blob reading,
# CSV metadata, gather_codes, ensure_schema / attach_meta, detect_type, and the
# three parsers (parse_tall_csv / parse_wide_csv / parse_json) + process_blob.
# ---------------------------------------------------------------------------

library(data.table)
library(stringr)
library(arrow)
library(jsonlite)
.HAVE_CALLR <- requireNamespace("callr", quietly = TRUE)

# ---- config (paths + shared schema constants used by the parsers) ---------

DATA_DIR      <- "./data"
BLOB_DIR      <- file.path(DATA_DIR, "blobs")
PARQUET_DIR   <- file.path(DATA_DIR, "parquet")
TMP_DIR       <- file.path(DATA_DIR, "tmp")

# Ensure TMP_DIR exists but NEVER wipe it here -- see file header.
if (!dir.exists(TMP_DIR)) dir.create(TMP_DIR, recursive = TRUE)

log_msg <- function(...) message(sprintf("[%s] %s", format(Sys.time(), "%H:%M:%S"), sprintf(...)))

# ---- size gating -----------------------------------------------------------
# Some hospital MRFs are enormous (multi-GB uncompressed), and fully
# materializing one in R -- especially JSON, which becomes a deeply nested list
# with substantial per-object overhead -- can exhaust available memory and crash
# the whole R session rather than raising a catchable error. Estimate the likely
# in-memory footprint BEFORE attempting to read/parse, and skip (log, don't
# crash) anything over the configured ceiling. Tune MAX_ESTIMATED_MB against
# your actual available RAM; JSON in particular can expand well beyond its
# uncompressed byte count once parsed into nested R list objects, so treat this
# ceiling as conservative, not exact -- it's sized to the on-disk/uncompressed
# footprint, not the eventual R object size.
MAX_ESTIMATED_MB     <- 1500  # skip (log, don't attempt) anything estimated above this
GZIP_EXPANSION_GUESS <- 8     # typical text-compression ratio for repetitive CSV/JSON
ZIP_EXPANSION_GUESS  <- 8     # fallback only, used if the zip's own listing can't be read

# Estimate a blob's uncompressed size in MB. The manifest already recorded the
# TRUE uncompressed size at fetch time (bytes_raw, measured before gzip
# compression) -- use that directly when present, since it's exact rather than
# a guess. Only fall back to inferring from the on-disk (possibly compressed)
# blob when bytes_raw is missing (e.g. an older manifest row from before that
# field existed). For a zip, the container's own directory listing gives the
# TRUE uncompressed size of each member for free. For gzip, there's no equally
# cheap trick (the trailing ISIZE field wraps at 4GB and can't be trusted for
# large files), so that fallback path uses a fixed expansion multiplier.
estimate_uncompressed_mb <- function(mrow) {
  if (!is.null(mrow$bytes_raw) && length(mrow$bytes_raw) == 1 &&
      !is.na(mrow$bytes_raw) && mrow$bytes_raw > 0) {
    return(as.numeric(mrow$bytes_raw) / 1e6)
  }
  storage_path <- mrow$storage_path
  on_disk_mb <- file.size(storage_path) / 1e6
  ext <- tolower(storage_path)
  if (str_detect(ext, "\\.zip$")) {
    members <- tryCatch(utils::unzip(storage_path, list = TRUE), error = function(e) NULL)
    if (!is.null(members) && nrow(members)) return(sum(members$Length) / 1e6)
    return(on_disk_mb * ZIP_EXPANSION_GUESS)
  }
  if (str_detect(ext, "\\.gz$")) return(on_disk_mb * GZIP_EXPANSION_GUESS)
  on_disk_mb
}

# Canonical output column order. Every parser is passed through ensure_schema()
# so the parquet dataset has one stable schema regardless of source format.
UNIFIED_COLS <- c(
  # provenance / metadata
  "content_hash", "source_url", "source_format", "run_date",
  "hospital_name", "last_updated_on", "version", "npi", "type_2_npi",
  "location_name", "hospital_address", "license_number", "license_state",
  "attester_name",
  # item / service level
  "description", "primary_code", "primary_code_type", "codes", "code_types",
  "setting", "drug_unit_of_measurement", "drug_type_of_measurement", "modifiers",
  "standard_charge_gross", "standard_charge_gross_raw",
  "standard_charge_discounted_cash", "standard_charge_discounted_cash_raw",
  "standard_charge_min", "standard_charge_min_raw",
  "standard_charge_max", "standard_charge_max_raw",
  "additional_generic_notes", "parse_ok",
  # payer / plan level
  "payer_name", "plan_name",
  "negotiated_dollar", "negotiated_dollar_raw",
  "negotiated_percentage", "negotiated_percentage_raw",
  "negotiated_algorithm", "methodology",
  "median_amount", "p10_percentile", "p90_percentile", "count",
  "additional_payer_notes"
)

# ---- value coercion -------------------------------------------------------

coerce_numeric_vec <- function(x) {
  raw     <- as.character(x)
  trimmed <- str_trim(raw)
  cleaned <- str_replace_all(trimmed, "[\\$,\\s]", "")
  cleaned[cleaned == ""] <- NA_character_
  val   <- suppressWarnings(as.numeric(cleaned))
  blank <- is.na(raw) | !nzchar(trimmed)
  ok    <- blank | !is.na(val)
  raw[blank] <- NA_character_
  list(value = val, raw = raw, ok = ok)
}

coerce_date <- function(x) {
  if (is.null(x) || length(x) == 0) return(NA_character_)
  s <- str_trim(as.character(x)[1])
  if (!nzchar(s) || is.na(s)) return(NA_character_)
  for (fmt in c("%Y-%m-%d", "%m/%d/%Y", "%m/%d/%y")) {
    d <- suppressWarnings(as.Date(s, format = fmt))
    if (!is.na(d)) return(format(d, "%Y-%m-%d"))
  }
  s
}

split_pipe <- function(x) {
  if (is.null(x) || length(x) == 0 || is.na(x) || !nzchar(x)) return(character(0))
  str_trim(str_split(as.character(x), "\\|")[[1]])
}

# ---- blob reading (handle .gz / .zip / plain) -----------------------------

sniff_text_format <- function(path) {
  con <- file(path, "rb"); on.exit(close(con), add = TRUE)
  b <- as.integer(readBin(con, "raw", n = 64L))
  if (length(b) == 0) return("empty")
  i <- 1L
  if (length(b) >= 3 && b[1] == 0xEF && b[2] == 0xBB && b[3] == 0xBF) i <- 4L
  while (i <= length(b) && b[i] %in% c(0x20, 0x09, 0x0A, 0x0D)) i <- i + 1L
  if (i <= length(b) && b[i] %in% c(0x7B, 0x5B)) return("json")
  "csv"
}

read_blob_members <- function(storage_path) {
  ext <- tolower(storage_path)
  if (!dir.exists(TMP_DIR)) dir.create(TMP_DIR, recursive = TRUE)
  
  if (str_detect(ext, "\\.zip$")) {
    members <- tryCatch(utils::unzip(storage_path, list = TRUE), error = function(e) NULL)
    if (is.null(members) || nrow(members) == 0) return(list())
    members <- members[members$Length > 0, , drop = FALSE]
    out <- list()
    for (nm in members$Name) {
      dest <- tempfile(tmpdir = TMP_DIR)
      con_in  <- unz(storage_path, nm, open = "rb")
      con_out <- file(dest, "wb")
      repeat { buf <- readBin(con_in, "raw", 8L * 1024^2); if (!length(buf)) break; writeBin(buf, con_out) }
      close(con_in); close(con_out)
      out[[length(out) + 1]] <- list(path = dest, member = nm, is_temp = TRUE)
    }
    return(out)
  }
  
  if (str_detect(ext, "\\.gz$")) {
    dest <- tempfile(tmpdir = TMP_DIR)
    con_in  <- gzfile(storage_path, "rb")
    con_out <- file(dest, "wb")
    repeat { buf <- readBin(con_in, "raw", 8L * 1024^2); if (!length(buf)) break; writeBin(buf, con_out) }
    close(con_in); close(con_out)
    return(list(list(path = dest, member = NA_character_, is_temp = TRUE)))
  }
  
  list(list(path = storage_path, member = NA_character_, is_temp = FALSE))
}

# ---- CSV metadata (rows 1-2) ----------------------------------------------

parse_metadata_csv <- function(path) {
  hdr <- tryCatch(as.character(unlist(fread(path, sep = ",", nrows = 1, header = FALSE, fill = TRUE, colClasses = "character"))),
                  error = function(e) character(0))
  val <- tryCatch(as.character(unlist(fread(path, sep = ",", skip = 1, nrows = 1, header = FALSE, fill = TRUE, colClasses = "character"))),
                  error = function(e) character(0))
  if (length(hdr) == 0) return(NULL)
  h <- tolower(str_trim(hdr))
  get_by <- function(name) {
    i <- which(h == name)
    if (length(i) && i[1] <= length(val)) str_trim(val[i[1]]) else NA_character_
  }
  lic_i     <- which(str_detect(h, "^license_number\\|"))
  lic_state <- if (length(lic_i)) toupper(str_trim(str_split(hdr[lic_i[1]], "\\|")[[1]][2])) else NA_character_
  lic_state <- if (!is.na(lic_state)) str_replace_all(lic_state, "[\\[\\]]", "") else NA_character_
  lic_num   <- if (length(lic_i) && lic_i[1] <= length(val)) str_trim(val[lic_i[1]]) else NA_character_
  
  npis <- split_pipe(get_by("type_2_npi"))
  list(
    hospital_name   = get_by("hospital_name"),
    last_updated_on = coerce_date(get_by("last_updated_on")),
    version         = get_by("version"),
    type_2_npi      = npis,
    npi             = if (length(npis)) npis[1] else "unknown",
    location_name   = split_pipe(get_by("location_name")),
    hospital_address= split_pipe(get_by("hospital_address")),
    license_number  = lic_num,
    license_state   = lic_state,
    attester_name   = get_by("attester_name")
  )
}

# ---- shared helpers for the parsers ---------------------------------------

gather_codes <- function(dt) {
  nm         <- names(dt)
  code_cols  <- nm[str_detect(nm, "^code\\|\\d+$")]
  n <- nrow(dt)
  if (length(code_cols) == 0) {
    return(list(codes = rep(list(character(0)), n), code_types = rep(list(character(0)), n),
                primary_code = rep(NA_character_, n), primary_code_type = rep(NA_character_, n)))
  }
  code_cols  <- code_cols[order(as.integer(str_extract(code_cols, "\\d+")))]
  type_cols  <- paste0(code_cols, "|type")
  nc <- length(code_cols)
  
  code_v <- str_trim(unlist(lapply(code_cols, function(cc) as.character(dt[[cc]])), use.names = FALSE))
  type_v <- str_trim(unlist(lapply(type_cols, function(tc)
    if (tc %in% nm) as.character(dt[[tc]]) else rep(NA_character_, n)), use.names = FALSE))
  rid  <- rep.int(seq_len(n), nc)
  slot <- rep(seq_len(nc), each = n)
  
  keep <- !is.na(code_v) & nzchar(code_v)
  L <- data.table(rid = rid[keep], slot = slot[keep], code = code_v[keep], type = type_v[keep])
  setorder(L, rid, slot)
  
  codes <- rep(list(character(0)), n); ctypes <- rep(list(character(0)), n)
  pcode <- rep(NA_character_, n); ptype <- rep(NA_character_, n)
  if (nrow(L)) {
    cby <- L[, .(v = list(code)), by = rid]; codes[cby$rid]  <- cby$v
    tby <- L[, .(v = list(type)), by = rid]; ctypes[tby$rid] <- tby$v
    prim <- L[, .SD[1], by = rid]
    pcode[prim$rid] <- prim$code; ptype[prim$rid] <- prim$type
  }
  list(codes = codes, code_types = ctypes, primary_code = pcode, primary_code_type = ptype)
}

LIST_COLS    <- c("type_2_npi", "location_name", "hospital_address", "codes", "code_types", "modifiers")
NUMERIC_COLS <- c("standard_charge_gross", "standard_charge_discounted_cash",
                  "standard_charge_min", "standard_charge_max",
                  "negotiated_dollar", "negotiated_percentage",
                  "median_amount", "p10_percentile", "p90_percentile")

ensure_schema <- function(dt) {
  dt <- as.data.table(dt)
  n <- nrow(dt)
  for (col in UNIFIED_COLS) {
    if (col %in% LIST_COLS) {
      if (!col %in% names(dt)) set(dt, j = col, value = rep(list(character(0)), n))
      else set(dt, j = col, value = lapply(dt[[col]], function(x) if (is.null(x)) character(0) else as.character(x)))
    } else if (col == "parse_ok") {
      if (!col %in% names(dt)) dt[, parse_ok := rep(TRUE, n)]
      else dt[, parse_ok := as.logical(parse_ok)]
    } else if (col %in% NUMERIC_COLS) {
      if (!col %in% names(dt)) dt[, (col) := rep(NA_real_, n)]
      else set(dt, j = col, value = as.numeric(dt[[col]]))
    } else {
      if (!col %in% names(dt)) dt[, (col) := rep(NA_character_, n)]
      else set(dt, j = col, value = as.character(dt[[col]]))
    }
  }
  setcolorder(dt, UNIFIED_COLS)
  dt[, ..UNIFIED_COLS]
}

attach_meta <- function(dt, meta, prov) {
  n <- nrow(dt)
  if (n == 0) return(dt)
  dt[, `:=`(
    content_hash  = prov$content_hash, source_url = prov$source_url,
    source_format = prov$source_format, run_date = prov$run_date,
    hospital_name = meta$hospital_name, last_updated_on = meta$last_updated_on,
    version = meta$version, npi = meta$npi,
    license_number = meta$license_number, license_state = meta$license_state,
    attester_name = meta$attester_name
  )]
  dt[, type_2_npi       := rep(list(meta$type_2_npi), n)]
  dt[, location_name    := rep(list(meta$location_name), n)]
  dt[, hospital_address := rep(list(meta$hospital_address), n)]
  dt
}

# ---- detect_type ----------------------------------------------------------

detect_type <- function(path) {
  fmt <- sniff_text_format(path)
  if (fmt == "empty") return("noncompliant")
  if (fmt == "json")  return("json")
  
  hdr <- tryCatch(names(fread(path, sep = ",", skip = 2, nrows = 0, fill = TRUE)),
                  error = function(e) character(0))
  if (length(hdr) == 0) return("noncompliant")
  h <- tolower(str_trim(hdr))
  
  has_gross <- any(h == "standard_charge|gross") || any(str_detect(h, "^standard_charge\\|(discounted_cash|min|max)$"))
  is_tall   <- ("payer_name" %in% h) && ("plan_name" %in% h)
  is_wide   <- any(str_detect(h, "^standard_charge\\|.+\\|.+\\|negotiated_"))
  
  if (is_tall) return("tall_csv")
  if (is_wide) return("wide_csv")
  if (has_gross && "description" %in% h) return("tall_csv")
  "noncompliant"
}

# ---- parse_tall_csv -------------------------------------------------------

TALL_RENAME <- c(
  "standard_charge|gross"           = "standard_charge_gross_src",
  "standard_charge|discounted_cash" = "standard_charge_discounted_cash_src",
  "standard_charge|min"             = "standard_charge_min_src",
  "standard_charge|max"             = "standard_charge_max_src",
  "standard_charge|negotiated_dollar"     = "negotiated_dollar_src",
  "standard_charge|negotiated_percentage" = "negotiated_percentage_src",
  "standard_charge|negotiated_algorithm"  = "negotiated_algorithm",
  "standard_charge|methodology"     = "methodology",
  "10th_percentile"                 = "p10_src",
  "90th_percentile"                 = "p90_src",
  "median_amount"                   = "median_src"
)

parse_tall_csv <- function(path, meta, prov) {
  dt <- fread(path, sep = ",", skip = 2, header = TRUE, fill = TRUE, colClasses = "character",
              na.strings = c("", "NA"))
  setnames(dt, names(dt), str_trim(names(dt)))
  for (old in names(TALL_RENAME)) if (old %in% names(dt)) setnames(dt, old, TALL_RENAME[[old]])
  
  cg <- gather_codes(dt)
  out <- data.table(
    description = if ("description" %in% names(dt)) dt$description else NA_character_,
    primary_code = cg$primary_code, primary_code_type = cg$primary_code_type,
    setting = if ("setting" %in% names(dt)) dt$setting else NA_character_,
    drug_unit_of_measurement = if ("drug_unit_of_measurement" %in% names(dt)) dt$drug_unit_of_measurement else NA_character_,
    drug_type_of_measurement = if ("drug_type_of_measurement" %in% names(dt)) dt$drug_type_of_measurement else NA_character_,
    payer_name = if ("payer_name" %in% names(dt)) dt$payer_name else NA_character_,
    plan_name  = if ("plan_name" %in% names(dt)) dt$plan_name else NA_character_,
    negotiated_algorithm = if ("negotiated_algorithm" %in% names(dt)) dt$negotiated_algorithm else NA_character_,
    methodology = if ("methodology" %in% names(dt)) dt$methodology else NA_character_,
    count = if ("count" %in% names(dt)) dt$count else NA_character_,
    additional_generic_notes = if ("additional_generic_notes" %in% names(dt)) dt$additional_generic_notes else NA_character_
  )
  out[, codes := cg$codes][, code_types := cg$code_types]
  out[, modifiers := lapply(if ("modifiers" %in% names(dt)) dt$modifiers else rep(NA, nrow(dt)),
                            function(x) if (is.na(x) || !nzchar(x)) character(0) else str_trim(str_split(x, "[|,]")[[1]]))]
  
  add_and_set <- function(src, dst) {
    if (src %in% names(dt)) { c3 <- coerce_numeric_vec(dt[[src]]) }
    else c3 <- list(value = rep(NA_real_, nrow(out)), raw = rep(NA_character_, nrow(out)), ok = rep(TRUE, nrow(out)))
    out[, (dst) := c3$value]; out[, (paste0(dst, "_raw")) := c3$raw]; c3$ok
  }
  ok <- rep(TRUE, nrow(out))
  ok <- ok & add_and_set("standard_charge_gross_src",           "standard_charge_gross")
  ok <- ok & add_and_set("standard_charge_discounted_cash_src", "standard_charge_discounted_cash")
  ok <- ok & add_and_set("standard_charge_min_src",             "standard_charge_min")
  ok <- ok & add_and_set("standard_charge_max_src",             "standard_charge_max")
  ok <- ok & add_and_set("negotiated_dollar_src",               "negotiated_dollar")
  ok <- ok & add_and_set("negotiated_percentage_src",           "negotiated_percentage")
  set_num_noraw <- function(src, dst) {
    if (src %in% names(dt)) { c3 <- coerce_numeric_vec(dt[[src]]); out[, (dst) := c3$value]; return(c3$ok) }
    out[, (dst) := NA_real_]; rep(TRUE, nrow(out))
  }
  ok <- ok & set_num_noraw("median_src", "median_amount")
  ok <- ok & set_num_noraw("p10_src",    "p10_percentile")
  ok <- ok & set_num_noraw("p90_src",    "p90_percentile")
  
  out[, parse_ok := ok]
  out[, additional_payer_notes := NA_character_]
  out <- attach_meta(out, meta, prov)
  ensure_schema(out)
}

# ---- parse_wide_csv -------------------------------------------------------

parse_payer_col <- function(cn) {
  parts <- str_trim(str_split(cn, "\\|")[[1]])
  n <- length(parts)
  head <- tolower(parts[1])
  if (head == "standard_charge" && n >= 4) {
    field <- tolower(parts[n]); plan <- parts[n - 1]; payer <- paste(parts[2:(n - 2)], collapse = "|")
    fld <- switch(field,
                  "negotiated_dollar" = "negotiated_dollar",
                  "negotiated_percentage" = "negotiated_percentage",
                  "negotiated_algorithm" = "negotiated_algorithm",
                  "methodology" = "methodology", NULL)
    if (is.null(fld)) return(NULL)
    return(list(payer = payer, plan = plan, field = fld))
  }
  if (head %in% c("median_amount", "10th_percentile", "90th_percentile", "count", "additional_payer_notes") && n >= 3) {
    plan <- parts[n]; payer <- paste(parts[2:(n - 1)], collapse = "|")
    fld <- switch(head,
                  "median_amount" = "median_amount",
                  "10th_percentile" = "p10_percentile",
                  "90th_percentile" = "p90_percentile",
                  "count" = "count",
                  "additional_payer_notes" = "additional_payer_notes")
    return(list(payer = payer, plan = plan, field = fld))
  }
  NULL
}

parse_wide_csv <- function(path, meta, prov) {
  dt <- fread(path, sep = ",", skip = 2, header = TRUE, fill = TRUE, colClasses = "character",
              na.strings = c("", "NA"))
  setnames(dt, names(dt), str_trim(names(dt)))
  dt[, .rid := .I]
  
  cg <- gather_codes(dt)
  item <- data.table(.rid = dt$.rid,
                     description = if ("description" %in% names(dt)) dt$description else NA_character_,
                     primary_code = cg$primary_code, primary_code_type = cg$primary_code_type,
                     setting = if ("setting" %in% names(dt)) dt$setting else NA_character_,
                     drug_unit_of_measurement = if ("drug_unit_of_measurement" %in% names(dt)) dt$drug_unit_of_measurement else NA_character_,
                     drug_type_of_measurement = if ("drug_type_of_measurement" %in% names(dt)) dt$drug_type_of_measurement else NA_character_,
                     additional_generic_notes = if ("additional_generic_notes" %in% names(dt)) dt$additional_generic_notes else NA_character_
  )
  item[, codes := cg$codes][, code_types := cg$code_types]
  item[, modifiers := lapply(if ("modifiers" %in% names(dt)) dt$modifiers else rep(NA, nrow(dt)),
                             function(x) if (is.na(x) || !nzchar(x)) character(0) else str_trim(str_split(x, "[|,]")[[1]]))]
  item_ok <- rep(TRUE, nrow(item))
  add_item <- function(src, dst) {
    if (src %in% names(dt)) { c3 <- coerce_numeric_vec(dt[[src]]) }
    else c3 <- list(value = rep(NA_real_, nrow(item)), raw = rep(NA_character_, nrow(item)), ok = rep(TRUE, nrow(item)))
    item[, (dst) := c3$value]; item[, (paste0(dst, "_raw")) := c3$raw]; c3$ok
  }
  item_ok <- item_ok & add_item("standard_charge|gross",           "standard_charge_gross")
  item_ok <- item_ok & add_item("standard_charge|discounted_cash", "standard_charge_discounted_cash")
  item_ok <- item_ok & add_item("standard_charge|min",             "standard_charge_min")
  item_ok <- item_ok & add_item("standard_charge|max",             "standard_charge_max")
  item[, item_ok := item_ok]
  
  payer_map <- Filter(Negate(is.null), setNames(lapply(names(dt), parse_payer_col), names(dt)))
  long_list <- list()
  if (length(payer_map)) {
    keys <- vapply(payer_map, function(p) paste(p$payer, p$plan, sep = "\r"), character(1))
    for (k in unique(keys)) {
      cols_k <- names(payer_map)[keys == k]
      info   <- payer_map[[cols_k[1]]]
      g <- data.table(.rid = dt$.rid, payer_name = info$payer, plan_name = info$plan)
      g_ok <- rep(TRUE, nrow(g))
      for (cn in cols_k) {
        fld <- payer_map[[cn]]$field
        if (fld %in% c("negotiated_dollar", "negotiated_percentage")) {
          c3 <- coerce_numeric_vec(dt[[cn]]); g[, (fld) := c3$value]; g[, (paste0(fld, "_raw")) := c3$raw]; g_ok <- g_ok & c3$ok
        } else if (fld %in% c("median_amount", "p10_percentile", "p90_percentile")) {
          c3 <- coerce_numeric_vec(dt[[cn]]); g[, (fld) := c3$value]; g_ok <- g_ok & c3$ok
        } else {
          g[, (fld) := str_trim(as.character(dt[[cn]]))]
        }
      }
      g[, payer_ok := g_ok]
      long_list[[length(long_list) + 1]] <- g
    }
  }
  
  if (length(long_list)) {
    payer_long <- rbindlist(long_list, fill = TRUE)
    has_charge <- rep(FALSE, nrow(payer_long))
    for (col in c("negotiated_dollar", "negotiated_percentage")) if (col %in% names(payer_long)) has_charge <- has_charge | !is.na(payer_long[[col]])
    if ("negotiated_algorithm" %in% names(payer_long)) has_charge <- has_charge | (!is.na(payer_long$negotiated_algorithm) & nzchar(payer_long$negotiated_algorithm))
    payer_long <- payer_long[has_charge]
  } else {
    payer_long <- data.table(.rid = integer(0))
  }
  
  if (nrow(payer_long)) {
    res_payer <- merge(payer_long, item, by = ".rid", all.x = TRUE)
    covered   <- unique(payer_long$.rid)
    base      <- item[!.rid %in% covered]
  } else {
    res_payer <- data.table()
    base      <- item
  }
  out <- rbindlist(list(res_payer, base), fill = TRUE)
  po <- rep(TRUE, nrow(out))
  if ("item_ok" %in% names(out))  po <- po & (is.na(out$item_ok)  | out$item_ok)
  if ("payer_ok" %in% names(out)) po <- po & (is.na(out$payer_ok) | out$payer_ok)
  out[, parse_ok := po]
  out[, c(".rid", "item_ok", "payer_ok") := NULL]
  out <- attach_meta(out, meta, prov)
  ensure_schema(out)
}

# ---- parse_json -----------------------------------------------------------

.HAVE_SIMDJSON <- requireNamespace("RcppSimdJson", quietly = TRUE)

read_json_root <- function(path) {
  if (.HAVE_SIMDJSON) {
    root <- tryCatch(
      RcppSimdJson::fload(path, max_simplify_lvl = "list"),
      error = function(e) {
        log_msg("WARN: RcppSimdJson failed on %s (%s); falling back to jsonlite (slow).",
                basename(path), conditionMessage(e))
        NULL
      }
    )
    if (!is.null(root)) return(root)
  } else {
    log_msg("WARN: RcppSimdJson not installed; using jsonlite (slow) for %s. Install RcppSimdJson to speed up JSON parsing.",
            basename(path))
  }
  jsonlite::fromJSON(path, simplifyVector = FALSE)
}

parse_json <- function(path, prov) {
  root <- read_json_root(path)
  
  g  <- function(x, k) { v <- x[[k]]; if (is.null(v)) NA else v }
  ga <- function(x, k) { v <- x[[k]]; if (is.null(v)) list() else v }
  chr1 <- function(v) if (is.null(v)) NA_character_ else as.character(v[1])
  num <- function(v) {
    if (is.null(v)) return(list(value = NA_real_, ok = TRUE))
    if (length(v) > 1) v <- v[1]
    if (is.na(v)) return(list(value = NA_real_, ok = TRUE))
    vv <- suppressWarnings(as.numeric(v))
    list(value = vv, ok = !is.na(vv))
  }
  numv <- function(v) {
    if (is.null(v)) return(NA_real_)
    if (length(v) > 1) v <- v[1]
    if (is.na(v)) return(NA_real_)
    suppressWarnings(as.numeric(v))
  }
  
  npis <- unlist(ga(root, "type_2_npi"))
  meta <- list(
    hospital_name    = as.character(g(root, "hospital_name")),
    last_updated_on  = coerce_date(g(root, "last_updated_on")),
    version          = as.character(g(root, "version")),
    type_2_npi       = as.character(npis),
    npi              = if (length(npis)) as.character(npis[1]) else "unknown",
    location_name    = as.character(unlist(ga(root, "location_name"))),
    hospital_address = as.character(unlist(ga(root, "hospital_address"))),
    license_number   = as.character(g(root[["license_information"]], "license_number")),
    license_state    = as.character(g(root[["license_information"]], "state")),
    attester_name    = as.character(g(root[["attestation"]], "attester_name"))
  )
  
  sci <- ga(root, "standard_charge_information")
  
  n_total <- 0L
  for (item in sci) {
    for (sc in ga(item, "standard_charges")) {
      n_total <- n_total + max(length(ga(sc, "payers_information")), 1L)
    }
  }
  
  if (n_total == 0L) {
    out <- attach_meta(data.table(), meta, prov)
    return(ensure_schema(out))
  }
  
  NA_c <- rep(NA_character_, n_total); NA_r <- rep(NA_real_, n_total)
  col <- list(
    description = NA_c, primary_code = NA_c, primary_code_type = NA_c,
    setting = NA_c, drug_unit_of_measurement = NA_c, drug_type_of_measurement = NA_c,
    standard_charge_gross = NA_r, standard_charge_discounted_cash = NA_r,
    standard_charge_min = NA_r, standard_charge_max = NA_r,
    additional_generic_notes = NA_c,
    payer_name = NA_c, plan_name = NA_c,
    negotiated_dollar = NA_r, negotiated_percentage = NA_r,
    negotiated_algorithm = NA_c, methodology = NA_c,
    median_amount = NA_r, p10_percentile = NA_r, p90_percentile = NA_r,
    count = NA_c, additional_payer_notes = NA_c,
    parse_ok = logical(n_total)
  )
  codes_col <- vector("list", n_total)
  ctypes_col <- vector("list", n_total)
  modifiers_col <- vector("list", n_total)
  
  k <- 0L
  for (item in sci) {
    desc     <- as.character(g(item, "description"))
    codeinfo <- ga(item, "code_information")
    codes  <- vapply(codeinfo, function(c) chr1(c[["code"]]), character(1))
    ctypes <- vapply(codeinfo, function(c) chr1(c[["type"]]), character(1))
    p_code <- if (length(codes))  codes[1]  else NA_character_
    p_type <- if (length(ctypes)) ctypes[1] else NA_character_
    drug   <- item[["drug_information"]]
    d_unit <- if (is.null(drug)) NA_character_ else chr1(drug[["unit"]])
    d_type <- if (is.null(drug)) NA_character_ else chr1(drug[["type"]])
    
    for (sc in ga(item, "standard_charges")) {
      gross <- num(g(sc, "gross_charge")); cash <- num(g(sc, "discounted_cash"))
      mn    <- num(g(sc, "minimum"));      mx   <- num(g(sc, "maximum"))
      item_ok <- gross$ok && cash$ok && mn$ok && mx$ok
      mods    <- as.character(unlist(ga(sc, "modifier_code")))
      sc_set  <- chr1(sc[["setting"]])
      sc_note <- chr1(sc[["additional_generic_notes"]])
      
      payers <- ga(sc, "payers_information")
      np     <- length(payers)
      m      <- max(np, 1L)
      idx    <- (k + 1L):(k + m)
      
      col$description[idx]       <- desc
      col$primary_code[idx]      <- p_code
      col$primary_code_type[idx] <- p_type
      col$setting[idx]           <- sc_set
      col$drug_unit_of_measurement[idx] <- d_unit
      col$drug_type_of_measurement[idx] <- d_type
      col$standard_charge_gross[idx]           <- gross$value
      col$standard_charge_discounted_cash[idx] <- cash$value
      col$standard_charge_min[idx]             <- mn$value
      col$standard_charge_max[idx]             <- mx$value
      col$additional_generic_notes[idx]        <- sc_note
      for (jj in idx) { codes_col[[jj]] <- codes; ctypes_col[[jj]] <- ctypes; modifiers_col[[jj]] <- mods }
      
      if (np == 0L) {
        col$parse_ok[idx] <- item_ok
        k <- k + 1L
        next
      }
      
      col$payer_name[idx]           <- vapply(payers, function(p) chr1(p[["payer_name"]]), character(1))
      col$plan_name[idx]            <- vapply(payers, function(p) chr1(p[["plan_name"]]), character(1))
      col$negotiated_algorithm[idx] <- vapply(payers, function(p) chr1(p[["standard_charge_algorithm"]]), character(1))
      col$methodology[idx]          <- vapply(payers, function(p) chr1(p[["methodology"]]), character(1))
      col$count[idx]                <- vapply(payers, function(p) chr1(p[["count"]]), character(1))
      col$additional_payer_notes[idx] <- vapply(payers, function(p) chr1(p[["additional_payer_notes"]]), character(1))
      
      nd  <- vapply(payers, function(p) numv(p[["standard_charge_dollar"]]), numeric(1))
      npc <- vapply(payers, function(p) numv(p[["standard_charge_percentage"]]), numeric(1))
      col$negotiated_dollar[idx]     <- nd
      col$negotiated_percentage[idx] <- npc
      col$median_amount[idx]  <- vapply(payers, function(p) numv(p[["median_amount"]]), numeric(1))
      col$p10_percentile[idx] <- vapply(payers, function(p) numv(p[["10th_percentile"]]), numeric(1))
      col$p90_percentile[idx] <- vapply(payers, function(p) numv(p[["90th_percentile"]]), numeric(1))
      
      nd_raw  <- lapply(payers, function(p) p[["standard_charge_dollar"]])
      npc_raw <- lapply(payers, function(p) p[["standard_charge_percentage"]])
      nd_ok  <- vapply(seq_len(np), function(j) { v <- nd_raw[[j]];  is.null(v) || is.na(v[1]) || !is.na(nd[j]) },  logical(1))
      npc_ok <- vapply(seq_len(np), function(j) { v <- npc_raw[[j]]; is.null(v) || is.na(v[1]) || !is.na(npc[j]) }, logical(1))
      col$parse_ok[idx] <- item_ok & nd_ok & npc_ok
      
      k <- k + np
    }
  }
  
  out <- as.data.table(col)
  out[, codes := codes_col][, code_types := ctypes_col][, modifiers := modifiers_col]
  out <- attach_meta(out, meta, prov)
  ensure_schema(out)
}

# ---- process_blob ---------------------------------------------------------

process_blob <- function(mrow) {
  members <- tryCatch(read_blob_members(mrow$storage_path), error = function(e) list())
  if (length(members) == 0) {
    return(list(data = NULL, log = data.table(content_hash = mrow$content_hash,
                                              storage_path = mrow$storage_path, member = NA_character_,
                                              detected = "unreadable", n_rows = 0L, reason = "could not read blob")))
  }
  data_parts <- list(); logs <- list()
  for (m in members) {
    kind <- tryCatch(detect_type(m$path), error = function(e) "noncompliant")
    prov <- list(content_hash = mrow$content_hash, source_url = mrow$source_url,
                 source_format = kind, run_date = substr(as.character(mrow$run_date), 1, 10))
    dt <- NULL; reason <- NA_character_
    dt <- tryCatch({
      if (kind == "json")      { parse_json(m$path, prov) }
      else if (kind == "tall_csv") { parse_tall_csv(m$path, parse_metadata_csv(m$path), prov) }
      else if (kind == "wide_csv") { parse_wide_csv(m$path, parse_metadata_csv(m$path), prov) }
      else { reason <- "does not match CMS tall/wide/json"; NULL }
    }, error = function(e) { reason <<- paste("parse error:", conditionMessage(e)); NULL })
    
    if (!is.null(dt) && nrow(dt)) data_parts[[length(data_parts) + 1]] <- dt
    logs[[length(logs) + 1]] <- data.table(content_hash = mrow$content_hash,
                                           storage_path = mrow$storage_path, member = m$member, detected = kind,
                                           n_rows = if (is.null(dt)) 0L else nrow(dt), reason = reason)
    if (isTRUE(m$is_temp)) unlink(m$path)
  }
  list(data = if (length(data_parts)) rbindlist(data_parts, fill = TRUE) else NULL,
       log  = rbindlist(logs, fill = TRUE))
}

# Shared size-gate check. Returns NULL if the blob should proceed, or a
# fully-formed result list (to return immediately) if it should be skipped.
check_size_gate <- function(mrow) {
  est_mb <- tryCatch(estimate_uncompressed_mb(mrow), error = function(e) NA_real_)
  if (!is.na(est_mb) && est_mb > MAX_ESTIMATED_MB) {
    log <- data.table(
      content_hash = mrow$content_hash, storage_path = mrow$storage_path,
      member = NA_character_, detected = "skipped_too_large", n_rows = 0L,
      reason = sprintf("estimated ~%.0f MB uncompressed exceeds MAX_ESTIMATED_MB=%d",
                       est_mb, MAX_ESTIMATED_MB)
    )
    return(list(content_hash = mrow$content_hash, n_rows = 0L, wrote = FALSE,
                skipped_too_large = TRUE, log = log))
  }
  NULL
}

# The actual parse + write, with NO size check (caller's job). This is the
# piece run inside an isolated subprocess by process_and_write_blob_isolated()
# below, so a crash here only kills that subprocess, never the driver.
process_blob_and_write_core <- function(mrow) {
  r <- process_blob(mrow)
  wrote <- FALSE; n_rows <- 0L
  if (!is.null(r$data) && nrow(r$data)) {
    n_rows <- nrow(r$data)
    write_dataset(r$data, PARQUET_DIR, partitioning = c("run_date", "npi"),
                  format = "parquet",
                  basename_template = paste0(mrow$content_hash, "-part-{i}.parquet"),
                  existing_data_behavior = "overwrite")
    wrote <- TRUE
  }
  list(content_hash = mrow$content_hash, n_rows = n_rows, wrote = wrote,
       skipped_too_large = FALSE, log = r$log)
}

# Non-isolated version: size gate + core, all in the calling process. Kept as
# a fallback for when the `callr` package isn't installed -- no crash
# protection, but otherwise identical.
process_and_write_blob <- function(mrow) {
  gate <- check_size_gate(mrow)
  if (!is.null(gate)) return(gate)
  process_blob_and_write_core(mrow)
}

# Isolated version: the size gate stays cheap and in-process (no need to pay
# subprocess-spawn cost for a blob we're going to skip anyway), but the actual
# parse runs in a disposable subprocess with a timeout. This is what actually
# contains a crash: an R session dying from memory exhaustion, a native-code
# segfault inside arrow/RcppSimdJson, or a genuine infinite loop on a
# pathological file (e.g. a wide-format CSV with an extreme number of distinct
# payer/plan columns -- size in bytes doesn't predict this kind of blowup,
# since parse_wide_csv's cost scales with column cardinality, not file size).
# tryCatch cannot catch any of these: a crashed session never returns control
# to a tryCatch at all. A crash or timeout here is logged and the blob is
# simply not marked done, so it will be retried (and, if it fails again,
# logged again) on the next run rather than silently vanishing.
BLOB_TIMEOUT_SECS <- 600  # kill a single blob's parse if it hangs this long

process_and_write_blob_isolated <- function(mrow, root, timeout_secs = BLOB_TIMEOUT_SECS) {
  gate <- check_size_gate(mrow)
  if (!is.null(gate)) return(gate)
  
  if (!.HAVE_CALLR) {
    log_msg("WARN: callr not installed; running blob %s WITHOUT crash isolation. Install callr (install.packages(\"callr\")) for crash containment.",
            substr(mrow$content_hash, 1, 12))
    return(process_blob_and_write_core(mrow))
  }
  
  res <- tryCatch(
    callr::r(
      func = function(mrow, root) {
        setwd(root)
        source(file.path("code", "03_parsers.R"))
        process_blob_and_write_core(mrow)
      },
      args = list(mrow = mrow, root = root),
      timeout = timeout_secs
    ),
    error = function(e) {
      log_msg("WARN: blob %s crashed or timed out in an isolated subprocess (%s)",
              substr(mrow$content_hash, 1, 12), conditionMessage(e))
      NULL
    }
  )
  
  if (is.null(res)) {
    log <- data.table(content_hash = mrow$content_hash, storage_path = mrow$storage_path,
                      member = NA_character_, detected = "crashed_or_timed_out", n_rows = 0L,
                      reason = "subprocess crashed (likely memory exhaustion) or exceeded timeout")
    return(list(content_hash = mrow$content_hash, n_rows = 0L, wrote = FALSE,
                skipped_too_large = FALSE, log = log))
  }
  res
}