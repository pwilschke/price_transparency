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

library(data.table)
library(stringr)
library(arrow)
library(jsonlite)

# ---- config ---------------------------------------------------------------

DATA_DIR      <- "./data"
BLOB_DIR      <- file.path(DATA_DIR, "blobs")
MANIFEST_DIR  <- file.path(DATA_DIR, "manifest")
PARQUET_DIR   <- file.path(DATA_DIR, "parquet")
TMP_DIR       <- file.path(DATA_DIR, "tmp")

SNAPSHOTS_CSV     <- file.path(MANIFEST_DIR, "mrf_snapshots.csv")
URL_MAP_CSV       <- file.path(MANIFEST_DIR, "url_hospital_map.csv")
PARQUET_INDEX_CSV <- file.path(MANIFEST_DIR, "parquet_index.csv")
PROCESSING_LOG    <- file.path(MANIFEST_DIR, "processing_log.csv")

log_msg <- function(...) message(sprintf("[%s] %s", format(Sys.time(), "%H:%M:%S"), sprintf(...)))

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

# Vectorized numeric coercion. Strips $ , and whitespace. Returns value / raw /
# ok. ok is FALSE only when a non-blank string fails to parse (blanks are fine).
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

# ISO 8601 preferred; CMS also allows M/D/YYYY and MM/DD/YYYY. Returns an ISO
# string, or the original text if nothing parses (never silently blanked).
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

# Split a pipe-delimited scalar (CMS packs multi-valued metadata this way).
split_pipe <- function(x) {
  if (is.null(x) || length(x) == 0 || is.na(x) || !nzchar(x)) return(character(0))
  str_trim(str_split(as.character(x), "\\|")[[1]])
}

# ---- blob reading (handle .gz / .zip / plain) -----------------------------

# Sniff whether a decompressed file is json or csv from its first real byte,
# ignoring the (often lying) extension.
sniff_text_format <- function(path) {
  con <- file(path, "rb"); on.exit(close(con), add = TRUE)
  b <- as.integer(readBin(con, "raw", n = 64L))
  if (length(b) == 0) return("empty")
  i <- 1L
  if (length(b) >= 3 && b[1] == 0xEF && b[2] == 0xBB && b[3] == 0xBF) i <- 4L  # BOM
  while (i <= length(b) && b[i] %in% c(0x20, 0x09, 0x0A, 0x0D)) i <- i + 1L
  if (i <= length(b) && b[i] %in% c(0x7B, 0x5B)) return("json")
  "csv"
}

# Return one or more readable plain files for a stored blob. A zip may hold
# several members; each becomes its own logical file. Caller unlinks temps.
read_blob_members <- function(storage_path) {
  ext <- tolower(storage_path)
  if (!dir.exists(TMP_DIR)) dir.create(TMP_DIR, recursive = TRUE)

  if (str_detect(ext, "\\.zip$")) {
    members <- tryCatch(utils::unzip(storage_path, list = TRUE), error = function(e) NULL)
    if (is.null(members) || nrow(members) == 0) return(list())
    members <- members[members$Length > 0, , drop = FALSE]   # drop dir entries
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

  # plain
  list(list(path = storage_path, member = NA_character_, is_temp = FALSE))
}

# ---- CSV metadata (rows 1-2) ----------------------------------------------

# Read row 1 (metadata header) and row 2 (metadata values), map by header text
# so we tolerate extra optional columns. Returns a one-row list of meta fields.
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
  # license header looks like "license_number|[state]" or "license_number|CA"
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

# Gather the code|i / code|i|type column families into list columns + a scalar
# primary (first non-blank) code/type. Operates on a data.table `dt`.
gather_codes <- function(dt) {
  nm         <- names(dt)
  code_cols  <- nm[str_detect(nm, "^code\\|\\d+$")]
  # order by the numeric index so primary = code|1 when present
  code_cols  <- code_cols[order(as.integer(str_extract(code_cols, "\\d+")))]
  type_cols  <- paste0(code_cols, "|type")
  type_cols  <- ifelse(type_cols %in% nm, type_cols, NA_character_)
  n <- nrow(dt)
  if (length(code_cols) == 0) {
    return(list(codes = rep(list(character(0)), n), code_types = rep(list(character(0)), n),
                primary_code = rep(NA_character_, n), primary_code_type = rep(NA_character_, n)))
  }
  code_mat <- as.matrix(dt[, ..code_cols])
  type_mat <- matrix(NA_character_, n, length(code_cols))
  for (j in seq_along(type_cols)) if (!is.na(type_cols[j])) type_mat[, j] <- as.character(dt[[type_cols[j]]])
  codes <- vector("list", n); ctypes <- vector("list", n)
  pcode <- rep(NA_character_, n); ptype <- rep(NA_character_, n)
  for (i in seq_len(n)) {
    cv <- str_trim(as.character(code_mat[i, ])); tv <- str_trim(as.character(type_mat[i, ]))
    keep <- !is.na(cv) & nzchar(cv)
    cv <- cv[keep]; tv <- tv[keep]
    codes[[i]]  <- cv
    ctypes[[i]] <- tv
    if (length(cv)) { pcode[i] <- cv[1]; ptype[i] <- if (length(tv)) tv[1] else NA_character_ }
  }
  list(codes = codes, code_types = ctypes, primary_code = pcode, primary_code_type = ptype)
}

# Column types for the unified schema. Everything not listed here is character.
# Enforcing this on EVERY parser output guarantees all parquet files share one
# schema, so arrow::open_dataset() can read them together (an all-NA column in
# one file would otherwise infer as logical and clash with double elsewhere).
LIST_COLS    <- c("type_2_npi", "location_name", "hospital_address", "codes", "code_types", "modifiers")
NUMERIC_COLS <- c("standard_charge_gross", "standard_charge_discounted_cash",
                  "standard_charge_min", "standard_charge_max",
                  "negotiated_dollar", "negotiated_percentage",
                  "median_amount", "p10_percentile", "p90_percentile")

# Ensure a data.table has exactly UNIFIED_COLS, in order, each with its
# canonical type (missing -> typed NA / empty list).
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

# Attach file-level metadata (list-valued fields repeated as list columns).
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

  # csv: the DATA header is row 3
  hdr <- tryCatch(names(fread(path, sep = ",", skip = 2, nrows = 0, fill = TRUE)),
                  error = function(e) character(0))
  if (length(hdr) == 0) return("noncompliant")
  h <- tolower(str_trim(hdr))

  has_gross <- any(h == "standard_charge|gross") || any(str_detect(h, "^standard_charge\\|(discounted_cash|min|max)$"))
  is_tall   <- ("payer_name" %in% h) && ("plan_name" %in% h)
  is_wide   <- any(str_detect(h, "^standard_charge\\|.+\\|.+\\|negotiated_"))

  if (is_tall) return("tall_csv")
  if (is_wide) return("wide_csv")
  if (has_gross && "description" %in% h) return("tall_csv")  # item-level only, no payer cols
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
  # percentile / median: numeric, no raw kept
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

# Parse a wide payer column name into (payer, plan, field) or NULL if it is an
# item-level column.
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

  # --- item-level table (one row per item) ---
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

  # --- payer column groups ---
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
        } else {  # negotiated_algorithm, methodology, count, additional_payer_notes -> string
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
  # fold ok flags
  po <- rep(TRUE, nrow(out))
  if ("item_ok" %in% names(out))  po <- po & (is.na(out$item_ok)  | out$item_ok)
  if ("payer_ok" %in% names(out)) po <- po & (is.na(out$payer_ok) | out$payer_ok)
  out[, parse_ok := po]
  out[, c(".rid", "item_ok", "payer_ok") := NULL]
  out <- attach_meta(out, meta, prov)
  ensure_schema(out)
}

# ---- parse_json -----------------------------------------------------------

parse_json <- function(path, prov) {
  root <- jsonlite::fromJSON(path, simplifyVector = FALSE)
  g  <- function(x, k) { v <- x[[k]]; if (is.null(v)) NA else v }
  ga <- function(x, k) { v <- x[[k]]; if (is.null(v)) list() else v }

  npis <- unlist(ga(root, "type_2_npi"))
  meta <- list(
    hospital_name   = as.character(g(root, "hospital_name")),
    last_updated_on = coerce_date(g(root, "last_updated_on")),
    version         = as.character(g(root, "version")),
    type_2_npi      = as.character(npis),
    npi             = if (length(npis)) as.character(npis[1]) else "unknown",
    location_name   = as.character(unlist(ga(root, "location_name"))),
    hospital_address= as.character(unlist(ga(root, "hospital_address"))),
    license_number  = as.character(g(root[["license_information"]], "license_number")),
    license_state   = as.character(g(root[["license_information"]], "state")),
    attester_name   = as.character(g(root[["attestation"]], "attester_name"))
  )

  sci <- ga(root, "standard_charge_information")
  rows <- vector("list", 0)
  num <- function(v) { if (is.null(v) || is.na(v)) return(list(value = NA_real_, ok = TRUE)); vv <- suppressWarnings(as.numeric(v)); list(value = vv, ok = !is.na(vv)) }

  for (item in sci) {
    desc  <- as.character(g(item, "description"))
    codeinfo <- ga(item, "code_information")
    codes  <- vapply(codeinfo, function(c) as.character(g(c, "code")), character(1))
    ctypes <- vapply(codeinfo, function(c) as.character(g(c, "type")), character(1))
    drug   <- item[["drug_information"]]
    d_unit <- if (is.null(drug)) NA_character_ else as.character(g(drug, "unit"))
    d_type <- if (is.null(drug)) NA_character_ else as.character(g(drug, "type"))

    for (sc in ga(item, "standard_charges")) {
      setting <- as.character(g(sc, "setting"))
      gross <- num(g(sc, "gross_charge")); cash <- num(g(sc, "discounted_cash"))
      mn <- num(g(sc, "minimum")); mx <- num(g(sc, "maximum"))
      mods <- as.character(unlist(ga(sc, "modifier_code")))
      gnotes <- as.character(g(sc, "additional_generic_notes"))
      item_ok <- gross$ok && cash$ok && mn$ok && mx$ok
      payers <- ga(sc, "payers_information")

      mk_base <- function() data.table(
        description = desc, primary_code = if (length(codes)) codes[1] else NA_character_,
        primary_code_type = if (length(ctypes)) ctypes[1] else NA_character_,
        setting = setting, drug_unit_of_measurement = d_unit, drug_type_of_measurement = d_type,
        standard_charge_gross = gross$value, standard_charge_gross_raw = NA_character_,
        standard_charge_discounted_cash = cash$value, standard_charge_discounted_cash_raw = NA_character_,
        standard_charge_min = mn$value, standard_charge_min_raw = NA_character_,
        standard_charge_max = mx$value, standard_charge_max_raw = NA_character_,
        additional_generic_notes = gnotes
      )

      if (length(payers) == 0) {
        r <- mk_base(); r[, `:=`(payer_name = NA_character_, plan_name = NA_character_, parse_ok = item_ok)]
        r[, codes := list(codes)][, code_types := list(ctypes)][, modifiers := list(mods)]
        rows[[length(rows) + 1]] <- r
      } else {
        for (p in payers) {
          nd <- num(g(p, "standard_charge_dollar")); npct <- num(g(p, "standard_charge_percentage"))
          med <- num(g(p, "median_amount")); p10 <- num(g(p, "10th_percentile")); p90 <- num(g(p, "90th_percentile"))
          r <- mk_base()
          r[, `:=`(
            payer_name = as.character(g(p, "payer_name")), plan_name = as.character(g(p, "plan_name")),
            negotiated_dollar = nd$value, negotiated_percentage = npct$value,
            negotiated_algorithm = as.character(g(p, "standard_charge_algorithm")),
            methodology = as.character(g(p, "methodology")),
            median_amount = med$value, p10_percentile = p10$value, p90_percentile = p90$value,
            count = as.character(g(p, "count")), additional_payer_notes = as.character(g(p, "additional_payer_notes")),
            parse_ok = item_ok && nd$ok && npct$ok
          )]
          r[, codes := list(codes)][, code_types := list(ctypes)][, modifiers := list(mods)]
          rows[[length(rows) + 1]] <- r
        }
      }
    }
  }
  out <- if (length(rows)) rbindlist(rows, fill = TRUE) else data.table()
  out <- attach_meta(out, meta, prov)
  ensure_schema(out)
}

# ---- process_blob ---------------------------------------------------------

# Convert one manifest row (one content hash / storage path) into unified rows.
# Returns list(data = <DT or NULL>, log = <DT of per-logical-file outcomes>).
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

# ---- driver ---------------------------------------------------------------

load_blobs_to_process <- function() {
  stopifnot(file.exists(SNAPSHOTS_CSV))
  snap <- fread(SNAPSHOTS_CSV)
  snap <- snap[!is.na(storage_path) & storage_path != "" & !is.na(content_hash)]
  setorder(snap, content_hash, run_date)
  latest <- snap[, .SD[.N], by = content_hash]   # most recent sighting per blob
  # a readable source_url for provenance (first url mapped to this hash)
  if (file.exists(URL_MAP_CSV)) {
    umap <- unique(fread(URL_MAP_CSV)[, .(url, name)])
    latest <- merge(latest, umap, by = "url", all.x = TRUE)
  }
  setnames(latest, "url", "source_url")
  latest
}

if (sys.nframe() == 0) {
  for (d in c(PARQUET_DIR, MANIFEST_DIR, TMP_DIR)) if (!dir.exists(d)) dir.create(d, recursive = TRUE)

  blobs <- load_blobs_to_process()
  done  <- if (file.exists(PARQUET_INDEX_CSV)) fread(PARQUET_INDEX_CSV)$content_hash else character(0)
  todo  <- blobs[!content_hash %in% done]
  log_msg("Blobs total=%d, already converted=%d, to process=%d",
          nrow(blobs), length(done), nrow(todo))

  all_logs <- list(); newly_done <- character(0); total_rows <- 0L
  for (i in seq_len(nrow(todo))) {
    mrow <- as.list(todo[i])
    r <- process_blob(mrow)
    all_logs[[length(all_logs) + 1]] <- r$log
    if (!is.null(r$data) && nrow(r$data)) {
      # Unique per-blob filename so two blobs sharing a run_date/npi partition
      # don't clobber each other (default basename is always part-0.parquet).
      write_dataset(r$data, PARQUET_DIR, partitioning = c("run_date", "npi"),
                    format = "parquet",
                    basename_template = paste0(mrow$content_hash, "-part-{i}.parquet"),
                    existing_data_behavior = "overwrite")
      total_rows <- total_rows + nrow(r$data)
      newly_done <- c(newly_done, mrow$content_hash)
    }
    if (i %% 25 == 0) log_msg("...%d / %d blobs processed", i, nrow(todo))
  }

  if (length(all_logs)) {
    logdt <- rbindlist(all_logs, fill = TRUE)
    logdt[, run_at := format(Sys.time(), "%Y-%m-%dT%H:%M:%S")]
    fwrite(logdt, PROCESSING_LOG, append = file.exists(PROCESSING_LOG))
  }
  if (length(newly_done)) {
    idx <- data.table(content_hash = unique(newly_done),
                      converted_at = format(Sys.time(), "%Y-%m-%dT%H:%M:%S"))
    fwrite(idx, PARQUET_INDEX_CSV, append = file.exists(PARQUET_INDEX_CSV))
  }

  logdt <- if (length(all_logs)) rbindlist(all_logs, fill = TRUE) else data.table(detected = character(0))
  tab <- table(logdt$detected)
  log_msg("Run complete. Detected: %s", paste(sprintf("%s=%d", names(tab), tab), collapse = ", "))
  log_msg("Rows written this run: %d; blobs newly converted: %d", total_rows, length(newly_done))
}
