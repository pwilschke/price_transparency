# Baseline pipeline: resolve hospital homepages -> cms-hpt.txt -> MRF links
#
# Strategy
#   1. Start from the dolthub "homepage" for each hospital.
#   2. Follow HTTP redirects and record the FINAL effective URL. This handles
#      cases like southerntnwinchester.com -> highpointhealthsystem.com/winchester:
#      the redirected HOST ROOT (path stripped) is where the beacon file lives.
#   3. CMS requires a plaintext beacon file at the site root: <root>/cms-hpt.txt
#      containing direct link(s) to the machine-readable file(s) (MRFs).
#      Try the beacon at both the original host root and the redirected host root.
#   4. Parse the beacon permissively (formats vary in practice): extract every
#      URL, then classify which look like MRFs (standardcharges / .csv / .json).
#   5. HEAD-check each candidate MRF link to catch dead/old links and capture the
#      final URL, HTTP status, content-type, and size.
#
# NOTE: requires network access + `curl` on PATH. Intended to be run on a
# machine with internet; this file only defines the logic.

library(data.table)
library(arrow)
library(collapse)
library(stringr)

# ---- working directory -------------------------------------------------------
# Resolve the project root regardless of how this script is invoked: RStudio
# "Source", `Rscript process_urls.R` from any working directory (e.g. Task
# Scheduler's default, which is NOT this folder), or sourced from run_pipeline.R.
# Assumes this file lives in <project_root>/code/.
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
    return(getwd())  # last resort
  normalizePath(file.path(dirname(script_path), ".."))
}
setwd(find_project_root())
if (!dir.exists("data"))
  stop("Run from project root (a 'data' dir must exist).")

USER_AGENT <- "price-transparency-research/0.1 (contact: research)"
TIMEOUT    <- 30      # seconds per request
POLITE_SLEEP <-
  0.5   # seconds between hospitals, to be a good citizen

# ---- low-level curl helpers --------------------------------------------------

# Follow redirects, return final URL + status without downloading the body.
# Returns list(ok, http_code, final_url, content_type, size).
# Try HEAD first (cheap); if it fails, errors out, or the server rejects it
# (some hospital CDNs return 405/501 or nothing at all for HEAD even though
# GET works fine), retry with a plain GET.
curl_head <- function(url, allow_get_fallback = TRUE) {
  fmt <-
    "%{http_code}\t%{url_effective}\t%{content_type}\t%{size_download}"
  
  run <- function(method_flag) {
    out <- tryCatch(
      system2(
        "curl",
        c(
          "-sL",
          method_flag,
          "--max-time",
          TIMEOUT,
          "-A",
          shQuote(USER_AGENT),
          "-o",
          "/dev/null",
          "-w",
          shQuote(fmt),
          shQuote(url)
        ),
        stdout = TRUE,
        stderr = FALSE
      ),
      error = function(e)
        character(0)
    )
    if (length(out) == 0 || !nzchar(out[1]))
      return(NULL)
    parts <- str_split(out[length(out)], "\t")[[1]]
    code  <- suppressWarnings(as.integer(parts[1]))
    if (is.na(code) || code == 0)
      return(NULL)
    list(
      ok = code >= 200 && code < 400,
      http_code    = code,
      final_url    = ifelse(length(parts) >= 2, parts[2], NA_character_),
      content_type = ifelse(length(parts) >= 3, parts[3], NA_character_),
      size         = ifelse(
        length(parts) >= 4,
        suppressWarnings(as.numeric(parts[4])),
        NA_real_
      )
    )
  }
  
  res <- run("-I")  # HEAD
  if (is.null(res) || res$http_code %in% c(0, 403, 405, 501)) {
    if (allow_get_fallback)
      res <- run(character(0))  # GET fallback
  }
  if (is.null(res)) {
    return(
      list(
        ok = FALSE,
        http_code = NA_integer_,
        final_url = NA_character_,
        content_type = NA_character_,
        size = NA_real_
      )
    )
  }
  res
}

# Follow redirects and return the body as a single string (or NA).
curl_get <- function(url) {
  out <- tryCatch(
    system2(
      "curl",
      c(
        "-sL",
        "--max-time",
        TIMEOUT,
        "-A",
        shQuote(USER_AGENT),
        shQuote(url)
      ),
      stdout = TRUE,
      stderr = FALSE
    ),
    error = function(e)
      character(0)
  )
  if (length(out) == 0)
    return(NA_character_)
  paste(out, collapse = "\n")
}

# ---- URL utilities -----------------------------------------------------------

normalize_homepage <- function(url) {
  url <- str_trim(url)
  url[url == ""] <- NA_character_
  # add scheme if missing so curl/parsing behaves
  needs_scheme <- !is.na(url) & !str_detect(url, "^https?://")
  url[needs_scheme] <- paste0("https://", url[needs_scheme])
  url
}

# scheme://host  (path, query, fragment stripped). This is the key step that
# turns ".../winchester" into the site root where cms-hpt.txt should live.
root_of <- function(url) {
  m <- str_match(url, "^(https?)://([^/?#]+)")
  ifelse(is.na(m[, 1]), NA_character_, paste0(m[, 2], "://", tolower(m[, 3])))
}

# Given a homepage, produce the set of cms-hpt.txt URLs worth trying.
hpt_candidates <- function(homepage) {
  homepage <- normalize_homepage(homepage)
  if (is.na(homepage))
    return(character(0))
  res <- curl_head(homepage)
  roots <- c(root_of(homepage), root_of(res$final_url))
  roots <- unique(roots[!is.na(roots)])
  paste0(roots, "/cms-hpt.txt")
}

# ---- beacon parsing ----------------------------------------------------------

URL_RE <- "https?://[^\\s\"'<>)\\]]+"

# Does a URL look like an MRF? standardcharges naming or a data-file extension.
is_mrf_url <- function(url) {
  u <- tolower(url)
  str_detect(u, "standardcharges") |
    str_detect(u, "\\.(csv|json)(\\.gz)?($|\\?)")
}

# Pull all URLs out of raw beacon text, dedupe, and flag MRF-looking ones.
parse_hpt <- function(txt) {
  if (is.na(txt) ||
      !nzchar(txt))
    return(data.table(url = character(0), is_mrf = logical(0)))
  urls <- unlist(str_extract_all(txt, URL_RE))
  urls <-
    str_replace(urls, "[.,;)]+$", "")   # trim trailing punctuation
  urls <- unique(str_trim(urls))
  if (length(urls) == 0)
    return(data.table(url = character(0), is_mrf = logical(0)))
  data.table(url = urls, is_mrf = is_mrf_url(urls))
}

# ---- per-hospital driver -----------------------------------------------------

process_one <- function(homepage, id = NA) {
  base <- data.table(
    id = id,
    homepage = homepage,
    beacon_url = NA_character_,
    beacon_found = FALSE,
    n_urls = 0L,
    n_mrf = 0L,
    mrf_urls = NA_character_,
    notes = NA_character_
  )
  cands <-
    tryCatch(
      hpt_candidates(homepage),
      error = function(e)
        character(0)
    )
  if (length(cands) == 0) {
    base$notes <- "no candidate beacon URL"
    return(base)
  }
  
  for (cand in cands) {
    head_res <- curl_head(cand)
    if (!isTRUE(head_res$ok))
      next
    txt <- curl_get(cand)
    parsed <- parse_hpt(txt)
    base$beacon_url   <- cand
    base$beacon_found <- TRUE
    base$n_urls       <- nrow(parsed)
    mrfs <- parsed[is_mrf == TRUE, url]
    base$n_mrf    <- length(mrfs)
    base$mrf_urls <-
      if (length(mrfs))
        paste(mrfs, collapse = " | ")
    else
      NA_character_
    if (length(mrfs) == 0)
      base$notes <- "beacon found, no MRF-looking links"
    return(base)
  }
  base$notes <- "beacon not reachable at any candidate root"
  base
}

# Validate MRF links found across hospitals: HEAD each one, record status.
check_mrf_links <- function(results) {
  urls <-
    unique(unlist(str_split(na.omit(results$mrf_urls), " \\| ")))
  urls <- urls[nzchar(urls)]
  if (length(urls) == 0)
    return(data.table())
  rows <- lapply(urls, function(u) {
    h <- curl_head(u)
    Sys.sleep(POLITE_SLEEP)
    data.table(
      url = u,
      http_code = h$http_code,
      final_url = h$final_url,
      content_type = h$content_type,
      size = h$size,
      ok = h$ok
    )
  })
  rbindlist(rows)
}

# ---- run over a sample -------------------------------------------------------

urls <- fread("./data/hosp_urls.csv")

stopifnot("homepage" %in% names(urls))
# pick a human-readable name column if present, for output readability
name_col <- "organization_name"

# Sample size for this pass. Defaults to ALL hospitals (real monthly run).
# Override for a quick test, e.g.:  HPT_SAMPLE_N=50 Rscript process_urls.R
n_env <- Sys.getenv("HPT_SAMPLE_N", unset = NA)

N <- fifelse(is.na(n_env), Inf, as.integer(n_env))

  as.integer(n_env)
samp <- urls[!is.na(homepage) & homepage != ""]
samp <- samp[seq_len(min(N, .N))]

# Many rows share the exact same homepage (a health system's website often
# serves several hospitals/campuses). The beacon lives at the homepage's
# root regardless of which hospital row pointed us there, so there's no
# reason to look it up more than once per unique homepage -- fetch once,
# then reattach the result to every hospital row that shares it.
n_unique_hp <- uniqueN(samp$homepage)
message(
  sprintf(
    "Processing %d unique homepages (from %d hospital rows; %.0f%% deduplicated)...",
    n_unique_hp,
    nrow(samp),
    100 * (1 - n_unique_hp / nrow(samp))
  )
)

homepages <- unique(samp$homepage)
res_list <- vector("list", length(homepages))
for (i in seq_along(homepages)) {
  res_list[[i]] <- process_one(homepages[i], id = i)
  if (i %% 25 == 0)
    message(sprintf("...%d / %d homepages checked", i, length(homepages)))
  Sys.sleep(POLITE_SLEEP)
}
hp_results <- rbindlist(res_list, fill = TRUE)
hp_results[, id := NULL]   # this was just this loop's own homepage-index counter, not a hospital id

# reattach: one output row per ORIGINAL hospital row (same grain as before),
# each carrying whichever homepage-level result it maps to. `id` here is
# re-set to the hospital row's own position, matching the original
# semantics (each hospital row gets a distinct id), NOT the homepage loop's
# counter -- otherwise every hospital sharing one homepage would collide on
# the same id, which load_url_hospital_map() downstream relies on being
# per-hospital-row distinct.
samp[, .orig_order := .I]
keep_name <- fifelse(!is.na(name_col) &&
      name_col %in% names(samp), name_col,
      NA_character_)
hosp_cols <-
  data.table(
    .orig_order = samp$.orig_order,
    homepage = samp$homepage,
    name = if (!is.na(keep_name))
      samp[[keep_name]]
    else
      NA_character_
  )
results <-
  merge(hosp_cols, hp_results, by = "homepage", all.x = TRUE)
setorder(results, .orig_order)
results[, id := .orig_order]
results[, .orig_order := NULL]

fwrite(results, "./data/hpt_results.csv")
message(
  sprintf(
    "Beacon found for %d / %d (%.0f%%); %d hospitals with >=1 MRF link.",
    sum(results$beacon_found),
    nrow(results),
    100 * mean(results$beacon_found),
    sum(results$n_mrf > 0)
  )
)

link_status <- check_mrf_links(results)
if (nrow(link_status)) {
  fwrite(link_status, "./data/mrf_link_status.csv")
  message(sprintf(
    "MRF links checked: %d, live (2xx/3xx): %d.",
    nrow(link_status),
    sum(link_status$ok)
  ))
}
