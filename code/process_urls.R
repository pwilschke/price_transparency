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
if (requireNamespace("rstudioapi", quietly = TRUE) && rstudioapi::isAvailable()) {
  setwd(dirname(rstudioapi::getSourceEditorContext()$path))
  setwd("../")
}
if (!dir.exists("data")) stop("Run from project root (a 'data' dir must exist).")

USER_AGENT <- "price-transparency-research/0.1 (contact: research)"
TIMEOUT    <- 30      # seconds per request
POLITE_SLEEP <- 0.5   # seconds between hospitals, to be a good citizen

# ---- low-level curl helpers --------------------------------------------------

# Follow redirects, return final URL + status without downloading the body.
# Returns list(ok, http_code, final_url, content_type, size).
# Try HEAD first (cheap); if it fails, errors out, or the server rejects it
# (some hospital CDNs return 405/501 or nothing at all for HEAD even though
# GET works fine), retry with a plain GET.
curl_head <- function(url, allow_get_fallback = TRUE) {
  fmt <- "%{http_code}\t%{url_effective}\t%{content_type}\t%{size_download}"
  
  run <- function(method_flag) {
    out <- tryCatch(
      system2("curl",
              c("-sL", method_flag, "--max-time", TIMEOUT, "-A", shQuote(USER_AGENT),
                "-o", "/dev/null", "-w", shQuote(fmt), shQuote(url)),
              stdout = TRUE, stderr = FALSE),
      error = function(e) character(0)
    )
    if (length(out) == 0 || !nzchar(out[1])) return(NULL)
    parts <- str_split(out[length(out)], "\t")[[1]]
    code  <- suppressWarnings(as.integer(parts[1]))
    if (is.na(code) || code == 0) return(NULL)
    list(ok = code >= 200 && code < 400,
         http_code    = code,
         final_url    = ifelse(length(parts) >= 2, parts[2], NA_character_),
         content_type = ifelse(length(parts) >= 3, parts[3], NA_character_),
         size         = ifelse(length(parts) >= 4, suppressWarnings(as.numeric(parts[4])), NA_real_))
  }
  
  res <- run("-I")  # HEAD
  if (is.null(res) || res$http_code %in% c(0, 403, 405, 501) ) {
    if (allow_get_fallback) res <- run(character(0))  # GET fallback
  }
  if (is.null(res)) {
    return(list(ok = FALSE, http_code = NA_integer_, final_url = NA_character_,
                content_type = NA_character_, size = NA_real_))
  }
  res
}

# Follow redirects and return the body as a single string (or NA).
curl_get <- function(url) {
  out <- tryCatch(
    system2("curl",
            c("-sL", "--max-time", TIMEOUT, "-A", shQuote(USER_AGENT), shQuote(url)),
            stdout = TRUE, stderr = FALSE),
    error = function(e) character(0)
  )
  if (length(out) == 0) return(NA_character_)
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
  if (is.na(homepage)) return(character(0))
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
  if (is.na(txt) || !nzchar(txt)) return(data.table(url = character(0), is_mrf = logical(0)))
  urls <- unlist(str_extract_all(txt, URL_RE))
  urls <- str_replace(urls, "[.,;)]+$", "")   # trim trailing punctuation
  urls <- unique(str_trim(urls))
  if (length(urls) == 0) return(data.table(url = character(0), is_mrf = logical(0)))
  data.table(url = urls, is_mrf = is_mrf_url(urls))
}

# ---- per-hospital driver -----------------------------------------------------

process_one <- function(homepage, id = NA) {
  base <- data.table(
    id = id, homepage = homepage,
    beacon_url = NA_character_, beacon_found = FALSE,
    n_urls = 0L, n_mrf = 0L,
    mrf_urls = NA_character_, notes = NA_character_
  )
  cands <- tryCatch(hpt_candidates(homepage), error = function(e) character(0))
  if (length(cands) == 0) { base$notes <- "no candidate beacon URL"; return(base) }

  for (cand in cands) {
    head_res <- curl_head(cand)
    if (!isTRUE(head_res$ok)) next
    txt <- curl_get(cand)
    parsed <- parse_hpt(txt)
    base$beacon_url   <- cand
    base$beacon_found <- TRUE
    base$n_urls       <- nrow(parsed)
    mrfs <- parsed[is_mrf == TRUE, url]
    base$n_mrf    <- length(mrfs)
    base$mrf_urls <- if (length(mrfs)) paste(mrfs, collapse = " | ") else NA_character_
    if (length(mrfs) == 0) base$notes <- "beacon found, no MRF-looking links"
    return(base)
  }
  base$notes <- "beacon not reachable at any candidate root"
  base
}

# Validate MRF links found across hospitals: HEAD each one, record status.
check_mrf_links <- function(results) {
  urls <- unique(unlist(str_split(na.omit(results$mrf_urls), " \\| ")))
  urls <- urls[nzchar(urls)]
  if (length(urls) == 0) return(data.table())
  rows <- lapply(urls, function(u) {
    h <- curl_head(u)
    Sys.sleep(POLITE_SLEEP)
    data.table(url = u, http_code = h$http_code, final_url = h$final_url,
               content_type = h$content_type, size = h$size, ok = h$ok)
  })
  rbindlist(rows)
}

# ---- run over a sample -------------------------------------------------------

if (sys.nframe() == 0) {   # only when run as a script, not when sourced
  urls <- fread("./data/hosp_urls.csv")

  stopifnot("homepage" %in% names(urls))
  # pick a human-readable name column if present, for output readability
  name_col <- "organization_name"

  N <- 50L   # sample size for the baseline pass; raise once hit rate looks good
  samp <- urls[!is.na(homepage) & homepage != ""][seq_len(min(N, .N))]

  message(sprintf("Processing %d homepages...", nrow(samp)))
  res_list <- vector("list", nrow(samp))
  for (i in seq_len(nrow(samp))) {
    res_list[[i]] <- process_one(samp$homepage[i], id = i)
    if (!is.na(name_col)) res_list[[i]][, name := samp[[name_col]][i]]
    Sys.sleep(POLITE_SLEEP)
  }
  results <- rbindlist(res_list, fill = TRUE)

  fwrite(results, "./data/hpt_results.csv")
  message(sprintf("Beacon found for %d / %d (%.0f%%); %d hospitals with >=1 MRF link.",
                  sum(results$beacon_found), nrow(results),
                  100 * mean(results$beacon_found),
                  sum(results$n_mrf > 0)))

  link_status <- check_mrf_links(results)
  if (nrow(link_status)) {
    fwrite(link_status, "./data/mrf_link_status.csv")
    message(sprintf("MRF links checked: %d, live (2xx/3xx): %d.",
                    nrow(link_status), sum(link_status$ok)))
  }
}
