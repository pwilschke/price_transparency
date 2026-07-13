# run_pipeline.R
#
# Master driver for the monthly MRF pipeline. Runs, in order:
#   1. process_urls.R          (beacon discovery: homepage -> cms-hpt.txt -> MRF links)
#   2. mrf_fetch_and_store.R   (conditional fetch + hash-dedup + blob store)
#   3. process_mrf.R           (parse blobs -> unified parquet dataset)
#
# Each step runs as its own Rscript SUBPROCESS, not via source(). Two reasons:
#   - all three scripts reuse names like log_msg / DATA_DIR / TMP_DIR; sourcing
#     them into one session would let later steps clobber earlier state.
#   - each script's `if (sys.nframe() == 0)` guard (which gates its actual
#     driver logic) only evaluates as intended when run standalone via
#     Rscript -- source()-ing them would silently skip the real work.
#
# Every step's stdout+stderr is captured to its own timestamped log file under
# data/logs/. By default the pipeline stops if a step fails; pass
# --continue-on-error to push through anyway.
#
# Usage (from anywhere -- this script finds its own folder):
#   Rscript run_pipeline.R
#   Rscript run_pipeline.R --skip-urls            # re-run fetch+process only
#   Rscript run_pipeline.R --continue-on-error
#
# Testing on a small sample before a full run:
#   HPT_SAMPLE_N=50 Rscript run_pipeline.R
# (HPT_SAMPLE_N is read by process_urls.R; leave unset for a real full run.)
# 
# NOTE: To push blob files to BackBlaze, run the following in Terminal:
# b2 sync .\data\blobs b2://price-transparency-mrfs/blobs
#
# Or to pull blob files from BackBlaze, run the following in Terminal:
# b2 sync b2://price-transparency-mrfs/blobs .\data\blobs
# ---------------------------------------------------------------------------
# Scheduling this monthly on Windows (Task Scheduler):
#   Action:        Start a program
#   Program:       full path to Rscript.exe, e.g.
#                  C:\Program Files\R\R-4.x.x\bin\Rscript.exe
#   Arguments:     full path to this file in quotes, e.g.
#                  "C:\Users\pwils\Documents\research\price_transparency\code\run_pipeline.R"
#   Start in:      leave blank -- doesn't matter, this script locates itself
#   Trigger:       Monthly, whatever day/time you like
# Test the exact Task Scheduler command first by running it directly in
# PowerShell (paste the Program path, a space, then the quoted Arguments path)
# so any errors show up in your terminal before you hand it to the scheduler.
# ---------------------------------------------------------------------------

cli_args          <- commandArgs(trailingOnly = TRUE)
SKIP_URLS         <- "--skip-urls" %in% cli_args
CONTINUE_ON_ERROR <- "--continue-on-error" %in% cli_args

# ---- resolve this script's own folder, independent of invocation cwd ------
find_script_path <- function() {
  a <- commandArgs(trailingOnly = FALSE)
  hit <- grep("--file=", a)
  if (length(hit)) return(normalizePath(sub("--file=", "", a[hit[1]])))
  if (requireNamespace("rstudioapi", quietly = TRUE) && rstudioapi::isAvailable()) {
    return(rstudioapi::getSourceEditorContext()$path)
  }
  stop("Could not determine this script's own path; run it via `Rscript run_pipeline.R`.")
}

THIS_SCRIPT  <- find_script_path()
CODE_DIR     <- dirname(THIS_SCRIPT)
PROJECT_ROOT <- normalizePath(file.path(CODE_DIR, ".."))
LOG_DIR      <- file.path(PROJECT_ROOT, "data", "logs")
if (!dir.exists(LOG_DIR)) dir.create(LOG_DIR, recursive = TRUE)

RSCRIPT_BIN <- file.path(R.home("bin"), if (.Platform$OS.type == "windows") "Rscript.exe" else "Rscript")
STAMP       <- format(Sys.time(), "%Y%m%d-%H%M%S")

log_msg <- function(...) cat(sprintf("[%s] %s\n", format(Sys.time(), "%H:%M:%S"), sprintf(...)))

run_step <- function(name, script_file) {
  script_path <- file.path(CODE_DIR, script_file)
  log_file    <- file.path(LOG_DIR, sprintf("%s_%s.log", STAMP, name))
  log_msg("Starting %s (%s) -> log: %s", name, script_file, log_file)
  t0  <- Sys.time()
  res <- system2(RSCRIPT_BIN, shQuote(script_path), stdout = log_file, stderr = log_file)
  elapsed <- round(as.numeric(difftime(Sys.time(), t0, units = "secs")), 1)
  ok <- identical(res, 0L)
  log_msg("%s %s in %ss (exit code %s)", name, if (ok) "finished" else "FAILED", elapsed, res)
  if (!ok) {
    log_msg("---- tail of %s ----", log_file)
    tryCatch(cat(tail(readLines(log_file, warn = FALSE), 30), sep = "\n"), error = function(e) NULL)
    if (!CONTINUE_ON_ERROR) stop(sprintf("%s failed (exit code %s); see %s", name, res, log_file))
  }
  ok
}

log_msg("=== Pipeline run starting: %s ===", PROJECT_ROOT)

results <- list()
if (!SKIP_URLS) {
  results$urls <- run_step("01_process_urls", "01_process_urls.R")
} else {
  log_msg("Skipping step 1 (process_urls.R) due to --skip-urls")
}
results$fetch   <- run_step("02_mrf_fetch_and_store", "02_mrf_fetch_and_store.R")
results$process <- run_step("03_process_mrf", "03_process_mrf.R")

log_msg("=== Pipeline run complete: %s ===",
        paste(sprintf("%s=%s", names(results), ifelse(unlist(results), "ok", "FAILED")), collapse = ", "))
