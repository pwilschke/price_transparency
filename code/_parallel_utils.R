# _parallel_utils.R
#
# Shared parallelism helpers for the MRF pipeline (sourced by 01, 02, 03).
#
# Design constraints:
#   - Target machine is a WINDOWS laptop with an UNKNOWN core count. Windows has
#     no fork(), so parallel::mclapply / future multicore silently run SERIAL
#     there. We therefore use PSOCK clusters (parallel::makeCluster), which work
#     identically on Windows/macOS/Linux -- each worker is a fresh R process.
#   - Worker count defaults to (cores - 1) so the laptop stays usable, and is
#     overridable with the PIPELINE_WORKERS env var (set it to 1 to force a
#     fully serial run, e.g. for debugging or timing comparisons).
#   - Only base R's `parallel` is required (ships with R); no extra packages.
#
# Public helpers:
#   detect_workers()                 -> integer number of workers to use
#   with_cluster(n, worker_init, FUN)-> run FUN(cl) with a PSOCK cluster, always
#                                       stopping it afterward (even on error)
#   par_lapply(cl, X, FUN, ...)      -> load-balanced parallel lapply, or plain
#                                       lapply when cl is NULL (serial)
#   host_of(url)                     -> lowercase scheme+host (for per-host work
#                                       partitioning); NA on unparseable input
#   partition_by_host(items, urls)   -> list of per-host item groups, so a host
#                                       is only ever touched by one worker

# Number of workers: min(user override | cores-1), never < 1.
detect_workers <- function() {
  ov <- suppressWarnings(as.integer(Sys.getenv("PIPELINE_WORKERS", unset = NA)))
  if (!is.na(ov) && ov >= 1L)
    return(ov)
  cores <- tryCatch(parallel::detectCores(), error = function(e) NA_integer_)
  if (is.na(cores) || cores < 1L)
    cores <- 1L
  max(1L, cores - 1L)
}

# Run FUN(cl) with a PSOCK cluster of `n` workers, guaranteeing the cluster is
# stopped afterward. If n <= 1, no cluster is created and FUN(NULL) runs in the
# main process (serial). `worker_init` is an optional function run once on each
# worker (e.g. to setwd() + source() the parsing functions); it receives no
# arguments and is executed via clusterCall.
with_cluster <- function(n, FUN, worker_init = NULL) {
  if (n <= 1L)
    return(FUN(NULL))
  cl <- parallel::makeCluster(n)           # PSOCK: cross-platform, no fork
  on.exit(parallel::stopCluster(cl), add = TRUE)
  if (!is.null(worker_init))
    parallel::clusterCall(cl, worker_init)
  FUN(cl)
}

# Load-balanced parallel lapply. cl = NULL -> ordinary serial lapply, so callers
# can use one code path regardless of worker count.
par_lapply <- function(cl, X, FUN, ...) {
  if (is.null(cl))
    return(lapply(X, FUN, ...))
  parallel::parLapplyLB(cl, X, FUN, ...)
}

# scheme://host in lowercase, path/query/fragment stripped. Mirrors root_of()
# in 01 but tolerant of a missing scheme (defaults to https reasoning: we only
# use the host part for grouping). Returns NA_character_ if nothing host-like.
host_of <- function(url) {
  u <- trimws(as.character(url))
  # add scheme if absent so the regex below finds a host
  needs <- !is.na(u) & nzchar(u) & !grepl("^[a-zA-Z][a-zA-Z0-9+.-]*://", u)
  u[needs] <- paste0("https://", u[needs])
  m <- regmatches(u, regexec("^([a-zA-Z][a-zA-Z0-9+.-]*)://([^/?#]+)", u))
  vapply(m, function(x) if (length(x) >= 3) tolower(x[3]) else NA_character_, character(1))
}

# Group `items` (any vector/list) by the host of the parallel `urls` vector, so
# each group can be handed to a single worker and a host is never hit by more
# than one worker at a time. Items whose URL has no parseable host are grouped
# under their own synthetic keys (each alone) so they still get processed.
# Returns an unnamed list of item-vectors (one per host).
partition_by_host <- function(items, urls) {
  stopifnot(length(items) == length(urls))
  h <- host_of(urls)
  # give unparseable hosts a unique key each so they don't all collapse together
  bad <- is.na(h) | !nzchar(h)
  h[bad] <- paste0("__nohost__", seq_len(sum(bad)))
  unname(split(items, h))
}

# Apply `fn` to each element of `items`, parallelizing ACROSS hosts while keeping
# each host serial + politely spaced. `urls` (same length as items) supplies the
# host each item belongs to. Items sharing a host are handed to a single worker,
# which processes them in order with `sleep` seconds between requests, so a host
# never sees more than one in-flight request. Distinct hosts run on separate
# workers concurrently.
#
# `fn` is applied by VALUE (fn(item)); write it to call your per-item worker by
# name (e.g. function(hp) process_one(hp)) so it resolves in the worker's own
# global env (populated by worker_init), rather than shipping a closure bound to
# the main process. Results are returned in the ORIGINAL order of `items`.
par_by_host <- function(cl, items, urls, fn, sleep = 0) {
  n <- length(items)
  if (n == 0L) return(list())
  h <- host_of(urls)
  bad <- is.na(h) | !nzchar(h)
  h[bad] <- paste0("__nohost__", seq_len(sum(bad)))
  idx_groups <- unname(split(seq_len(n), h))

  # each group: process its items in order, sleeping between (not after last)
  run_group <- function(idxs) {
    out <- vector("list", length(idxs))
    for (j in seq_along(idxs)) {
      out[[j]] <- fn(items[[idxs[j]]])
      if (sleep > 0 && j < length(idxs)) Sys.sleep(sleep)
    }
    list(idxs = idxs, out = out)
  }

  grp_results <- par_lapply(cl, idx_groups, run_group)

  res <- vector("list", n)
  for (g in grp_results)
    for (j in seq_along(g$idxs)) res[[g$idxs[j]]] <- g$out[[j]]
  res
}
