# Pulls a full list of hospitals from CMS

library(data.table)
library(collapse)
library(stringr)

# set working directory to the project root
# Check if running in RStudio
if (requireNamespace("rstudioapi", quietly = TRUE) && rstudioapi::isAvailable()) {
  setwd(dirname(rstudioapi::getSourceEditorContext()$path))
  setwd("../")
} else {
  # If not in RStudio, assume we're already in the right directory or it's been set
  if (!file.exists("code/pull_meps_fyc.R")) {
    stop("Working directory is not set correctly. Please setwd() to the project root before sourcing.")
  }
}

cms_hosps <- "https://data.cms.gov/provider-data/sites/default/files/resources/893c372430d9d71a1c52737d01239d47_1777413958/Hospital_General_Information.csv"
dest <- "./data/cms_hospitals.csv"

download.file(
  url = cms_hosps,
  destfile = dest,
  method = "wget"
)

hosps <- fread("./data/cms_hospitals.csv")


hosps_url <- "https://www.dolthub.com/csv/dolthub/standard-charge-files/main/hospitals?include_bom=0"

download.file(
  url = hosps_url,
  destfile = "./data/hosp_urls.csv",
  method = "wget"
)

urls <- fread("./data/hosp_urls.csv")
