require(plyr)
require(dplyr)
require(sf)
require(rcrea)
require(splitr)
require(pbmcapply)
require(dplyr)
# remove.packages("ggmap")
# devtools::install_github("dkahle/ggmap") # Needed for bbox and google basemaps to work
require(ggmap)
require(fuzzyjoin)
require(googleCloudStorageR)


ggmap::register_google("AIzaSyAM2hj3VbXCSjAXIjLjLH_DfPPSiV8Bhg0")



dir_hysplit_output <- here::here("hysplit_output")
dir.create(dir_hysplit_output, showWarnings = F, recursive = T)

dir_hysplit_met <- Sys.getenv("DIR_HYSPLIT_MET", here::here("hysplit_met"))
dir.create(dir_hysplit_met, showWarnings = F, recursive = T)

dir_results <- "results"
dir.create(dir_results, showWarnings = F, recursive = T)

dir_modis <- Sys.getenv("DIR_MODIS14A1")

trajs.bucket <- "crea-public"
trajs.folder <- "data/trajectories/plots"
