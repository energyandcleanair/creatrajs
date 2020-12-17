

# MODIS::EarthdataLogin(usr=Sys.getenv("EARTHDATA_USR"), pwd=Sys.getenv("EARTHDATA_PWD"))

dir_results <- "results"
dir_hysplit_output <- here::here("hysplit_output")
dir_hysplit_met <- Sys.getenv("DIR_HYSPLIT_MET", here::here("hysplit_met"))
trajs.bucket <- Sys.getenv("GCS_DEFAULT_BUCKET")
trajs.folder <- "data/trajectories/plots"


init_folders <- function(base_folder){

  dir.create(dir_hysplit_output, showWarnings = F, recursive = T)
  dir.create(dir_hysplit_met, showWarnings = F, recursive = T)
  dir.create(dir_results, showWarnings = F, recursive = T)


}

