

#' A wrapper around splitr::download_met_files
#'
#' @return
#' @export
#'
#' @examples
download_weather <- function(met_type, dates, duration_hour){
  dir_hysplit_met <- Sys.getenv("DIR_HYSPLIT_MET", here::here(utils.get_cache_folder("weather")))
  if(length(dates)){
    print(paste("Downloading weather data into", dir_hysplit_met))
    dir.create(dir_hysplit_met, recursive = T, showWarnings = F)
    splitr::download_met_files(
      met_type = met_type,
      days = as.Date(dates) %>% sort(),
      duration = duration_hour,
      direction = "backward",
      met_dir = dir_hysplit_met
    )
  }
}
