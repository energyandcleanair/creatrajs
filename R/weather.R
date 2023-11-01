

#' A wrapper around splitr::download_met_files
#'
#' @return
#' @export
#'
#' @examples
download_weather <- function(met_type, dates, duration_hour){

  if(met_type == "gdas1"){
    remove_incomplete_gdas1()
  }

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


remove_incomplete_gdas1 <- function(){

  dir_hysplit_met <- Sys.getenv("DIR_HYSPLIT_MET", here::here(utils.get_cache_folder("weather")))

  files <- list.files(path=dir_hysplit_met, pattern='gdas1.*', full.names = T)
  infos <- file.info(files)
  infos$filename <- basename(rownames(infos))
  infos$filepath <- rownames(infos)
  infos$size_mb <- infos$size / 2^20
  rownames(infos) <- NULL

  infos <- infos %>%
    mutate(valid=dplyr::case_when(
      grepl("w1|w2|w3|w4", filename) ~ round(size_mb) == 571,
      # w5 cases
      grepl("feb", filename) ~  round(size_mb) == 82,
      grepl("apr|jun|nov|sep", filename) ~ round(size_mb) == 163,
      grepl("aug|dec|jan|jul|mar|may|oct", filename) ~ round(size_mb) == 245
    ))

  to_remove <- infos %>%
    filter(is.na(valid) | !valid) %>%
    pull(filepath)

  if(length(to_remove) > 0){
    print(glue("Removing {length(to_remove)} gdas1 weather files"))
    file.remove(to_remove)
  }
}

