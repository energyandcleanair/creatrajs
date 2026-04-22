

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

  dir_hysplit_met <- path.expand(Sys.getenv("DIR_HYSPLIT_MET", here::here(utils.get_cache_folder("weather"))))
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
    if(met_type == "gdas1"){
      gdas1_map_current7days_to_week(dir_hysplit_met)
    }
  }
}

gdas1_read_header_date <- function(filepath){
  if(!file.exists(filepath) || file.info(filepath)$size <= 0){
    return(NA)
  }

  con <- file(filepath, "rb")
  on.exit(close(con), add = TRUE)
  header <- rawToChar(readBin(con, what = "raw", n = 64))
  header <- gsub("\\s+", " ", trimws(header))
  tokens <- unlist(strsplit(header, " "))

  if(length(tokens) < 3){
    return(NA)
  }

  yy <- suppressWarnings(as.integer(tokens[1]))
  mm <- suppressWarnings(as.integer(tokens[2]))
  dd <- suppressWarnings(as.integer(tokens[3]))
  if(any(is.na(c(yy, mm, dd)))){
    return(NA)
  }

  year <- if (yy < 100) 2000 + yy else yy
  parsed <- as.Date(sprintf("%04d-%02d-%02d", year, mm, dd), format = "%Y-%m-%d")
  if (is.na(parsed)) {
    return(NA)
  }
  parsed
}

gdas1_filename_from_date <- function(d){
  if(any(is.na(d))){
    return(NA_character_)
  }
  month_name <- tolower(format(d, "%b"))
  year_2digit <- format(d, "%y")
  week_number <- ceiling(as.integer(format(d, "%d")) / 7)
  paste0("gdas1.", month_name, year_2digit, ".w", week_number)
}

gdas1_map_current7days_to_week <- function(dir_hysplit_met){
  current_path <- file.path(dir_hysplit_met, "current7days")
  current_date <- gdas1_read_header_date(current_path)
  target_file <- gdas1_filename_from_date(current_date)

  if(is.na(current_date) || is.na(target_file)){
    return(invisible(NULL))
  }

  target_path <- file.path(dir_hysplit_met, target_file)
  target_ok <- file.exists(target_path) && file.info(target_path)$size > 0
  if(target_ok){
    return(invisible(NULL))
  }

  copied <- file.copy(current_path, target_path, overwrite = TRUE)
  if(copied){
    print(glue::glue("Mapped current7days to {target_file} from header date {current_date}"))
  }
}


remove_incomplete_gdas1 <- function(){

  dir_hysplit_met <- path.expand(Sys.getenv("DIR_HYSPLIT_MET", here::here(utils.get_cache_folder("weather"))))

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

  # Look for those whose modification time doesn't match
  buffer_days = 2
  delay_before_redownloading_hour <- 2
  infos <- infos %>%
    mutate(date = str_extract(filename, "[a-z]{3}[0-9]{2}"),
           date = as.Date(paste0("01",date), format="%d%b%y"),
           #extract the week number only
           weekn = gsub("\\.w", "", str_extract(filename, "\\.w[0-9]{1}"))) %>%
    mutate(
      date_expected=pmin(
        date + lubridate::days(7 * as.numeric(weekn) + buffer_days),
        lubridate::floor_date(date + lubridate::days(31), "month") + lubridate::days(buffer_days)
      )
    )

  to_remove <- unique(c(to_remove,
                 infos %>%
                   filter(lubridate::date(ctime) < lubridate::date(date_expected),
                          # Not updated in the past hour
                          ctime < Sys.time() - lubridate::hours(delay_before_redownloading_hour),
                          !grepl("current7days", filepath)) %>%
                   pull(filepath)))

  if(length(to_remove) > 0){
    print(glue::glue("Removing {length(to_remove)} gdas1 weather files"))
    file.remove(to_remove)
  }
}

