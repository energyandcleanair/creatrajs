

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
      # splitr only fetches gdas1 weekly files from the top-level archive directory
      # (ftp://.../archives/gdas1/<file>), which relies on NOAA maintaining a symlink
      # per week. When NOAA forgets a symlink (it happened for gdas1.may26.w4 / week of
      # 2026-05-22), the download fails and leaves a 0-byte file, breaking every
      # trajectory whose window touches that week. The real file still lives one level
      # deeper, in the year subfolder. Backfill any missing/incomplete weekly file from
      # there before falling back to the current7days mapping.
      gdas1_backfill_from_year_archive(dir_hysplit_met, dates = dates, duration_hour = duration_hour)
      gdas1_map_current7days_to_week(dir_hysplit_met)
    }
  }
}

GDAS1_FTP_DIR <- "ftp://arlftp.arlhq.noaa.gov/archives/gdas1"

#' Minimum plausible size (MB) for a complete gdas1 file, by type.
#'
#' Used to detect 0-byte / truncated downloads. Thresholds sit safely below the
#' current NOAA sizes (full week ~585 MB, was ~571 MB before 2026; w5 partials
#' ~84 / 167 / 251 MB depending on month length) so the check tolerates the
#' periodic size growth that previously caused valid files to be flagged invalid
#' and re-downloaded on every run.
#'
#' @param filename gdas1 file name, e.g. "gdas1.may26.w4"
gdas1_min_size_mb <- function(filename){
  dplyr::case_when(
    grepl("\\.w[1-4]$", filename) ~ 550,                    # full week
    grepl("feb", filename) ~ 70,                            # w5: 1 day
    grepl("apr|jun|sep|nov", filename) ~ 150,               # w5: 30-day month, 2 days (~167 MB)
    grepl("jan|mar|may|jul|aug|oct|dec", filename) ~ 230,   # w5: 31-day month, 3 days (~251 MB)
    TRUE ~ 0
  )
}

#' Weekly gdas1 file names needed to cover a set of receptor dates.
#'
#' Mirrors splitr:::get_met_gdas1's naming (gdas1.<mon><yy>.w<week>) and span
#' (backward trajectories reach `duration_hour` before the earliest date).
#'
#' @param dates receptor dates
#' @param duration_hour backward trajectory duration in hours
gdas1_expected_weekly_files <- function(dates, duration_hour){
  dates <- as.Date(dates)
  span <- seq(min(dates) - ceiling(duration_hour / 24), max(dates), by = "1 day")
  mon <- tolower(format(span, "%b"))
  yy <- format(span, "%y")
  wk <- ceiling(as.integer(format(span, "%d")) / 7)
  unique(sprintf("gdas1.%s%s.w%d", mon, yy, wk))
}

#' Backfill missing/incomplete gdas1 weekly files from the year subfolder.
#'
#' NOAA serves gdas1 weekly files from the top-level archive directory via
#' per-week symlinks into a year subfolder (e.g. .../archives/gdas1/2026/). When a
#' top-level symlink is missing, splitr's download fails and leaves a 0-byte file.
#' The underlying file is still available in the year subfolder, so we fetch it
#' directly from there. Download to a .part temp file and only move it into place
#' once it passes the size check, so a failed/partial fetch never overwrites good
#' data nor leaves another broken file behind.
#'
#' @param dir_hysplit_met directory holding the gdas1 met files
#' @param dates receptor dates (used to know which weekly files are expected)
#' @param duration_hour backward trajectory duration in hours
gdas1_backfill_from_year_archive <- function(dir_hysplit_met, dates, duration_hour){
  expected <- gdas1_expected_weekly_files(dates, duration_hour)

  for(fn in expected){
    fp <- file.path(dir_hysplit_met, fn)
    size_mb <- if(file.exists(fp)) file.info(fp)$size / 2^20 else NA_real_
    if(!is.na(size_mb) && round(size_mb) >= gdas1_min_size_mb(fn)){
      next # already present and complete
    }

    year <- 2000L + as.integer(sub("^gdas1\\.[a-z]{3}([0-9]{2})\\.w[0-9]$", "\\1", fn))
    if(is.na(year)){
      next # unexpected name, leave it to splitr
    }
    url <- sprintf("%s/%d/%s", GDAS1_FTP_DIR, year, fn)
    tmp <- paste0(fp, ".part")

    print(glue::glue("gdas1 file {fn} missing or incomplete; backfilling from year archive {url}"))
    ok <- tryCatch({
      downloader::download(url = url,
                           destfile = path.expand(tmp),
                           method = "wget",
                           extra = c("-N -c "),
                           quiet = TRUE,
                           mode = "wb",
                           cacheOK = FALSE)
      file.exists(tmp) && (file.info(tmp)$size / 2^20) >= gdas1_min_size_mb(fn)
    }, error = function(e){
      print(e)
      FALSE
    })

    if(isTRUE(ok)){
      file.rename(tmp, fp)
      print(glue::glue("Backfilled {fn} ({round(file.info(fp)$size / 2^20)} MB) from year archive"))
    }else{
      if(file.exists(tmp)) file.remove(tmp)
      print(glue::glue("Backfill failed for {fn} from {url}"))
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

  # A file is valid if it reaches the minimum plausible size for its type. Using a
  # lower bound (rather than an exact byte count) catches 0-byte/truncated downloads
  # while tolerating NOAA's periodic file-size growth, which previously made every
  # current full-week file fail an exact "== 571 MB" check and get re-downloaded each run.
  infos <- infos %>%
    mutate(valid = round(size_mb) >= gdas1_min_size_mb(filename))

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

