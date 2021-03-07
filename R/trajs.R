#' Calculate trajectories at a geometry and dates
#'
#' @param dates
#' @param location_id
#' @param geometry
#' @param country
#' @param met_type
#' @param heights
#' @param duration_hour
#' @param ...
#'
#' @return tibble of trajectories
#' @export
#'
#' @examples
trajs.get <- function(dates,
                      location_id,
                      geometry,
                      country,
                      met_type,
                      heights,
                      duration_hour,
                      cache_folder=NULL,
                      ...){

  tryCatch({
    print(paste0("Calculating trajs for ", location_id))

    if(!is.null(cache_folder)){
      filenames <- trajs.cache_filename(location_id, country, met_type, heights, duration_hour, dates)
      filepaths <- file.path(cache_folder, filenames)

      filepaths.existing <- filepaths[file.exists(filepaths)]
      filepaths.missing <- filepaths[!file.exists(filepaths)]
      dates.missing <- dates[!file.exists(filepaths)]
      heights.missing <- heights[!file.exists(filepaths)]
    }else{
      filepaths.existing <- c()
      dates.missing <- dates
      heights.missing <- heights
    }

    trajs <- do.call("bind_rows",
                     lapply(filepaths.existing, readRDS))

    if(length(dates.missing)>0){
      trajs.missing <- hysplit.trajs(dates.missing,
                                     geometry=geometry,
                                     met_type=met_type,
                                     duration_hour=duration_hour,
                                     heights=heights.missing)

      # Save to cache
      if(!is.null(cache_folder)){
        trajs.missing.days <- split(trajs.missing, lubridate::date(trajs.missing$traj_dt_i))
        lapply(names(trajs.missing.days),
               function(date){
                 t <- trajs.missing.days[[date]]
                 height <- heights.missing[dates.missing==date]
                 f <- file.path(cache_folder,
                                trajs.cache_filename(location_id,
                                                     country,
                                                     met_type,
                                                     height,
                                                     duration_hour, date))
                 saveRDS(t,f)
               })
      }

      # Combine all
      trajs <- bind_rows(trajs,
                         trajs.missing)
    }

    return(trajs)
  }, error=function(c){
    return(NA)
  })
}



trajs.extent <- function(trajs, buffer_km){
  tryCatch({
    suppressMessages(sf::st_as_sf(trajs, coords=c("lon","lat"), crs=4326) %>%
                       group_by(run) %>%
                       summarise() %>%
                       sf::st_cast("LINESTRING") %>%
                       sf::st_transform(crs=3857) %>%
                       sf::st_buffer(buffer_km*1000) %>%
                       # sf::st_bbox() %>%
                       # sf::st_as_sfc() %>%
                       sf::st_transform(crs=4326) %>%
                       sf::st_union())

  }, error=function(c){
    return(NA)
  })
}


trajs.cache_filename <- function(location_id, country, met_type, height, duration_hour, date){
  paste(tolower(country),
        tolower(location_id),
        gsub("\\.","",tolower(met_type)),
        height,
        duration_hour,
        gsub("-","",date),
        "RDS",
        sep=".")
}


hysplit.trajs <- function(dates, geometry, heights, duration_hour, met_type){

  dir_hysplit_met <- Sys.getenv("DIR_HYSPLIT_MET", here::here(utils.get_cache_folder("weather")))
  dir_hysplit_output <- utils.get_cache_folder("trajs/output")
  lat <- sf::st_coordinates(geometry)[2]
  lon <- sf::st_coordinates(geometry)[1]

  tryCatch({
    # Probably slower to do date by date,
    # But facing unknown issue when too many dates at a time
    # Probably if one fails, all fail
    trajs <- do.call("bind_rows",
                     pbapply::pblapply(seq_along(dates),
                                       function(i){
                                         splitr::hysplit_trajectory(
                                           lon = lon,
                                           lat = lat,
                                           height = heights[[i]],
                                           duration = duration_hour,
                                           days = lubridate::date(dates[[i]]),
                                           daily_hours = c(0, 6, 12, 18),
                                           direction = "backward",
                                           met_type = met_type,
                                           extended_met = F,
                                           met_dir = dir_hysplit_met,
                                           exec_dir = dir_hysplit_output,
                                           clean_up = T
                                         )}))

    # Update fields to be compatible with OpenAIR
    trajs$hour.inc <- trajs$hour_along
    trajs$date <- trajs$traj_dt_i
    trajs$date2 <- trajs$traj_dt
    trajs$year <- lubridate::year(trajs$traj_dt_i)
    trajs$month <- lubridate::month(trajs$traj_dt_i)
    trajs$day <- lubridate::date(trajs$traj_dt_i)

    return(trajs)
  },
  error=function(c){
    print(c)
    return(NA)
  })
}
