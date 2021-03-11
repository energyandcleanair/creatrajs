#' Calculate trajectories at a geometry and dates
#'
#' @param dates
#' @param location_id
#' @param geometry
#' @param met_type
#' @param heights
#' @param duration_hour
#' @param ...
#'
#' @return list of tibble of trajectories (named by dates)
#' @export
#'
#' @examples
trajs.get <- function(dates,
                      location_id,
                      geometry,
                      met_type,
                      heights,
                      duration_hour,
                      cache_folder=NULL,
                      parallel=F, # NOT TOTALLY WORKING YET (weather download at least is an issue)
                      mc.cores=max(parallel::detectCores()-1,1),
                      ...){



  # Row by row
  trajs.get.one <- function(date,
                            location_id,
                            geometry,
                            met_type,
                            height,
                            duration_hour,
                            cache_folder){
    tryCatch({

      file.cache <- file.path(
        cache_folder,
        trajs.cache_filename(location_id, met_type, height, duration_hour, date))

      if(!is.null(cache_folder) &&
         file.exists(file.cache) &&
         file.info(file.cache)$size > 100){
        # Cache version exists and has data
        return(readRDS(file.cache))
      }else{
        # Compute trajs
        t <- hysplit.trajs(date=date,
                           geometry=geometry,
                           met_type=met_type,
                           duration_hour=duration_hour,
                           height=height
        )

        if(length(t)==0 || (length(t)==1 && is.na(t))){
          return(NA)
        }

        # Save to cache
        if(!is.null(cache_folder)){
          saveRDS(t,file.cache)
        }
        return(t)
      }}, error=function(c){
        print(c)
        warning(paste("Failed to calculate trajs:", c))
        return(NA)
      })
  }

  # Run all of them
  mapply_ <- if(parallel) function(...){pbmcapply::pbmcmapply(..., mc.cores=mc.cores)} else pbapply::pbmapply

  if(is.null(cache_folder)){
    cache_folder <- list(cache_folder)
  }

  trajs<- mapply_(
    trajs.get.one,
    date=dates,
    location_id=location_id,
    geometry=geometry,
    met_type=met_type,
    height=heights,
    duration_hour=duration_hour,
    cache_folder=cache_folder,
    SIMPLIFY=F)

  return(trajs)
}



#' Add a buffer
#'
#' @param trajs
#' @param buffer_km
#'
#' @return
#' @export
#'
#' @examples
trajs.buffer <- function(trajs, buffer_km){
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


trajs.cache_filename <- function(location_id, met_type, height, duration_hour, date){
  paste(tolower(location_id),
        gsub("\\.","",tolower(met_type)),
        height,
        duration_hour,
        gsub("-","",date),
        "RDS",
        sep=".")
}


hysplit.trajs <- function(date, geometry, height, duration_hour, met_type){

  dir_hysplit_met <- Sys.getenv("DIR_HYSPLIT_MET", here::here(utils.get_cache_folder("weather")))
  dir_hysplit_output <- tempdir() # Important so that several computaitons can be ran simultaneously!!
  lat <- sf::st_coordinates(geometry)[2]
  lon <- sf::st_coordinates(geometry)[1]

  tryCatch({
    trajs <-  splitr::hysplit_trajectory(
       lon = lon,
       lat = lat,
       height = height,
       duration = duration_hour,
       days = lubridate::date(date),
       daily_hours = c(0, 6, 12, 18),
       direction = "backward",
       met_type = met_type,
       extended_met = F,
       met_dir = dir_hysplit_met,
       exec_dir = dir_hysplit_output,
       clean_up = T
     )

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


#' Create a circular buffer around a geometry
#'
#' @param geometry
#' @param buffer_km
#'
#' @return
#' @export
#'
#' @examples
trajs.circular_extent <- function(geometry, buffer_km){
  tryCatch({
    sf::st_sfc(geometry, crs=4326) %>%
      sf::st_transform(crs=3857) %>%
      sf::st_buffer(buffer_km*1000) %>%
      sf::st_transform(crs=4326)
    # sf::st_bbox() %>%
    # sf::st_as_sfc()
  }, error=function(c){
    return(NA)
  })
}


#' Generate a "pie slice" of with a certain angle, coming
#' from wind direction. Using wind_speed * duration to determine
#' radius of the pie slice.
#'
#' @param geometry
#' @param buffer_km
#' @param wd The angle, measured in a clockwise direction, between true north and the direction from which
#' the wind is blowing (wd from ISD). Calm winds if wd==0
#'
#' @return
#' @export
#'
#' @examples
trajs.oriented_extent <- function(geometry, duration_hour, ws, wd, width_deg=90,
                                default_buffer_km=200,
                                max_buffer_km=800){

  st_wedge <- function(x, y, r, wd, width_deg, distance_km, n=20){
    if(wd==0){
      # Calm winds. We do a full circle, but reduce its buffer
      # so that area covered is the same
      n=n*360/width_deg
      distance_km = distance_km * sqrt(width_deg/360)
      width_deg=360
    }

    theta = seq(wd+180-width_deg/2, wd+180+width_deg/2, length=n) * pi/180
    xarc = x + distance_km*1000*sin(theta)
    yarc = y + distance_km*1000*cos(theta)
    xc = c(x, xarc, x)
    yc = c(y, yarc, y)
    sf::st_polygon(list(cbind(xc,yc)))
  }

  tryCatch({
    # ws in m per second * 10
    distance_km <- min(ws * 3600/1000 * duration_hour / 10, max_buffer_km)
    if(is.na(distance_km)){
      # Probably missing wind speed
      distance_km <- default_buffer_km
    }

    sf::st_sfc(geometry, crs=4326) %>%
      sf::st_transform(crs=3857) %>%
      st_wedge(x=sf::st_coordinates(.)[1],
               y=sf::st_coordinates(.)[2],
               wd=wd,
               # ws is in meters per second * 10)
               distance_km = distance_km,
               width_deg=width_deg
      ) %>%
      sf::st_sfc() %>%
      sf::st_set_crs(3857) %>%
      sf::st_transform(crs=4326)
  }, error=function(c){
    return(NA)
  })
}

