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
                      hours=c(0,6,12,18),
                      timezone="UTC",
                      use_cache=T, # If False, will not read cache BUT will try to write in it
                      cache_folder=NULL,
                      parallel=F,
                      mc.cores=max(parallel::detectCores()-1,1),
                      debug=T,
                      ...){


  # Edge case with India: HYSPLIT doesn't like non-integer hours offset
  if(timezone=="Asia/Kolkata") timezone <- "Asia/Lahore"

  # Row by row
  trajs.get.one <- function(date,
                            location_id,
                            geometry,
                            met_type,
                            height,
                            duration_hour,
                            timezone,
                            hours,
                            cache_folder,
                            debug){
    tryCatch({

      file.cache <- file.path(
        cache_folder,
        trajs.cache_filename(location_id, met_type, height, duration_hour, date))

      if(!is.null(cache_folder) &&
         use_cache &&
         file.exists(file.cache) &&
         file.info(file.cache)$size > 600){
        # Cache version exists and has data
        t <- readRDS(file.cache)

        #backward compatibility:
        if("date" %in% names(t)){
          t <- t %>% rename(date_recept=date)
        }
        return(t)
      }else{
        # Compute trajs
        t <- hysplit.trajs(date=date,
                           geometry=geometry,
                           met_type=met_type,
                           duration_hour=duration_hour,
                           height=height,
                           timezone=timezone,
                           hours=hours
        )

        if(length(t)==0 || (length(t)==1 && is.na(t))){
          return(NA)
        }

        # Save to cache
        if(!is.null(cache_folder)){
          saveRDS(t,file.cache)
        }

        if(debug){
          message("Memory used: ", round(pryr::mem_used()/1E6),"MB")
        }

        return(t)
      }}, error=function(c){
        print(c)
        warning(paste("Failed to calculate trajs:", c))
        return(NA)
      })
  }

  # Run all of them
  mapply_ <- if(parallel){
    function(...){
      #pbmcmapply is annoying...
      #Result structure varies whether there's been a warning or not
      result <- pbmcapply::pbmcmapply(..., mc.cores=mc.cores)
      if("value" %in% names(result)) result$value else result
      }
    }else{
      pbapply::pbmapply
    }

  if(is.null(cache_folder)){
    cache_folder <- list(cache_folder)
  }

  # If parallel, we download met files first to avoid concurrency conflict
  if(parallel){
    # But we only download those for trajectories we need to compute
    if(!is.null(cache_folder)){
      files.cache <- file.path(cache_folder,
        trajs.cache_filename(location_id, met_type, heights, duration_hour, dates))

      missing_dates <- dates[!file.exists(files.cache)]
    }else{
      missing_dates <- dates
    }


    dir_hysplit_met <- Sys.getenv("DIR_HYSPLIT_MET", here::here(utils.get_cache_folder("weather")))
    print(paste("Downloading weather data into", dir_hysplit_met))
    dir.create(dir_hysplit_met, recursive = T, showWarnings = T)
    if(length(missing_dates)){
      splitr::download_met_files(
        met_type = met_type,
        days = as.Date(missing_dates),
        duration = duration_hour,
        direction = "backward",
        met_dir = dir_hysplit_met
      )
    }
    print("Done")
  }

  trajs<- mapply_(
    trajs.get.one,
    date=dates,
    location_id=location_id,
    geometry=geometry,
    met_type=met_type,
    height=heights,
    duration_hour=duration_hour,
    timezone=timezone,
    cache_folder=cache_folder,
    hours=list(hours),
    debug=debug,
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
trajs.buffer <- function(trajs, buffer_km, merge=T, group_cols=c("location_id","date","run")){
  tryCatch({


    if(!all(group_cols %in% names(trajs))){
      group_cols <- intersect(names(trajs), group_cols)
      message("Reducing trajs grouping columns to ", group_cols)
    }


    t <- sf::st_as_sf(trajs[!is.na(trajs$lat),],
                 coords=c("lon","lat"), crs=4326) %>%
      group_by_at(group_cols) %>%
      mutate(count=dplyr::n())

    do_buffer <- function(t){
      suppressMessages(t %>%
                         sf::st_transform(crs=3857) %>%
                         sf::st_buffer(buffer_km*1000) %>%
                         # sf::st_bbox() %>%
                         # sf::st_as_sfc() %>%
                         sf::st_transform(crs=4326))
    }

    t.lines <- t %>%
      filter(count>1) %>% #LINESTRING WITH ONLY ONE POINT CAN'T BE BUFFERED
      group_by_at(group_cols) %>%
      arrange(traj_dt) %>%
      summarise(do_union = FALSE) %>%
      sf::st_cast("LINESTRING")

    t.points <- t %>%
      filter(count==1) %>%
      select_at(c(group_cols,"geometry"))

    b <- rbind(
      do_buffer(t.lines),
      do_buffer(t.points))

    if(merge){
      b <- suppressMessages(sf::st_union(b))
    }

    return(b)
  }, error=function(c){
    warning("Failed to buffer trajs: ",c)
    return(NA)
  })
}



#' Add a buffer extent to each single trajectory run
#'
#' @param trajs
#' @param buffer_km
#'
#' @return
#' @export
#'
#' @examples
trajs.split_by_run_and_buffer <- function(mt, buffer_km){


  # Test
  nest_cols <- names(mt) %>%
    setdiff("trajs")

  trajs <- mt %>%
    tidyr::unnest(trajs) %>%
    mutate(date_fire=lubridate::date(date_particle))

  extents <- trajs.buffer(trajs, buffer_km, group_cols = c("location_id","date","date_fire","run"), merge=F) %>%
    as.data.frame() %>%
    rename(extent=geometry)

  # tic()
  # trajs.run <- trajs %>%
  #   mutate(run2=run) %>%
  #   group_by_at(c(nest_cols,"run2")) %>%
  #   tidyr::nest() %>%
  #   rename(trajs=data,
  #          run=run2)
  # toc()
  #
  # tic()
  # left_join(trajs.run, extents)
  # toc()
  return(extents)
}

#' Add a buffer extent to each single trajectory run
#'
#' @param trajs
#' @param buffer_km
#'
#' @return
#' @export
#'
#' @examples
trajs.split_by_firedate_and_buffer <- function(mt, buffer_km, return_full=F){

  nest_cols <- names(mt) %>%
    setdiff("trajs")

  print("--unnesting")
  trajs <- mt %>%
    tidyr::unnest(trajs) %>%
    mutate(date_particle=lubridate::date(date_particle))

  print("--buffering")
  extents <- trajs.buffer(trajs, buffer_km, merge=F, group_cols=c("location_id", "date", "date_particle", "run")) %>%
    as.data.frame() %>%
    rename(extent=geometry)

  if(return_full){
    # With trajectories but much slower
    print("--nesting")
    trajs %>%
      mutate(run2=run) %>%
      group_by_at(c(nest_cols,"run2")) %>%
      tidyr::nest() %>%
      rename(trajs=data,
             run=run2) %>%
      left_join(trajs.run, extents)
  }else{
    # We keep extent only, much faster
    print("--distinguishing...")
    trajs %>%
      dplyr::distinct_at(c(nest_cols,"run")) %>%
      left_join(extents)
  }
}

trajs.buffer_pts <- function(trajs, buffer_km, res_deg){

  tryCatch({
    t.sf <- trajs.buffer(trajs, buffer_km, merge=F)
    runs <- unique(t.sf$run)

    lapply(runs, function(run){
      t=t.sf[t.sf$run==run,]
      suppressMessages(t %>%
        sf::st_make_grid(cellsize = res_deg/2, what = "centers") %>% # We want to be sure we'll be in every cell
        sf::st_intersection(t) %>%
        as.data.frame() %>%
        mutate(run=!!run))
    }) %>%
      do.call(bind_rows,.) %>%
      sf::st_as_sf()
  }, error=function(c){
    warning("Failed to buffer trajs pts: ",c)
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


hysplit.trajs <- function(date, geometry, height, duration_hour, met_type, timezone="UTC", hours=c(0, 6, 12, 18)){

  dir_hysplit_met <- Sys.getenv("DIR_HYSPLIT_MET", here::here(utils.get_cache_folder("weather")))
  dir_hysplit_output <- file.path(tempdir(), substr(uuid::UUIDgenerate(),1,6)) # Important so that several computations can be ran simultaneously!!
  dir.create(dir_hysplit_output, showWarnings = F, recursive = T)
  lat <- sf::st_coordinates(geometry)[2]
  lon <- sf::st_coordinates(geometry)[1]

  # Build date/hour combinations in UTC
  offset <- as.double(as.POSIXct(date,tz=timezone)-as.POSIXct(date,tz="UTC"), units="hours")
  hours_utc <- hours + offset

  tryCatch({
    trajs <- splitr::hysplit_trajectory(
                                 lon = lon,
                                 lat = lat,
                                 height = height,
                                 duration = duration_hour,
                                 days = as.Date(date),
                                 daily_hours = hours_utc, #It accepts negative hours
                                 direction = "backward",
                                 met_type = met_type,
                                 extended_met = F,
                                 met_dir = dir_hysplit_met,
                                 exec_dir = dir_hysplit_output,
                                 clean_up = T)

    # Update fields to be compatible with OpenAIR
    trajs$hour.inc <- trajs$hour_along
    trajs$date_recept <- trajs$traj_dt_i
    trajs$date_particle <- trajs$traj_dt
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


#' Convert a trajectories tibble to a raster stack (one layer per day)
#'
#' @param t
#' @param res_deg
#'
#' @return
#' @export
#'
#' @examples
trajs.to_rasterstack <- function(t, buffer_km, res_deg){


  # Create raster
  t.sf <- trajs.buffer(t, buffer_km)
  t.sp <- as(t.sf, "Spatial")
  r <- raster::raster(t.sp,resolution=res_deg)
  raster::crs(r) <- 4326
  t$id <- seq(1, nrow(t))

  # Rasterize for every single dat
  ts <- split(t, lubridate::date(t$traj_dt))
  lapply(ts,
         function(t){
           # Cannot buffer: polygons only affect a cell if it contains cell center
           # We create a grid for every polygon
           t.sf.pts <- trajs.buffer_pts(t, buffer_km=buffer_km, res_deg=res_deg)
           raster::rasterize(as(t.sf.pts,"Spatial"),
                             r,
                             field='run',
                             fun=function(x, ...) {length(unique(na.omit(x)))}
                             )

         }) %>% raster::stack()
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

