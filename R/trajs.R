#' A wrapper to compute trajectories for many cities, many dates conveniently
#'
#' @param date_from
#' @param date_to
#' @param source
#' @param geometry
#' @param met_type
#' @param height
#' @param duration_hour
#' @param hours
#' @param timezone
#' @param use_cache
#' @param save_to_cache
#' @param parallel
#' @param mc.cores
#' @param debug
#'
#' @return
#' @export
#'
#' @examples
trajs.compute <- function(
  source=NULL,
  city=NULL,
  aggregate_level="city",
  date_from="2020-01-01",
  date_to=lubridate::today(),
  met_type=creatrajs::DEFAULT_MET_TYPE,
  height=creatrajs::DEFAULT_HEIGHT,
  duration_hour=creatrajs::DEFAULT_DURATION_HOUR,
  hours=creatrajs::DEFAULT_HOURS,
  timezone="UTC",
  use_cache=T, # If False, will not read cache BUT will try to write in it if upload_to_cache
  save_to_cache=use_cache,
  parallel=F,
  mc.cores=max(parallel::detectCores()-1,1),
  debug=T){

    # Either source or city should be provided
    if(is.null(source) & all(is.null(city))){
      stop("Either source or city should be provided")
    }

    # Apply default values (useful when creaengine calls this function with NULL values)
    if(is.null(met_type)) met_type <- creatrajs::DEFAULT_MET_TYPE
    if(is.null(height)) height <- creatrajs::DEFAULT_HEIGHT
    if(is.null(duration_hour)) duration_hour <- creatrajs::DEFAULT_DURATION_HOUR
    if(is.null(hours)) hours <- creatrajs::DEFAULT_HOURS
    if(is.null(date_to)) date_to <- lubridate::today()
    if(is.null(date_from)) date_from <- lubridate::today() - lubridate::days(7)
    if(is.null(use_cache)) use_cache <- T
    if(is.null(save_to_cache)) save_to_cache <- use_cache

    # Print all variables of current environment
    if(debug){
      for (obj in ls()) {
        cat(paste0(obj, ": "))
        print(get(obj))
      }
    }

    # Clean met files
    if(met_type == 'gdas1'){
      remove_incomplete_gdas1()
    }

    l <- rcrea::locations(level=aggregate_level,
                          city=city,
                          source=source,
                          with_geometry = T) %>%
      dplyr::distinct(id, geometry)

    dates <- seq(as.POSIXct(date_from, "UTC"),
                as.POSIXct(date_to, "UTC"),
                by="1 day")

    # Compute trajs
    # Looping over l rows
    trajs <- mapply(function(location_id, geometry){
      trajs.get(dates=dates,
                location_id=location_id,
                geometry=geometry,
                met_type=met_type,
                height=height,
                duration_hour=duration_hour,
                hours=hours,
                timezone=timezone,
                use_cache=use_cache,
                save_to_cache=save_to_cache,
                parallel=parallel,
                mc.cores=mc.cores,
                debug=debug)},
      l$id, l$geometry, SIMPLIFY=F, USE.NAMES = F) %>%
      unlist(recursive = F)

    do.call(rbind, trajs[!is.na(trajs)])
}



#' Calculate trajectories at a geometry and dates
#'
#' @param dates
#' @param location_id
#' @param geometry
#' @param met_type
#' @param height
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
                      height,
                      duration_hour,
                      hours=c(0,6,12,18),
                      timezone="UTC",
                      use_cache=T, # If False, will not read cache BUT will try to write in it if upload_to_cache
                      save_to_cache=use_cache,
                      parallel=F,
                      mc.cores=max(parallel::detectCores()-1,1),
                      debug=F,
                      complete_only=T,
                      ...){


  message("Computing trajs for ", location_id, " from ", min(dates), " to ", max(dates))

  # Edge case with India: HYSPLIT doesn't like non-integer hours offset
  if(all(timezone=="Asia/Kolkata")) timezone <- "Asia/Lahore"

  # Reconvert geometry to sf (e.g. mapply will convert it back to a simple point)
  geometry <- sf::st_geometry(geometry)


  trajs.get_cache <- function(location_id,
                  met_type,
                  date,
                  height,
                  duration_hour,
                  hours,
                  recompute_if_cache_na,
                  recompute_if_incomplete){

    found <- db.download_trajs(
      location_id = location_id,
      met_type = met_type,
      date = date,
      height = height,
      duration_hour,
      hours = hours
    )

    # Return early if nothing is found or if multiple rows are found.
    if (is.null(found) || nrow(found) != 1) return(NA)

    t <- found$trajs[[1]]

    # Rename 'date' column if it exists.
    if ("date" %in% names(t)) {
      t <- t %>% rename(date_recept = date)
    }

    # Conditions to determine if re-computation is necessary.
    cache_empty <- (nrow(t) <= 1 && recompute_if_cache_na)
    cache_incomplete <- (recompute_if_incomplete && !trajs.is_complete(t, duration_hours = duration_hour, hours=hours))

    if (cache_empty) {
      print(glue('Cached trajectory exists but is empty. Recomputing {date}'))
      return(NA)
    }

    if (cache_incomplete) {
      print(glue('Cached trajectory exists but is incomplete. Recomputing {date}'))
      return(NA)
    }

    return(t)
  }

  # Row by row
  trajs.get_one <- function(date,
                            location_id,
                            geometry,
                            met_type,
                            height,
                            duration_hour,
                            timezone,
                            hours,
                            use_cache,
                            save_to_cache,
                            debug,
                            recompute_if_cache_na=T,
                            recompute_if_incomplete=T){
    tryCatch({

      # Check in cache if it exists
      if(use_cache){
        t_cache <- trajs.get_cache(
          location_id=location_id,
          met_type=met_type,
          date=date,
          height=height,
          duration_hour=duration_hour,
          hours=hours,
          recompute_if_cache_na=recompute_if_cache_na,
          recompute_if_incomplete=recompute_if_incomplete
        )

        if(!all(is.na(t_cache))) return(t_cache)
        print("No cache found, recomputing")
      }


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
        message("No trajectories computed for ", location_id, " on ", date)
        return(NA)
      }

      # Save to cache
      if(save_to_cache){
        db.upload_trajs(trajs=t,
                        location_id=location_id,
                        met_type=met_type,
                        date=date,
                        duration_hour=duration_hour,
                        height=height,
                        hours=hours
                        )
      }

      return(t)
      }, error=function(c){
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

  # If parallel, we download met files first to avoid concurrency conflict
  if(parallel){
    if(use_cache){
      available <- tibble(location_id=location_id, date=dates, height=height)
      available_l <- split(available, paste(available$location_id, available$height, sep="_"))
      available <- lapply(available_l, function(l){
        available_dates_height <- db.available_dates(location_id=unique(l$location_id),
                           met_type=met_type,
                           duration_hour=duration_hour,
                           height=unique(l$height),
                           hours=hours)
        l %>% rowwise() %>% mutate(is.available=date %in% available_dates_height)
      }) %>% do.call(bind_rows, .)


      available_dates <- available %>% filter(is.available) %>% pull(date)
      missing_dates <- available %>% filter(!is.available) %>% pull(date)
      print(sprintf("Found %d available in cache. %d still missing",
                    length(available_dates),
                    length(missing_dates)))
    }else{
      missing_dates <- dates
    }

    # Weather data
    download_weather(met_type=met_type,
                     dates=missing_dates,
                     duration_hour=duration_hour)
    print("Done")
  }

  trajs <- mapply_(
    trajs.get_one,
    date=dates,
    location_id=location_id,
    geometry=geometry,
    met_type=met_type,
    height=height,
    duration_hour=duration_hour,
    timezone=timezone,
    use_cache=use_cache,
    save_to_cache=save_to_cache,
    hours=list(hours),
    debug=debug,
    SIMPLIFY=F)

  if(complete_only){
    complete <- unlist(lapply(trajs, trajs.is_complete, duration_hour=duration_hour, hours=hours))
    trajs[!complete] <- NA
  }

  return(trajs)
}


#' Indicates whether a trajectory dataframe is complete or not.
#'
#' @param trajs
#' @param duration_hours
#'
#' @return
#' @export
#'
#' @examples
trajs.is_complete <- function(trajs, duration_hours, hours){
  if(all(is.null(trajs)) || all(is.na(trajs)) || nrow(trajs) ==0) return(FALSE)
  is_complete <- all(trajs %>%
        group_by(run) %>%
        summarise(hour_along=-min(hour_along, na.rm=F)) %>%
        mutate(ok = !is.na(hour_along) & (hour_along == duration_hours) & nrow(.) == length(hours)) %>%
        pull(ok))

  # DEBUG
  if(is.na(is_complete)){
    print(trajs)
    print(trajs %>%
            group_by(run) %>%
            summarise(hour_along=-min(hour_along, na.rm=F)) %>%
            mutate(ok = !is.na(hour_along) & (hour_along == duration_hours) & nrow(.) == length(hours)))
    stop("Something is weird.")
  }

  return(is_complete)
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


trajs.cache_filename <- function(location_id, met_type, height, duration_hour, hours, date){
  paste(tolower(location_id),
        gsub("\\.","",tolower(met_type)),
        height,
        duration_hour,
        gsub("-","",date),
        paste(hours, collapse='_'),
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
  hours_utc <- hours + round(offset)

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
                                 clean_up = F)

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

