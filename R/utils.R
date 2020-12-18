utils.ggmap_register <- function(){
    try(readRenviron(".Renviron"))
    register_google(Sys.getenv("GOOGLE_MAP_API_KEY"))
}

utils.buffer_km <- function(g, buffer_km){
  g %>%
    st_set_crs(4326) %>%
    st_transform(crs=3857) %>%
    st_buffer(buffer_km*1000) %>%
    st_transform(crs=4326)
}



utils.trajs_at_date <- function(date, lat, lon, met_type, duration_hour, height){

  dir.create(dir_hysplit_output, showWarnings = F, recursive = T)

  tryCatch({
    print(paste("Trajs at date:",date))
    trajs <- splitr::hysplit_trajectory(
      lon = lon,
      lat = lat,
      height = height,
      duration = duration_hour,
      days = date,
      daily_hours = c(0, 6, 12, 18),
      direction = "backward",
      met_type = met_type,
      extended_met = F,
      met_dir = dir_hysplit_met,
      exec_dir = dir_hysplit_output,
      clean_up = F
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


utils.attach.trajs <- function(mf, met_type, duration_hour, height){

  mft <- mf %>%
    rowwise() %>%
    mutate(
      lat=st_coordinates(geometry)[2],
      lon=st_coordinates(geometry)[1])

  trajs <- pbapply::pbmapply(utils.trajs_at_date,
                  lubridate::date(mft$date),
                  mft$lat,
                  mft$lon,
                  met_type,
                  duration_hour,
                  height,
                  SIMPLIFY = F
                  # mc.cores=parallel::detectCores()-1
                  )


  mft$trajs=trajs
  mft$lat = NULL
  mft$lon = NULL

  return(mft)
}

utils.frp.read.modis <- function(date){
  tryCatch({
    # Using the dat format
    folder1 <- file.path(dir_modis14a1_archive, "MOD14", lubridate::year(date)) #TERRA
    folder2 <- file.path(dir_modis14a1_archive, "MYD14", lubridate::year(date)) #AQUA
    pattern <- paste0("M.D14_006_Fire_Table_",lubridate::year(date), sprintf("%03d", lubridate::yday(date)),".dat")
    f1 <- list.files(folder1, pattern, full.names = T)
    f2 <- list.files(folder2, pattern, full.names = T)

    d1 <- read.csv(f1, skip = 6)
    d2 <- read.csv(f2, skip = 6)

    sf1 <- sf::st_as_sf(d1, coords=c("FP_longitude","FP_latitude"), crs=4326)
    sf2 <- sf::st_as_sf(d2, coords=c("FP_longitude","FP_latitude"), crs=4326)

    rbind(sf1, sf2)
  },error=function(c){
    return(NA)
  })
}




utils.attach.frp <- function(mft, buffer_km){

  frp.average.along.traj <- function(date, traj, buffer_km){
    t.date <- traj %>% filter(lubridate::date(traj_dt)==lubridate::date(date))
    t.buffered <- st_as_sf(t.date, coords=c("lon","lat"), crs=4326) %>%
      utils.buffer_km(buffer_km)
    rfp <- utils.frp.read.modis(date)

    if(all(is.na(rfp))){
      return(0)
    }

    t.rfp <- t.buffered %>% sf::st_join(rfp, st_intersects)
    power <- coalesce( mean(t.rfp$FP_power, na.rm=T),0)

    return(power)
  }

  frp.calc <- function(traj, buffer_km){
    dates <- lubridate::date(traj$traj_dt) %>% unique()
    power <- do.call("mean", lapply(dates, frp.average.along.traj, traj=traj, buffer_km=buffer_km))
    return(power)
  }

  mft %>%
    rowwise() %>%
    mutate(frp=frp.calc(trajs, buffer_km))

}


utils.attach.frp.raster<- function(mft, buffer_km, duration_hour){

  mft2 <- mft %>%
    ungroup() %>%
    mutate(trajs_extent=purrr::map(trajs, utils.modis.traj_extent, buffer_km=buffer_km))

  # We're building one geotiff file per date per region
  frp.rasters <- mft2 %>%
    group_by(country, location_id) %>%
    summarise(extent=utils.modis.union_extents(trajs_extent),
              date_from=min(date)-lubridate::hours(duration_hour),
              date_to=max(date)) %>%
    mutate(geotiffs=purrr::pmap(., utils.modis.geotiffs)) %>%
    dplyr::select(country, location_id, geotiffs) %>%
    tidyr::unnest(geotiffs) %>%
    rename(fire_raster=raster)


  stack_aqua_terra <- function(x){
    s <- raster::stack(unlist(x))
    # ms <- calc(s, function(x) max(x, na.rm = TRUE))
    return(s)
  }
  # Combine TERRA and AQUA rasters
  frp.rasters <- frp.rasters %>%
    ungroup()%>%
    group_by(country, location_id, date) %>%
    summarise(fire_raster=list(stack_aqua_terra(fire_raster)))

  # Calculate mean frp along trajectories
  # wtf <- mft2 %>%
  #   mutate(frp=purrr::pmap_dbl(., utils.modis.frp_at_traj, frp.rasters=frp.rasters, buffer_km=buffer_km))
  #
  # wtfd <- wtf %>%
  #   dplyr::select(country, location_id, date, frp) %>%
  #   dplyr::group_by(country, location_id) %>%
  #   tidyr::nest(frp=c(date,frp))
  #
  # # Adding frp to weather data
  result <- mft %>%
    left_join(frp.rasters, by=c("country","location_id","date"))

  return(result)
}



utils.attach.basemaps <- function(m, radius_km=100, zoom_level=6){

  utils.ggmap_register()

  mc <- m %>% distinct(location_id, geometry)

  geometry_to_basemap <- function(g, radius_km, zoom_level){
    print(Sys.getenv("GDAL_DATA"))
    print(g)
    print(g %>%
            st_set_crs(4326))
    bbox_100km <- g %>%
      st_set_crs(4326) %>%
      st_transform(crs=3857) %>%
      st_buffer(radius_km*1000) %>%
      st_transform(crs=4326) %>%
      st_bbox()
    print(bbox)
    ggmap::get_map(location=unname(bbox_100km),zoom=zoom_level,
            source="google", terrain="terrain")
  }

  basemaps <- mc %>%
    rowwise() %>%
    mutate(basemap = list(geometry_to_basemap(geometry, radius_km, zoom_level)))

  return(m %>% left_join(basemaps))

}

utils.save.meta <- function(filename, date, poll, value, unit, source, fires, country, location_id, ..., met_type, duration_hour, height, folder=dir_results){
  d <- tibble(country, location_id, source, lubridate::date(date), poll, value, unit, height, met_type, duration_hour)
  filepath <- file.path(folder, paste0(filename,".dat"))
  write.csv(d, file=filepath, row.names=F)
  return(filepath)
}

