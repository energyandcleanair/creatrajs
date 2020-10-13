utils.buffer_km <- function(g, buffer_km){
  g %>%
    st_transform(crs=3857) %>%
    st_buffer(buffer_km*1000) %>%
    st_transform(crs=4326)
}

utils.read.fires <- function(){
  files <- list.files("data","fire_.*.csv", full.names = T)

  read.csv.fire <-function(f){
    read.csv(f, stringsAsFactors = F) %>%
      mutate_at(c("satellite","version"),as.character)
  }

  f <- do.call("bind_rows",lapply(files, read.csv.fire))
  f
}



utils.attach.fires <- function(m, f, radius_km=100){

  f.sf <- sf::st_as_sf(f, coords=c("longitude","latitude")) %>%
    mutate(date=lubridate::date(acq_date),
           geometry.fire=geometry) %>%
    filter(date>=min(m$date),
           date<=max(m$date))
  sf::st_crs(f.sf) <- 4326

  regions <- m %>% distinct(region_id, geometry)


  f.regions <- regions %>% sf::st_as_sf() %>%
    sf::st_transform(crs=3857) %>%
    sf::st_buffer(radius_km*1000) %>%
    sf::st_transform(crs=4326) %>%
    sf::st_join(f.sf, join=st_contains) %>%
    tibble() %>%
    rename(date.fire=date) %>%
    select(-c(geometry))

  m$date_from <- m$date - lubridate::hours(x=duration_hour)

  mf <- m %>% fuzzy_left_join(
    f.regions,
    by = c(
      "region_id" = "region_id",
      "date_from" = "date.fire",
      "date" = "date.fire"
    ),
    match_fun = list(`==`, `<=`, `>=`)
  ) %>%
    rename(region_id=region_id.x) %>%
    select(-c(region_id.y)) %>%
    tidyr::nest(fires=-names(m))


  #
  # f.regions <- regions %>% sf::st_as_sf() %>%
  #   sf::st_transform(crs=3857) %>%
  #   sf::st_buffer(radius_km*1000) %>%
  #   sf::st_transform(crs=4326) %>%
  #   sf::st_join(f.sf, join=st_contains) %>%
  #   tibble() %>%
  #   select(-c(geometry)) %>%
  #   tidyr::nest(fires=-c(region_id, date))
  #
  # mf <- m %>%
  #   left_join(f.regions , by=c("region_id","date"))

  return(mf)
}

utils.trajs_at_date <- function(date, lat, lon, met_type, duration_hour, height){
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

  trajs <- pbmcmapply(utils.trajs_at_date,
                  lubridate::date(mft$date),
                  mft$lat,
                  mft$lon,
                  met_type,
                  duration_hour,
                  height,
                  SIMPLIFY = F,
                  mc.cores=detectCores()-1)

  mft$trajs=trajs
  mft$lat = NULL
  mft$lon = NULL

  return(mft)
}

utils.frp.read.modis <- function(date){
  tryCatch({
    # Using the dat format
    folder1 <- file.path(dir_modis, "MOD14", lubridate::year(date)) #TERRA
    folder2 <- file.path(dir_modis, "MYD14", lubridate::year(date)) #AQUA
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


utils.attach.frpv2<- function(mft, f2, buffer_km){

  frp.average.along.traj <- function(date, traj, f2, buffer_km){

    t.date <- traj %>% filter(lubridate::date(traj_dt)==lubridate::date(date))
    t.buffered <- st_as_sf(t.date, coords=c("lon","lat"), crs=4326) %>%
      utils.buffer_km(buffer_km)
    rfp <- f2 %>% filter(date==!!date)

    if(nrow(rfp)==0){
      return(0)
    }

    t.rfp <- t.buffered %>% sf::st_join(sf::st_as_sf(rfp, coords=c("longitude","latitude"),crs=4326),
                                        st_intersects)
    power <- coalesce( mean(t.rfp$frp, na.rm=T),0)

    return(power)
  }

  frp.calcv2 <- function(traj, f2, buffer_km){
    dates <- lubridate::date(traj$traj_dt) %>% unique()
    power <- do.call("mean", lapply(dates, frp.average.along.traj, traj=traj, f2=f2, buffer_km=buffer_km))
    return(power)
  }

  f2 <- f2 %>% mutate(date=lubridate::date(acq_date)) %>% sf::st_as_sf(coords=c("longitude","latitude"),crs=4326)

  mft %>%
    rowwise() %>%
    mutate(frp=frp.calcv2(trajs, f2, buffer_km))

}



utils.attach.basemaps <- function(m, radius_km=100, zoom_level=6){

  mc <- m %>% distinct(region_id, geometry)

  geometry_to_basemap <- function(g, radius_km, zoom_level){

    bbox_100km <- g %>%
      st_transform(crs=3857) %>%
      st_buffer(radius_km*1000) %>%
      st_transform(crs=4326) %>%
      st_bbox()

    get_map(location=unname(bbox_100km),zoom=zoom_level,
            source="google", terrain="terrain")
  }

  basemaps <- mc %>%
    rowwise() %>%
    mutate(basemap = list(geometry_to_basemap(geometry, radius_km, zoom_level)))

  return(m %>% left_join(basemaps))

}

utils.save.meta <- function(filename, date, poll, value, unit, source, fires, country, region_id, ..., met_type, duration_hour, height){
  d <- tibble(country, region_id, source, lubridate::date(date), poll, value, unit, height, met_type, duration_hour)
  filepath <- file.path(dir_results, paste0(filename,".dat"))
  write.csv(d, file=filepath, row.names=F)
  return(filepath)
}
