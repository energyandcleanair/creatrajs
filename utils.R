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

utils.trajs_at_date <- function(date, lat, lon){
  tryCatch({
    trajs <- splitr::hysplit_trajectory(
      lon = lon,
      lat = lat,
      height = height,
      duration = duration,
      days = date,
      daily_hours = c(0, 6, 12, 18),
      direction = "backward",
      met_type = met_type,
      extended_met = F,
      met_dir = dir_hysplit_met,
      exec_dir = here::here(dir_hysplit_output),
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


utils.attach.trajs <- function(mf){

  mft <- mf %>%
    rowwise() %>%
    mutate(
      lat=st_coordinates(geometry)[2],
      lon=st_coordinates(geometry)[1])

  trajs <- pbmcapply::pbmcmapply(utils.trajs_at_date,
                  mft$date,
                  mft$lat,
                  mft$lon,
                  SIMPLIFY = F,
                  mc.cores = parallel::detectCores()-1)

  mft$trajs=trajs
  mft$lat = NULL
  mft$lon = NULL

  return(mft)
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

  basemaps <- mc %>% rowwise() %>%
    mutate(basemap = list(geometry_to_basemap(geometry, radius_km, zoom_level)))

  return(m %>% left_join(basemaps))

}

utils.save.meta <- function(filename, date, poll, value, unit, source, fires, country, region_id, ...){
  n.fire = nrow(fires)
  d <- tibble(country, region_id, source, lubridate::date(date), poll, value, unit, n.fire, height, met_type, duration)
  filepath <- file.path(dir_results, paste0(filename,".dat"))
  write.csv(d, file=filepath, row.names=F)
  return(filepath)
}
