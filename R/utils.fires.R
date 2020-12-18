utils.fires.get_firms_folder <- function(){
  try(readRenviron(".Renviron"))
  try(readRenviron(".env"))

  d <- Sys.getenv("DIR_FIRMS")
  if(d==""){
    stop("DIR_FIRMS environment variable not defined")
  }
  return(d)
}

utils.fires.get_firms_subfolder <- function(region="South_Asia"){
  d <- utils.fires.get_firms_folder()
  return(file.path(d, "suomi-npp-viirs-c2", region))
}

utils.fires.download <- function(region="South_Asia"){
  d <- utils.fires.get_firms_folder()
  dir.create(d, showWarnings = F, recursive = T)

  modis_key <- Sys.getenv("MODIS_KEY")
  cmd <- paste0("wget -e robots=off -N -m -np -R .html,.tmp -nH --cut-dirs=5 \"https://nrt3.modaps.eosdis.nasa.gov/api/v2/content/archives/FIRMS/suomi-npp-viirs-c2/", region, "/\" --header \"Authorization: Bearer ",
                modis_key,
                "\" -P ",
                d)

  system(cmd)

}


utils.fires.read <- function(region="South_Asia"){
  d <- utils.fires.get_firms_subfolder(region=region)
  files_nrt <- list.files(d,paste0("SUOMI_VIIRS_C2_",region,"_VNP14IMGTDL_NRT_(\\d*).txt"), full.names = T) #TODO Further filtering
  files_archive <- list.files(d,"fire_.*.csv", full.names = T)

  read.csv.fire <-function(f){
    tryCatch({
      read.csv(f, stringsAsFactors = F) %>%
        mutate_at(c("satellite","version","acq_time","acq_date","daynight","confidence"),as.character) %>%
        mutate(file=f,
               date=lubridate::date(acq_date))
    }, error=function(c){
      warning(paste("Failed reading file", f))
      return(NULL)
    })
  }

  f <- do.call("bind_rows",pblapply(c(files_nrt, files_archive), read.csv.fire))
  f
}


utils.fires.attach <- function(m, f, radius_km=100){

  f.sf <- sf::st_as_sf(f, coords=c("longitude","latitude")) %>%
    mutate(date=lubridate::date(acq_date),
           geometry.fire=geometry) %>%
    filter(date>=min(m$date),
           date<=max(m$date))
  sf::st_crs(f.sf) <- 4326

  regions <- m %>% distinct(location_id, geometry)


  f.regions <- regions %>% sf::st_as_sf() %>%
    sf::st_set_crs(crs=4326) %>%
    sf::st_transform(crs=3857) %>%
    sf::st_buffer(radius_km*1000) %>%
    sf::st_transform(crs=4326) %>%
    sf::st_join(f.sf, join=st_contains) %>%
    tibble() %>%
    rename(date.fire=date) %>%
    dplyr::select(-c(geometry))

  m$date_from <- m$date - lubridate::hours(x=duration_hour)

  mf <- m %>% fuzzy_left_join(
    f.regions,
    by = c(
      "location_id" = "location_id",
      "date_from" = "date.fire",
      "date" = "date.fire"
    ),
    match_fun = list(`==`, `<=`, `>=`)
  ) %>%
    rename(location_id=location_id.x) %>%
    dplyr::select(-c(location_id.y)) %>%
    tidyr::nest(fires=-names(m))


  #
  # f.regions <- regions %>% sf::st_as_sf() %>%
  #   sf::st_transform(crs=3857) %>%
  #   sf::st_buffer(radius_km*1000) %>%
  #   sf::st_transform(crs=4326) %>%
  #   sf::st_join(f.sf, join=st_contains) %>%
  #   tibble() %>%
  #   select(-c(geometry)) %>%
  #   tidyr::nest(fires=-c(location_id, date))
  #
  # mf <- m %>%
  #   left_join(f.regions , by=c("location_id","date"))

  return(mf)
}
