utils.fires.get_firms_folder <- function(){
  suppressWarnings(readRenviron(".Renviron"))
  suppressWarnings(readRenviron(".env"))

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

utils.fires.download <- function(date_from=NULL, date_to=NULL, region="South_Asia"){
  d <- utils.fires.get_firms_subfolder(region=region)
  dir.create(d, showWarnings = F, recursive = T)


  urls_suffixs <- if(!is.null(date_from)){
    paste0("SUOMI_VIIRS_C2_",
                          region,
                          "_VNP14IMGTDL_NRT_",
                          as.POSIXct(seq(lubridate::date(date_from),
                                         lubridate::date(date_to),
                                         by="day")) %>% strftime("%Y%j"),
                          ".txt")

  }else{""}

  modis_key <- Sys.getenv("MODIS_KEY")
  for(url_suffix in urls_suffixs){
    cmd <- paste0("wget -e robots=off -nc -np -R .html,.tmp -nH --cut-dirs=5 \"https://nrt3.modaps.eosdis.nasa.gov/api/v2/content/archives/FIRMS/suomi-npp-viirs-c2/", region, "/", url_suffix,"\" --header \"Authorization: Bearer ",
                  modis_key,
                  "\" -P ",
                  d)
    system(cmd)
  }
}


utils.fires.read <- function(date_from=NULL, date_to=NULL, region="South_Asia"){

  d <- utils.fires.get_firms_subfolder(region=region)

  files_nrt <- list.files(d,paste0("SUOMI_VIIRS_C2_",region,"_VNP14IMGTDL_NRT_(\\d*).txt"), full.names = T) #TODO Further filtering
  files_archive <- list.files(d,"fire_.*.csv", full.names = T)
  files <- c(files_nrt, files_archive)

  f_to_date <- function(f){
    as.POSIXct(gsub(".*([0-9]{7})\\.(txt|csv)","\\1", f),
               format="%Y%j")
  }


  if(!is.null(date_from)){
    files <- files[f_to_date(files) >= as.POSIXct(date_from)]
  }
  if(!is.null(date_to)){
    files <- files[f_to_date(files) <= as.POSIXct(date_to)]
  }

  read.csv.fire <-function(f){
    tryCatch({
      read.csv(f, stringsAsFactors = F) %>%
        mutate_at(c("satellite","version","acq_time","acq_date","daynight","confidence"), as.character) %>%
        mutate(file=f,
               date=lubridate::date(acq_date))
    }, error=function(c){
      warning(paste("Failed reading file", f))
      message(c)
      return(NULL)
    })
  }

  f <- do.call("bind_rows",pbapply::pblapply(files, read.csv.fire))
  f %>% dplyr::filter(!is.na(acq_date))
}


utils.fires.attach <- function(m, f, radius_km=100){

  if(nrow(f)==0){
    return(m %>% mutate(fires=NULL))
  }

  f.sf <- sf::st_as_sf(f, coords=c("longitude","latitude")) %>%
    mutate(date=lubridate::date(acq_date),
           geometry.fire=geometry) %>%
    filter(date>=min(m$date),
           date<=max(m$date))
  sf::st_crs(f.sf) <- 4326

  regions <- m %>% distinct(location_id, geometry)


  f.regions <- regions %>% sf::st_as_sf() %>%
    sf::st_set_crs(4326) %>%
    sf::st_transform(crs=3857) %>%
    sf::st_buffer(radius_km*1000) %>%
    sf::st_transform(crs=4326) %>%
    sf::st_join(f.sf, join=st_contains) %>%
    tibble() %>%
    rename(date.fire=date) %>%
    dplyr::select(-c(geometry))

  m$date_from <- m$date - lubridate::hours(x=duration_hour)

  mf <- m %>% fuzzyjoin::fuzzy_left_join(
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

  return(mf)
}
