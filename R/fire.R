
#' Download active fire data into local folder
#'
#' @param date_from
#' @param date_to
#' @param region
#'
#' @return
#' @export
#'
#' @examples
fire.download <- function(date_from=NULL, date_to=NULL, region="Global"){
  d <- file.path(utils.get_firms_folder(),
                 "suomi-npp-viirs-c2",
                 region)
  dir.create(d, showWarnings = F, recursive = T)

  date_from <- max(lubridate::date(date_from), lubridate::date("2020-01-01"))
  # Data not available in repository before that
  # Have been downloaded manually prior 2020
  # https://firms.modaps.eosdis.nasa.gov/download/
  if(date_to >= date_from){
    files.all <- paste0("SUOMI_VIIRS_C2_",
                        region,
                        "_VNP14IMGTDL_NRT_",
                        as.POSIXct(seq(lubridate::date(date_from),
                                       lubridate::date(date_to),
                                       by="day")) %>% strftime("%Y%j"),
                        ".txt")

    # Even though wget can manage already downloaded files,
    # this is faster and less verbose in console
    files.existing <- list.files(d, "*.txt")
    file.todownload <- setdiff(files.all, files.existing)

    modis_key <- Sys.getenv("MODIS_KEY")
    for(f in file.todownload){
      cmd <- paste0("wget -e robots=off -nc -np -R .html,.tmp -nH --cut-dirs=5 \"https://nrt3.modaps.eosdis.nasa.gov/api/v2/content/archives/FIRMS/suomi-npp-viirs-c2/", region, "/", f,"\" --header \"Authorization: Bearer ",
                    modis_key,
                    "\" -P ",
                    d)
      system(cmd)
    }
  }
}



#' Title
#'
#' @param date_from
#' @param date_to
#' @param region
#' @param extent.sp
#'
#' @return
#' @export
#'
#' @examples
fire.read <- function(date_from=NULL, date_to=NULL, region="Global", extent.sp=NULL){

  d <- utils.get_firms_subfolder(region=region)

  files_nrt <- list.files(d, paste0("SUOMI_VIIRS_C2_",region,"_VNP14IMGTDL_NRT_(\\d*).txt"), full.names = T) #TODO Further filtering
  files_archive <- list.files(d, "fire_.*[0-9]{7}.txt", full.names = T)
  files <- c(files_nrt, files_archive)

  f_min_date <- function(f, date_from, date_to){
    # ifelse(
    as.POSIXct(gsub(".*([0-9]{7})\\.(txt|csv)","\\1", f), format="%Y%j")
    # Yearly files have been manually spread into smaller daily ones
    # as.POSIXct(gsub(".*([0-9]{4})\\.(txt|csv)","\\1-01-01", f), format="%Y-%m-%d")
    # )
  }

  f_max_date <- function(f, date_from, date_to){
    # ifelse(
    # stringr::str_detect(f, "[0-9]{7}"),
    as.POSIXct(gsub(".*([0-9]{7})\\.(txt|csv)","\\1", f), format="%Y%j")
    # Yearly files have been manually spread into smaller daily ones
    # as.POSIXct(gsub(".*([0-9]{4})\\.(txt|csv)","\\1-12-31", f), format="%Y-%m-%d")
    # )
  }

  files <- files[is.null(date_from) | (f_max_date(files) >= as.POSIXct(date_from))]
  files <- files[is.null(date_to) | (f_min_date(files) <= as.POSIXct(date_to))]

  if(length(files)==0){
    warning("No file found for the corresponding dates")
    return(NULL)
  }

  read.csv.fire <-function(f){
    tryCatch({
      # sp::over so much faster than sf (~5x)
      # fread vs read.csv also saves a lot of time
      d <- data.table::fread(f,
                             stringsAsFactors = F,
                             colClasses = c("acq_time"="character",
                                            "version"="character"))[,c("latitude","longitude","acq_date","frp")]
      d.coords <- cbind(d$longitude, d$latitude)
      df <- sp::SpatialPointsDataFrame(d.coords, d, proj4string=sp::CRS("+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs"))[c("acq_date","frp")]

      # Keep only in extent if indicated
      if(!is.null(extent.sp)){
        sp::proj4string(extent.sp) <- sp::proj4string(df)
        df <- df[!is.na(sp::over(df, extent.sp)),]
      }

      df %>%
        sf::st_as_sf() # To allow bind_rows (vs rbind)

    }, error=function(c){
      warning(paste("Failed reading file", f))
      message(c)
      return(NULL)
    })
  }

  fires <- do.call("bind_rows",
                   pbmcapply::pbmclapply(sort(files[!is.na(files)]), # Sort to read fire_global* first
                                         read.csv.fire,
                                         mc.cores = parallel::detectCores()-1))
  fires
}



fire.summary <- function(date, extent, duration_hour, f.sf){
  extent.sf <- sf::st_sfc(extent)
  sf::st_crs(extent.sf) <- sf::st_crs(f.sf)

  suppressMessages(f.sf %>%
                     filter(acq_date <= date, acq_date>=lubridate::date(date) - lubridate::hours(duration_hour)) %>%
                     filter(nrow(.)>0 & sf::st_intersects(., extent.sf, sparse = F)) %>%
                     as.data.frame() %>%
                     group_by() %>%
                     summarise(fire_frp=sum(frp, na.rm=T),
                               fire_count=dplyr::n()))
}



#' Join active fire data to tibble with trajs and date column. In each row of mt, trajs should be a tibble encapsulated in a list
#'
#' @param mt
#' @param duration_hour
#'
#' @return
#' @export
#'
fire.attach_to_trajs <- function(mt, buffer_km=10){

  if(!all(c("location_id", "date", "trajs") %in% names(mt))){
    stop("wt should  contain the following columns: ",paste("location_id", "date", "trajs"))
  }

  # Split by fire date (which is different than pollution date)
  mtf <- mt %>%
    tidyr::unnest(trajs, names_sep=".") %>%
    mutate(run=trajs.run) %>% # Need to keep run for buffer calculation
    # mutate(date_fire=lubridate::date(trajs.traj_dt)) %>%
    tidyr::nest(trajs=-c(location_id, date, run),
                 .names_sep=".") %>%
    rowwise() %>%
    mutate(extent=trajs.buffer(trajs=trajs, buffer_km=buffer_km),
           min_date_fire=min(trajs$traj_dt),
           max_date_fire=max(trajs$traj_dt)
           )

  # Read and only keep fires within extent to save memory
  print("Reading fire files")
  extent.sp <- sf::as_Spatial(mtf$extent[!sf::st_is_empty(mtf$extent)])

  fire.download(date_from=min(mtf$min_date_fire),
                date_to=max(max_date_fire))

  f.sf <- fire.read(date_from=min(mtf$min_date_fire),
                    date_to=max(mtf$max_date_fire),
                    extent.sp=extent.sp)
  print("Done")


  print("Attaching fire")
  mtf$fires <- pbapply::pbmapply(
    fire.attach_to_trajs_run,
    trajs_run=mtf$trajs,
    extent=mtf$extent,
    f.sf=list(f.sf),
    delay_hour=24,
    SIMPLIFY = F
  )
  print("Done")

  return(mtf)
}

#' Attach fires to a single trajectory run
#'
#' @param trajs_run
#' @param f.sf
#' @param delay_hour how "old" can a fire be to be accounted for in trajectory
#'
#' @return tibble of fires
#'
#' @examples
fire.attach_to_trajs_run <- function(trajs_run, extent, f.sf, delay_hour=24){

  if(length(unique(trajs_run$run))>1){
    stop("This function should only be called for one trajectory run")
  }

  extent.sf <- sf::st_sfc(extent)
  sf::st_crs(extent.sf) <- sf::st_crs(f.sf)

  f.sf %>%
    filter(acq_date <= max(trajs_run$traj_dt),
           acq_date >= min(trajs_run$traj_dt) - lubridate::hours(delay_hour)) %>%
    filter(nrow(.)>0 &   suppressMessages(sf::st_intersects(., extent.sf, sparse = F))) %>%
     as.data.frame() %>%
  select(acq_date, frp) %>%
  full_join(trajs_run,by = character()) %>%
  filter(as.POSIXct(acq_date, tz="UTC") <= traj_dt,
         as.POSIXct(acq_date, tz="UTC") >= traj_dt - lubridate::hours(delay_hour)) %>%
  group_by() %>%
  summarise(
    fire_frp=sum(frp, na.rm=T),
    fire_count=dplyr::n()
  )
}



#' Attach fire information to extents for every date. Much simpler than trajectories.
#'
#' @param dates
#' @param extents
#' @param delay_hour
#'
#' @return
#' @export
#'
#' @examples
fire.attach_to_extents <- function(mt,
                                  delay_hour=72){


  if(!all(c("location_id", "date", "extent") %in% names(mt))){
    stop("wt should  contain the following columns: ",paste("location_id", "date", "extent"))
  }

  extent.sp <- sf::as_Spatial(mt$extent[!sf::st_is_empty(mt$extent)])

  # Read and only keep fires within extent to save memory
  print("Reading fire files")
  f.sf <- fire.read(date_from=min(mt$date)-lubridate::hours(delay_hour),
                    date_to=max(mt$date),
                    extent.sp=extent.sp)
  print("Done")

  if(nrow(f.sf)==0){
    return(wt %>% mutate(fires=NULL))
  }

  print("Attaching fire")
  mtf$fires <- pbapply::pbmapply(
    fire.attach_to_extent,
    date=mtf$date,
    extent=mtf$extent,
    f.sf=list(f.sf),
    delay_hour=delay_hour,
    SIMPLIFY = F
  )
  print("Done")

  return(mtf)
}


#' Attach fires to a single extent
#'
#' @param trajs_run
#' @param f.sf
#' @param delay_hour how "old" can a fire be to be accounted for in trajectory
#'
#' @return tibble of fires
#'
#' @examples
fire.attach_to_extent <- function(date, extent, f.sf, delay_hour){

  extent.sf <- sf::st_sfc(extent)
  sf::st_crs(extent.sf) <- sf::st_crs(f.sf)

  f.sf %>%
    filter(acq_date <= date,
           acq_date >= date - lubridate::hours(delay_hour)) %>%
    filter(nrow(.)>0 & suppressMessages(sf::st_intersects(., extent.sf, sparse = F))) %>%
    as.data.frame() %>%
    select(acq_date, frp) %>%
    full_join(trajs_run,by = character()) %>%
    filter(as.POSIXct(acq_date, tz="UTC") <= traj_dt,
           as.POSIXct(acq_date, tz="UTC") >= traj_dt - lubridate::hours(delay_hour)) %>%
    group_by() %>%
    summarise(
      fire_frp=sum(frp, na.rm=T),
      fire_count=dplyr::n()
    )
}
