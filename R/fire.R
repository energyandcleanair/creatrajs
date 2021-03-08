
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
      sp::proj4string(extent.sp) <- sp::proj4string(df)

      df[!is.na(sp::over(df, extent.sp)),] %>%
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



#' Join active fire data to geometry and date provided in wt
#'
#' @param wt
#' @param one_extent_per_date Is there a different geometry for every single date
#' @param duration_hour
#'
#' @return
#' @export
#'
fire.attach <- function(wt,
                        one_extent_per_date=T,
                        duration_hour=72){

  extent.sp <- sf::as_Spatial(wt$extent[!sf::st_is_empty(wt$extent)])
  # extent.sp.union <- rgeos::gUnaryUnion(extent.sp)

  # Read and only keep fires within extent to save memory
  print("Reading fire files")
  f.sf <- fire.read(date_from=min(wt$date_fire),
                               date_to=max(wt$date_fire),
                               extent.sp=extent.sp)
  print("Done")

  if(nrow(f.sf)==0){
    print("DEBUG0")
    return(wt %>% mutate(fires=NULL))
  }

  if(one_extent_per_date){
    # Much slower, but required
    # We do one summary per row
    print("DEBUG1")
    regions <- wt %>% distinct(station_id, date_meas, date_fire, extent)



    print("Attaching fires to stations and dates")
    regions$fires <- pbapply::pbmapply(fire.summary,
                                       date=regions$date_fire,
                                       extent=regions$extent,
                                       duration_hour=duration_hour,
                                       f.sf=list(f.sf),
                                       SIMPLIFY=F)
    #mc.cores = 1) # Too memory intensive otherwise -> killed ?


    wtf <- wt %>% left_join(regions %>%
                              tidyr::unnest(fires) %>%
                              select(-c(extent))) %>%
      select(-c(extent)) %>%
      group_by(station_id, date=date_meas) %>%
      # If you want to give more weight to
      # Close fires, that'd be here probably
      summarise(fire_frp=sum(fire_frp),
                fire_count=sum(fire_count))

    print("Done")

  }else{
    print("DEBUG3")
    regions <- wt %>% distinct(station_id, extent)


    # We summarize before joining, less flexible but faster
    f.regions <- regions %>% sf::st_as_sf() %>%
      sf::st_join(f.sf, join=sf::st_contains) %>%
      tibble() %>%
      group_by(station_id, date_fire=acq_date) %>%
      summarise(frp=sum(frp, na.rm=T),
                fire_count=dplyr::n())

    # wt$date_from <- wt$date_meas - lubridate::hours(x=duration_hour)

    wtf <- wt %>% dplyr::left_join(
      f.regions %>% mutate(date_fire=lubridate::date(date_fire)),
      by = c("station_id", "date_fire")
    ) %>%
      group_by(station_id, date=date_meas) %>%
      summarise(fire_frp=sum(frp, na.rm=T),
                fire_count=sum(fire_count, na.rm=T))

  }

  return(wtf)
}
