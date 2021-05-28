gfas.filename_to_date <- function(f){
    as.Date(gsub(".*([0-9]{8})\\.nc","\\1", f), format="%Y%m%d", tz="UTC")
}

gfas.available_filenames <- function(){
  d <- utils.get_gfas_folder()
  list.files(d, "gfas_([0-9]{8})*.nc", full.names = T)
}


#' Download GFAS data into local folder
#'
#' @param date_from
#' @param date_to
#' @param region
#'
#' @return
#' @export
#'
#' @examples
gfas.download <- function(date_from=NULL, date_to=NULL, region="Global"){

  d <- utils.get_gfas_folder()
  dir.create(d, showWarnings = F, recursive = T)


  required_dates <- seq(as.Date(date_from, tz="UTC"), as.Date(date_to, tz="UTC"), by="days")
  available_dates <- gfas.filename_to_date(gfas.available_filenames())
  download_dates <- as.Date(setdiff(required_dates, available_dates), origin="1970-01-01")


  # One request per contigous set
  sets.tbl <- tibble(date=download_dates) %>%
    arrange() %>%
    # rowwise() %>%
    mutate(jump=as.numeric(date-dplyr::lag(date),units="days")>1) %>%
    mutate(set=cumsum(tidyr::replace_na(jump,T)))

  sets <- split(sets.tbl$date, sets.tbl$set)

  ecmwf_user = Sys.getenv("ECMWF_API_EMAIL")
  ecmwf_key = Sys.getenv("ECMWF_API_KEY")

  if(ecmwf_user=="" | ecmwf_key==""){
    warning("Missing ECMWF_API_EMAIL or ECMWF_API_KEY environmental variable")
    return(NULL)
  }

  options(keyring_backend = "env") # TO AVOID ASKING USER A KEYRING PASSWORD
  ecmwfr::wf_set_key(user = ecmwf_user, key = ecmwf_key, service = "webapi")
  ecmwfr::wf_datasets(ecmwf_user)

  lapply(sets, function(set){
    date_from <- min(set)
    date_to <- max(set)

    message("Downloading from ",date_from," to ",date_to)
    filename <- sprintf("%s_to_%s.nc",
                        strftime(as.POSIXct(date_from),"%Y%m%d"),
                        strftime(as.POSIXct(date_to),"%Y%m%d"))
    request <- list(class = "mc",
                    dataset = "cams_gfas",
                    date = sprintf("%s/to/%s", date_from, date_to),
                    expver = "0001",
                    levtype = "sfc",
                    param = "87.210",
                    step = "0-24",
                    stream = "gfas",
                    time = "00:00:00",
                    type = "ga",
                    target = filename,
                    format = "netcdf")

    ecmwfr::wf_request(
      request,
      ecmwf_user,
      transfer = TRUE,
      path = d,
      time_out = 3600*5,
      # job_name = "gfas_download",
      verbose = TRUE
    )

    # Unpack file
    r <- raster::raster(file.path(d, filename))
    n <- raster::nbands(r)

    for(i in seq(n)){
      r.band <- raster::raster(file.path(d, filename), band=i)
      date <- lubridate::date(raster::getZ(r.band))
      f.band <- file.path(d, sprintf("gfas_%s.nc",strftime(date,"%Y%m%d")))
      if(!file.exists(f.band)){
        raster::writeRaster(r.band, f.band)
      }
    }

    file.remove(filename)
  })

}


#' Title
#'
#' @param date_from
#' @param date_to
#' @param region
#' @param extent.sp
#'
#' @return RasterStack with time variable
#' @export
#'
#' @examples
gfas.read <- function(date_from=NULL, date_to=NULL, extent.sp=NULL, show.progress=T){

  d <- utils.get_gfas_folder()

  fs <- gfas.available_filenames()
  f_date <- gfas.filename_to_date

  fs <- fs[is.null(date_from) | (f_date(fs) >= as.Date(date_from))]
  fs <- fs[is.null(date_to) | (f_date(fs) <= as.Date(date_to))]

  if(length(fs)==0){
    warning("No file found for the corresponding dates")
    return(NULL)
  }

  rs <- raster::stack(fs) %>%
    raster::setZ(f_date(fs))

  if(!is.null(extent.sp)){
    rs <- raster::crop(rs, extent.sp)
  }

  return(rs)
}




#' Join gfas to tibble with trajs and date column. In each row of mt, trajs should be a tibble encapsulated in a list
#'
#' @param mt
#' @param duration_hour
#'
#' @return
#' @export
#'
gfas.attach_to_trajs <- function(mt, buffer_km=10, delay_hour=24){

  if(!all(c("location_id", "date", "trajs") %in% names(mt))){
    stop("wt should  contain the following columns: ",paste("location_id", "date", "trajs"))
  }

  # Split by run
  print("Splitting by run")
  mtf <- trajs.split_by_firedate_and_buffer(mt, buffer_km) %>%
    mutate(date_fire=date_particle) %>%
    filter(!is.na(date_fire))
  print("Done")

  print("Downloading GFAS")
  gfas.download(date_from=min(mtf$date_fire, na.rm=T),
                date_to=max(mtf$date_fire, na.rm=T))
  print("Done")

  # Read and only keep fires within extent to save memory
  # And per year (or month)
  # date_group_fn <- function(x) strftime(x,"%Y%m") # Can be year, month, or even date (lot of redundancy in the latter case)
  # mtf$date_group <- date_group_fn(mtf$date_fire)

  print("Attaching GFAS (fire_date by fire_date)")
  mtf <- pbapply::pblapply(base::split(mtf, mtf$date_fire),
                            function(x){
                              print(unique(x$date_fire))
                              extent.sp <- sf::as_Spatial(x$extent[!sf::st_is_empty(x$extent)])
                              gfas_rs <- gfas.read(date_from=min(x$date_fire, na.rm=T),
                                                   date_to=max(x$date_fire, na.rm=T),
                                                   extent.sp=extent.sp,
                                                   show.progress=F)
                              if(is.null(gfas_rs)){
                                x$pm25_emission <- 0
                              }else{
                                x$pm25_emission <- raster::extract(gfas_rs, as(x$extent,"Spatial"), sum)
                              }
                              return(x)
                            }) %>%
    do.call(bind_rows,.)

  print("Regroup by day (join runs")
  result <- mt %>%
    left_join(
      mtf %>%
        group_by(location_id, date) %>%
        summarise_at(c("pm25_emission"),
                     sum,
                     na.rm=T) %>%
        group_by(location_id, date) %>%
        tidyr::nest() %>%
        rename(fires=data)
    )
  print("Done")

  return(result)
}

