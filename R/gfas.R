gfas.filename_to_date <- function(f){
    as.Date(gsub(".*([0-9]{8})\\.nc","\\1", f), format="%Y%m%d", tz="UTC")
}

gfas.available_filenames <- function(){
  d <- utils.get_gfas_folder()
  list.files(d, "*.nc", full.names = T) #TODO Further filtering
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

  ecmwfr::wf_set_key(user = Sys.getenv("ECMWF_API_EMAIL"), key = Sys.getenv("ECMWF_API_KEY"), service = "webapi")
  ecmwfr::wf_datasets(Sys.getenv("ECMWF_API_EMAIL"))

  lapply(sets, function(set){
    date_from <- min(set)
    date_to <- max(set)
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
      Sys.getenv("ECMWF_API_EMAIL"),
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
      r.band <- raster::raster(filename, band=i)
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

  fs <- fs[is.null(date_from) | (f_date(fs) >= as.POSIXct(date_from))]
  fs <- fs[is.null(date_to) | (f_date(fs) <= as.POSIXct(date_to))]

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
  mtf <- mt %>%
    rowwise() %>%
    mutate(trajs.run=list(trajs %>%
                            mutate(run2=run) %>%
                            group_by(run2) %>%
                            tidyr::nest(trajs=-run2) %>%
                            rename(run=run2))) %>%
    select(-c(trajs)) %>%
    tidyr::unnest(trajs.run) %>%
    rowwise() %>%
    filter(nrow(trajs)>1) %>%
    mutate(extent=trajs.buffer(trajs=trajs, buffer_km=buffer_km),
           min_date_fire=min(trajs$traj_dt, na.rm=T)-lubridate::hours(delay_hour),
           max_date_fire=max(trajs$traj_dt, na.rm=T)
    )
  print("Done")

  print("Downloading fires")
  gfas.download(date_from=min(mtf$min_date_fire, na.rm=T),
                date_to=max(mtf$max_date_fire, na.rm=T))
  print("Done")

  # Read and only keep fires within extent to save memory
  # And per year (or month)
  date_group_fn <- function(x) strftime(x,"%Y%m") # Can be year, month, or even date (lot of redundancy in the latter case)
  mtf$date_group <- date_group_fn(mtf$max_date_fire)

  print("Attaching GFAS (month by month)")
  mtf <- pbapply::pblapply(base::split(mtf, mtf$date_group),
                           function(mtf){
                             extent.sp <- sf::as_Spatial(mtf$extent[!sf::st_is_empty(mtf$extent)])
                             gfas_rs <- gfas.read(date_from=min(mtf$min_date_fire, na.rm=T)-lubridate::days(1),
                                               date_to=max(mtf$max_date_fire, na.rm=T),
                                               extent.sp=extent.sp,
                                               show.progress=F)

                             mtf$fires <- mapply(
                               gfas.attach_to_trajs_run,
                               trajs_run=mtf$trajs,
                               extent=mtf$extent,
                               gfas_rs=list(gfas_rs),
                               delay_hour=delay_hour,
                               SIMPLIFY = F
                             )
                             return(mtf)
                           }) %>%
    do.call(bind_rows,.)

  print("Regroup by day (join runs")
  result <- mt %>%
    left_join(
      mtf %>%
        tidyr::unnest(fires) %>%
        group_by(location_id, date) %>%
        summarise_at(c("pm25_emission"),
                     mean,
                     na.rm=T) %>%
        group_by(location_id, date) %>%
        tidyr::nest() %>%
        rename(fires=data)
    )
  print("Done")

  return(result)
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
gfas.attach_to_trajs_run <- function(trajs_run, extent, gfas_rs, delay_hour=24){

  if(length(unique(trajs_run$run))>1){
    stop("This function should only be called for one trajectory run")
  }

  if(is.null(gfas_rs)){
    return(tibble(pm25_emission=0))
  }

  extent.sf <- sf::st_sfc(extent)
  sf::st_crs(extent.sf) <- sf::st_crs(gfas_rs)

  rs_dates <- as.POSIXct(gfas_rs@z$time)
  rs_layers_idx <- (rs_dates <= max(trajs_run$traj_dt, na.rm=T)) &
    (rs_dates >= min(trajs_run$traj_dt, na.rm=T) - lubridate::hours(delay_hour))

  trajs_run_date <- split(trajs_run, lubridate::date(trajs_run$traj_dt))


  emissions <- lapply(trajs_run_date,
                      function(trajs_day){
                        date <- unique(lubridate::date(trajs_day$traj_dt))
                        rs_idx <- which(rs_dates==date)
                        if(length(rs_idx)==0){
                          return(NA)
                        }
                        rs_date <- raster::subset(gfas_rs, rs_idx)
                        extent <- trajs.buffer(trajs=trajs_day, buffer_km=buffer_km) %>% sf::st_as_sf()
                        raster::extract(rs_date, extent, sum)[[1]]
                      })


  return(tibble(pm25_emission=as.numeric(sum(unlist(emissions), na.rm=T))))

  #
  #
  # f.sf %>%
  #   rs_layers_idx
  #   filter( %>%
  #   filter(nrow(.)>0 &   suppressMessages(sf::st_intersects(., extent.sf, sparse = F))) %>%
  #   as.data.frame() %>%
  #   select(acq_date, frp) %>%
  #   full_join(trajs_run,by = character()) %>%
  #   filter(as.POSIXct(acq_date, tz="UTC") <= traj_dt,
  #          as.POSIXct(acq_date, tz="UTC") >= traj_dt - lubridate::hours(delay_hour)) %>%
  #   group_by() %>%
  #   summarise(
  #     fire_frp=sum(frp, na.rm=T),
  #     fire_count=dplyr::n()
  #   )
}


