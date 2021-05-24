
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

  date_from <- max(lubridate::date(date_from), lubridate::date("2020-01-01"))
  # # Data not available in repository before that
  # # Have been downloaded manually prior 2020
  # # https://firms.modaps.eosdis.nasa.gov/download/
  # if(date_to >= date_from){
  #   files.all <- paste0("SUOMI_VIIRS_C2_",
  #                       region,
  #                       "_VNP14IMGTDL_NRT_",
  #                       as.POSIXct(seq(lubridate::date(date_from),
  #                                      lubridate::date(date_to),
  #                                      by="day")) %>% strftime("%Y%j"),
  #                       ".txt")
  #
  #   # Even though wget can manage already downloaded files,
  #   # this is faster and less verbose in console
  #   files.existing <- list.files(d, "*.txt")
  #   file.todownload <- setdiff(files.all, files.existing)
  #
  #   modis_key <- Sys.getenv("MODIS_KEY")
  #   for(f in file.todownload){
  #     tryCatch({
  #       cmd <- paste0("wget -e robots=off -nc -np -R .html,.tmp -nH --cut-dirs=5 \"https://nrt3.modaps.eosdis.nasa.gov/api/v2/content/archives/FIRMS/suomi-npp-viirs-c2/", region, "/", f,"\" --header \"Authorization: Bearer ",
  #                     modis_key,
  #                     "\" -P ",
  #                     d)
  #       system(cmd)
  #     }, error=function(e) print(e))
  #   }
  # }
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

  fs <- list.files(d, "*.nc", full.names = T) #TODO Further filtering

  f_date <- function(f){
    as.POSIXct(gsub(".*([0-9]{8})\\.nc","\\1", f), format="%Y%m%d", tz="UTC")
  }

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


