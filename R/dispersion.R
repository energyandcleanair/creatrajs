#' Calculate backward dispersion at a geometry and dates
#'
#' @param dates
#' @param location_id
#' @param geometry
#' @param met_type
#' @param heights
#' @param duration_hour
#' @param ...
#'
#' @return list of tibble of dispersions (named by dates)
#' @export
#'
#' @examples
dispersion.get <- function(dates,
                      location_id,
                      geometry,
                      met_type,
                      heights,
                      duration_hour,
                      timezone="UTC",
                      res_deg=0.05,
                      cache_folder=NULL,
                      parallel=F, # NOT TOTALLY WORKING YET (weather download at least is an issue)
                      mc.cores=max(parallel::detectCores()-1,1),
                      ...){



  # Row by row
  dispersion.get.one <- function(date,
                            location_id,
                            geometry,
                            met_type,
                            height,
                            duration_hour,
                            timezone,
                            cache_folder,
                            res_deg){
    tryCatch({

      # This is so time consuming that
      # we save both sp and tif, in case we want to regenerate tif differently later on
      file.cache.tif <- file.path(
        cache_folder,
        dispersion.cache_filename(location_id, met_type, height, duration_hour, date, "tif"))

      file.cache.sp <- file.path(
        cache_folder,
        dispersion.cache_filename(location_id, met_type, height, duration_hour, date, "RDS"))

      file.cache.tif.exists <- !is.null(cache_folder) &&
        file.exists(file.cache.tif) &&
        file.info(file.cache.tif)$size > 100

      file.cache.sp.exists <- !is.null(cache_folder) &&
        file.exists(file.cache.sp) &&
        file.info(file.cache.sp)$size > 100

      if(file.cache.tif.exists){
        return(raster::raster(file.cache.tif))
      }else{
        if(file.cache.sp.exists){
          d <- readRDS(file.cache.sp)
        }else{
          # Compute dispersion
          d <- hysplit.dispersion(date=date,
                                  geometry=geometry,
                                  met_type=met_type,
                                  duration_hour=duration_hour,
                                  height=height,
                                  timezone=timezone
          )

          if(length(d)==0 || (length(d)==1 && is.na(d))){
            return(NA)
          }

          # Save to cache
          if(!is.null(cache_folder)){
            saveRDS(d, file.cache.sp)
          }
        }

        # Convert sp data to raster
        r.count <- dispersion.to_raster(d, res_deg=res_deg)
        # Save to cache
        if(!is.null(cache_folder)){
          saveRDS(d, file.cache.sp)
        }

        return(r.count)

      }}, error=function(c){
        print(c)
        warning(paste("Failed to calculate dispersion:", c))
        return(NA)
      })
  }

  # Run all of them
  mapply_ <- if(parallel){
    function(...){
      #pbmcmapply is annoying...
      #Result structure varies whether there's been a warning or not
      result <- pbmcapply::pbmcmapply(..., mc.cores=mc.cores)
      if("value" %in% names(result)) result$value else result
    }
  }else{
    pbapply::pbmapply
  }

  if(is.null(cache_folder)){
    cache_folder <- list(cache_folder)
  }

  disps <- mapply_(
    dispersion.get.one,
    date=dates,
    location_id=location_id,
    geometry=geometry,
    met_type=met_type,
    height=heights,
    duration_hour=duration_hour,
    timezone=timezone,
    res_deg=res_deg,
    cache_folder=cache_folder,
    SIMPLIFY=F)

  return(disps)
}


dispersion.cache_filename <- function(location_id, met_type, height, duration_hour, date, extension){
  paste("dispersion",
        tolower(location_id),
        gsub("\\.","",tolower(met_type)),
        height,
        duration_hour,
        gsub("-","",date),
        extension,
        sep=".")
}

dispersion.to_raster <- function(d, res_deg){

  d$id <- seq(1, nrow(d))
  sp::coordinates(d) <- ~lon+lat
  r <- raster::raster(d,
                      resolution=res_deg)
  raster::crs(r) <- sp::CRS("+init=epsg:4326")

  raster::rasterize(d,
                    r,
                    field="id",
                    fun='count')
}



hysplit.dispersion <- function(date, geometry, height, duration_hour, met_type, timezone="UTC"){

  dir_hysplit_met <- Sys.getenv("DIR_HYSPLIT_MET", here::here(utils.get_cache_folder("weather")))
  dir_hysplit_output <- file.path(tempdir(), substr(uuid::UUIDgenerate(),1,6)) # Important so that several computations can be ran simultaneously!!
  dir.create(dir_hysplit_output, showWarnings = F, recursive = T)
  lat <- sf::st_coordinates(geometry)[2]
  lon <- sf::st_coordinates(geometry)[1]

  # Build date/hour combinations in UTC
  date_utc <- as.POSIXct(date, tz=timezone) %>% lubridate::with_tz("UTC")

  tryCatch({
    # d <- splitr::hysplit_dispersion(lat = lat,
    #                                lon = lon,
    #                                height = height,
    #                                start_day = as.Date(date_utc),
    #                                start_hour = lubridate::hour(date_utc),
    #                                duration = duration_hour,
    #                                direction = "backward",
    #                                # vert_motion = 0,
    #                                # model_height = 20000,
    #                                # particle_num = 2500,
    #                                # particle_max = 10000,
    #                                species=list(
    #                                  name="particle",
    #                                  release_start = date_utc,
    #                                  release_end = date_utc + lubridate::hours(24), #We receive particles for 24h
    #                                  rate = 1, # mass units per hour
    #                                  # Receptor factors (taken from https://acp.copernicus.org/articles/20/10259/2020/)
    #                                  pdiam = 0.8, # particle diameter in µm
    #                                  density = 2, # g/cm3
    #                                  shape_factor = 0.8 #?? Taken from splitr example
    #                                ),
    #                                met_type = met_type,
    #                                met_dir =dir_hysplit_met,
    #                                exec_dir = dir_hysplit_output,
    #                                clean_up = T)


    # Running time on personal computer
    # 5 hours, 1 rate: 34 sec
    # 5 hours, 5 rate: 37 sec
    # 5 hours, 25 rate: 36 sec
    # 5 hours, 125 rate: 68 sec
    # But all have same size, not sure how this is relevant

    duration_sampling = 24
    duration_trajectories = duration_hour

    start_sampling = date_utc
    end_sampling = date_utc + lubridate::hours(duration_sampling)
    end_simulation = end_sampling
    start_simulation = start_sampling - lubridate::hours(duration_hour)

    dispersion_model <-
      splitr::create_dispersion_model() %>%
      # Source = receptor since we'll use 'backward' direction
      splitr::add_source(
        name = "particle",
        lat = lat, lon = lon, height = height,

        # Release dates need to be reversed!!
        # Splitr doesn't generate proper control file otherwise
        # see https://www.ready.noaa.gov/documents/Tutorial/html/src_back.html
        release_start = start_sampling,
        release_end = end_sampling, #We receive particles for 24h

        # Receptor factors (taken from https://acp.copernicus.org/articles/20/10259/2020/)
        rate = 1, # mass units per hour
        pdiam = 0.8, # particle diameter in µm
        density = 2, #g/cm3
        shape_factor = 0.8,
      ) %>%
      splitr::add_dispersion_params(
        start_time = start_simulation, #Trace them back for n hours
        #not the real end_time since we're going backward: end-start will be used and retracted to start in splitr
        end_time = end_simulation,
        direction = "backward", #When using backward, duration will have a minus sign in front.
        met_type = met_type,
        met_dir =dir_hysplit_met,
        exec_dir = dir_hysplit_output,
        clean_up = F
      ) %>%
      splitr::run_model()

    # dispersion_model %>% splitr::dispersion_plot()
    d <- dispersion_model$disp_df
    return(d)
  },
  error=function(c){
    print(c)
    return(NA)
  })
}
