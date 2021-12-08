
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

    # To generate EOSDIS Token: https://nrt3.modaps.eosdis.nasa.gov/profile/app-keys
    eosdis_token <- Sys.getenv("EOSDIS_TOKEN")
    for(f in file.todownload){
      tryCatch({
        cmd <- paste0("wget -e robots=off -nc -np -R .html,.tmp -nH --cut-dirs=5 \"https://nrt3.modaps.eosdis.nasa.gov/api/v2/content/archives/FIRMS/suomi-npp-viirs-c2/", region, "/", f,"\" --header \"Authorization: Bearer ",
                      eosdis_token,
                      "\" -P ",
                      d)
        system(cmd)
      }, error=function(e) print(e))
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
fire.read <- function(date_from=NULL, date_to=NULL, region="Global", extent.sp=NULL, show.progress=T,
                      parallel=T,
                      mc.cores=max(parallel::detectCores()-1,1)){

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
        df <- df[!is.na(sp::over(df, as(extent.sp,"SpatialPolygons"))),]
      }

      df %>%
        sf::st_as_sf() # To allow bind_rows (vs rbind)

    }, error=function(c){
      warning(paste("Failed reading file", f))
      message(c)
      return(NULL)
    })
  }

  lapply_ <- parallel::mclapply

  files_sorted <- sort(files[!is.na(files)]) # Sort to read fire_global* first
  if(length(files_sorted)==0){
    return(tibble())
  }

  fires <- do.call("bind_rows",
                   lapply_(files_sorted,
                           read.csv.fire,
                           mc.cores = ifelse(parallel, mc.cores, 1)))
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
#' @param split_days whether or not to split fires by their "age" (in days) before they reach the city
#'
#' @return
#' @export
#'
fire.attach_to_trajs <- function(mt, buffer_km=10, delay_hour=24,
                                 split_days=F,
                                 parallel=T,
                                 mc.cores=max(parallel::detectCores()-1,1)){

  if(!all(c("location_id", "date", "trajs") %in% names(mt))){
    stop("wt should  contain the following columns: ",paste("location_id", "date", "trajs"))
  }

  # Split by run
  print("Splitting by run")
  mtf <- trajs.split_by_run_and_buffer(mt, buffer_km) %>%
    filter(!is.na(date_fire))
  print("Done")

  print("Downloading fires")
  fire.download(date_from=min(mtf$date_fire, na.rm=T),
                date_to=max(mtf$date_fire, na.rm=T))
  print("Done")

  # Read and only keep fires within extent to save memory
  # And per year (or month)
  date_group_fn <- function(x) strftime(x,"%Y%m") # Can be year, month, or even date (lot of redundancy in the latter case)
  mtf$date_group <- date_group_fn(mtf$date_fire)

  print("Attaching fires (month by month)")
  mtf <- parallel::mclapply(base::split(mtf, mtf$date_group),
         function(mtf_chunk){
           tryCatch({
             print(unique(mtf_chunk$date_group))
             extent.sp <- sf::as_Spatial(mtf_chunk$extent[!sf::st_is_empty(mtf_chunk$extent)])
             f.sf <- fire.read(date_from=min(mtf_chunk$date_fire, na.rm=T)-lubridate::days(1),
                               date_to=max(mtf_chunk$date_fire, na.rm=T),
                               extent.sp=extent.sp,
                               show.progress=F,
                               parallel=F)

             if(is.null(f.sf)){
               warning("Didn't find any fire.")
               mtf_chunk$fires <- list(tibble())
             }else{
               mtf_chunk$fires <- mapply(
                 fire.attach_to_trajs_run,
                 date_fire=mtf_chunk$date_fire,
                 extent=mtf_chunk$extent,
                 f.sf=list(f.sf),
                 delay_hour=delay_hour,
                 SIMPLIFY = F
               )
             }
             return(mtf_chunk)
           }, error=function(e){
             warning("Failed to attach fire to trajectories: ", e, "\n: ", mtf_chunk)
             return(NULL)
           })
         }, mc.cores = ifelse(parallel, mc.cores,1)) %>%
    subset(unlist(lapply(., is.data.frame))) %>% # Sometimes a core fails
    do.call(bind_rows,.)

  print("Regroup by day (join runs)")

  if(split_days){
    fire_data <- mtf %>%
      tidyr::unnest(fires) %>%
      mutate(age_fire=difftime(as.Date(date), as.Date(date_fire), units="days")) %>%
      group_by(location_id, date, age_fire) %>%
      summarise_at(vars(starts_with("fire_")), mean, na.rm=T) %>%
      tidyr::pivot_wider(c(location_id, date),
                         names_from=age_fire,
                         names_prefix="dayminus",
                         values_from = c("fire_frp", "fire_count"))
  }else{
    fire_data <-  mtf %>%
      tidyr::unnest(fires) %>%
      group_by(location_id, date) %>%
      summarise_at(vars(starts_with("fire_")), mean, na.rm=T)
  }


  result <- mt %>%
    left_join(
      fire_data %>%
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
fire.attach_to_trajs_run <- function(date_fire, extent, f.sf,
                                     delay_hour=24){

  if(nrow(f.sf)==0){
    return(tibble(fire_frp=0, fire_count=0))
  }

  extent.sf <- sf::st_sfc(extent)
  sf::st_crs(extent.sf) <- sf::st_crs(f.sf)

  f.sf %>%
    # TODO devise a more accurate way yet not as slow as fully granular method
    filter(acq_date <= lubridate::date(date_fire),
           acq_date >= lubridate::date(date_fire) - lubridate::hours(delay_hour)) %>%
    filter(nrow(.)>0 & suppressMessages(sf::st_intersects(., extent.sf, sparse = F))) %>%
     as.data.frame() %>%
  select(acq_date, frp) %>%
  # full_join(trajs_run, by = character()) %>%
  # filter(as.POSIXct(acq_date, tz="UTC") <= traj_dt,
  #        as.POSIXct(acq_date, tz="UTC") >= traj_dt - lubridate::hours(delay_hour)) %>%
  group_by() %>%
  summarise(
    fire_frp=sum(frp, na.rm=T),
    fire_count=dplyr::n()
  )
}


#' Join active fire data to tibble with trajs_rs RASTER STACK and date column. In each row of mt, trajs_rs should be a rasterstack encapsulated in a list
#'
#' @param mt
#' @param duration_hour
#'
#' @return
#' @export
#'
fire.attach_to_trajs_rs <- function(mt, delay_hour=24){

  if(!all(c("location_id", "date", "trajs_rs") %in% names(mt))){
    stop("wt should  contain the following columns: ",paste("location_id", "date", "trajs_rs"))
  }

  layer_name_to_date <- function(n){strptime(n, "X%Y.%m.%d", tz="UTC")}

  # Split by run
  mtf <- mt %>%
    rowwise() %>%
    mutate(min_date_fire=min(layer_name_to_date(names(trajs_rs))),
           max_date_fire=max(layer_name_to_date(names(trajs_rs))),
           extent=list(sf::st_as_sfc(sf::st_bbox(raster::extent(trajs_rs))))
    )


  print("Downloading fires")
  fire.download(date_from=min(mtf$min_date_fire, na.rm=T),
                date_to=max(mtf$max_date_fire, na.rm=T))
  print("Done")

  # Read and only keep fires within extent to save memory
  # And per year (or month)
  date_group_fn <- function(x) strftime(x,"%Y%m") # Can be year, month, or even date (lot of redundancy in the latter case)
  mtf$date_group <- date_group_fn(mtf$max_date_fire)

  print("Attaching fires (month by month)")
  mtf <- pbapply::pblapply(base::split(mtf, mtf$date_group),
                           function(mtf){

                             extent.sp <- sf::as_Spatial(do.call(sf::st_union, mtf$extent))
                             f.sf <- fire.read(date_from=min(mtf$min_date_fire, na.rm=T)-lubridate::days(1),
                                               date_to=max(mtf$max_date_fire, na.rm=T),
                                               extent.sp=extent.sp,
                                               show.progress=F)

                             mtf$fires <- mapply(
                               fire.attach_to_trajs_single_rs,
                               trajs_rs=mtf$trajs_rs,
                               extent=mtf$extent,
                               f.sf=list(f.sf),
                               delay_hour=delay_hour,
                               SIMPLIFY = F
                             )
                             return(mtf)
                           }) %>%
    do.call(bind_rows,.)
  print("Done")

  return(mtf)
}

#' Attach fires to a single raster stack
#'
#' @param trajs_run
#' @param f.sf
#' @param delay_hour how "old" can a fire be to be accounted for in trajectory
#'
#' @return tibble of fires
#'
#' @examples
fire.attach_to_trajs_single_rs <- function(trajs_rs, extent, f.sf, delay_hour=24){


  extent.sf <- sf::st_sfc(extent)
  sf::st_crs(extent.sf) <- sf::st_crs(f.sf)

  dates <- strptime(names(trajs_rs), "X%Y.%m.%d", tz="UTC")

  f.sf.dates <- f.sf %>%
    filter(acq_date <= max(dates),
           acq_date >= min(dates) - lubridate::hours(delay_hour)) %>%
    filter(nrow(.)>0 &  suppressMessages(sf::st_intersects(., extent.sf, sparse = F)))

  f <- lapply(names(trajs_rs), function(layer){
    date <- strptime(layer, "X%Y.%m.%d", tz="UTC")
    fires.date <- f.sf %>% filter(acq_date==date)
    if(nrow(fires.date)==0){return(list(fire_count=0,fire_frp=0))}
    fires.date$weight <-
      raster::extract(
        trajs_rs[[layer]],
        fires.date
    )
    tibble(fire_count=sum(fires.date$weight, na.rm=T),
         fire_frp=sum(fires.date$weight*fires.date$frp, na.rm=T))
  })

  do.call(rbind, f) %>%
    group_by() %>%
    summarise_at(
      c("fire_frp","fire_count"),
      sum,
      na.rm=T)
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
  f.sf <- fire.read(date_from=min(mt$date, na.rm=T)-lubridate::hours(delay_hour),
                    date_to=max(mt$date),
                    extent.sp=extent.sp)
  print("Done")

  if(nrow(f.sf)==0){
    return(wt %>% mutate(fires=NULL))
  }

  print("Attaching fire")
  mt$fires <- pbapply::pbmapply(
    fire.attach_to_extent,
    date=mt$date,
    extent=mt$extent,
    f.sf=list(f.sf),
    delay_hour=delay_hour,
    SIMPLIFY = F
  )
  print("Done")

  return(mt)
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



#' Join active fire data to tibble with dispersion raster and date column. In each row of mt, trajs should be a tibble encapsulated in a list
#'
#' @param mt
#' @param duration_hour
#'
#' @return
#' @export
#'
fire.attach_to_disps <- function(mt, buffer_km=10, delay_hour=24){

  if(!all(c("location_id", "date", "disps") %in% names(mt))){
    stop("wt should  contain the following columns: ",paste("location_id", "date", "disps"))
  }


  # Split by run
  print("Splitting by run")
  mtf <- mt %>%
    rowwise() %>%
    mutate(extent=trajs.buffer(trajs=trajs, buffer_km=buffer_km),
           min_date_fire=min(trajs$traj_dt, na.rm=T)-lubridate::hours(delay_hour),
           max_date_fire=max(trajs$traj_dt, na.rm=T)
    )
  print("Done")

  print("Downloading fires")
  fire.download(date_from=min(mtf$min_date_fire, na.rm=T),
                date_to=max(mtf$max_date_fire, na.rm=T))
  print("Done")
  # Read and only keep fires within extent to save memory
  # And per year (or month)
  date_group_fn <- lubridate::month # Can be year, month, or even date (lot of redundancy in the latter case)
  mtf$date_group <- date_group_fn(mtf$max_date_fire)

  print("Attaching fires (month by month)")
  mtf <- pbapply::pblapply(base::split(mtf, mtf$date_group),
                           function(mtf){

                             extent.sp <- sf::as_Spatial(mtf$extent[!sf::st_is_empty(mtf$extent)])
                             f.sf <- fire.read(date_from=min(mtf$min_date_fire, na.rm=T),
                                               date_to=max(mtf$max_date_fire, na.rm=T),
                                               extent.sp=extent.sp,
                                               show.progress=F)

                             # if(nrow(f.sf)==0){
                             #   warning("No fire found. Something's probably wrong")
                             # }

                             mtf$fires <- mapply(
                               fire.attach_to_trajs_run,
                               trajs_run=mtf$trajs,
                               extent=mtf$extent,
                               f.sf=list(f.sf),
                               delay_hour=delay_hour,
                               SIMPLIFY = F
                             )
                             return(mtf)
                           }) %>%
    do.call(bind_rows,.)

  print("Regroup by day (join runs)")
  result <- mt %>%
    left_join(
      mtf %>%
        tidyr::unnest(fires) %>%
        group_by(location_id, date) %>%
        summarise_at(c("fire_frp","fire_count"),
                     mean,
                     na.rm=T) %>%
        group_by(location_id, date) %>%
        tidyr::nest() %>%
        rename(fires=data)
    )
  print("Done")

  return(result)
}


#' Count fires and sum FRP within geometries for every day between date_from and date_to
#'
#' @param date_from
#' @param date_to
#' @param geometries either a sf or sp
#'
#' @return
#' @export
#'
#' @examples
fire.aggregate <- function(date_from, date_to, geometries, fires=NULL){

  geometries_sp <- creahelpers::to_spdf(geometries)

  if(is.null(fires)){
    creatrajs::fire.download(date_from, date_to)
    fires <- creatrajs::fire.read(date_from=date_from, date_to=date_to, extent.sp = geometries_sp)
  }

  fires_sp <- as(fires, "Spatial")
  proj4string(fires_sp) <- proj4string(geometries_sp)
  proj4string(geometries_sp) <- proj4string(fires_sp)

  cbind(
    as.data.frame(fires) %>% select(-c(geometry)),
    sp::over(fires_sp, geometries_sp, returnList = F)) %>%
    rename(date=acq_date) %>%
    group_by_at(setdiff(names(.), "frp")) %>%
    summarise(
      frp=sum(frp),
      count=n()
    ) %>%
    ungroup()
}


#' Count fires and sum FRP within geometries for every day between date_from and date_to
#'
#' @param date_from
#' @param date_to
#' @param geometry either a sf or sp
#'
#' @return
#' @export
#'
#' @examples
fire.heatmap <- function(fires, geometry, zoom=7, facet_by="year"){

  ggmap::register_google(Sys.getenv("GOOGLE_MAP_API_KEY"))

  fires_df <- st_intersection(fires, st_as_sfc(st_bbox(geometry))) %>%
    cbind(., st_coordinates(.$geometry)) %>%
    as.data.frame() %>%
    select(-c(geometry)) %>%
    mutate(year=factor(lubridate::year(acq_date)))

  # Plot map
  plt <- ggmap::qmplot(x=X, y=Y,
         data = fires_df,
         maptype = "satellite",
         mapcolor="bw",
         color=I('darkred'),
         zoom=zoom,
         source = "google",
         xlim=sf::st_bbox(geometry)[c(1,3)],
         ylim=sf::st_bbox(geometry)[c(2,4)],
         darken = .7,
         legend = "topright") +
    stat_density_2d(aes(fill = ..level..),
                    geom = "polygon",
                    alpha = .3,
                    color = NA,
                    adjust=0.9) +
    scale_fill_distiller(palette="Reds") +
    geom_sf(data=sf::st_as_sf(geometries),
            inherit.aes = F,
            fill="transparent",
            color="#FFFFFF44")

  if(!is.null(facet_by)){
    plt <- plt + facet_wrap(as.formula(paste(".~ ", facet_by)))
  }

  return(plt)
}
