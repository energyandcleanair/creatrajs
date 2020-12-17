utils.modis.init <- function(){
  MODIS::MODISoptions(
    localArcPath=Sys.getenv("DIR_MODIS"),
    outDirPath=file.path(Sys.getenv("DIR_MODIS"),"processed"),
    dlmethod="wget",
    quiet=F)

  if(Sys.getenv("GDAL_PATH")!=""){
    MODIS::MODISoptions(gdalPath = Sys.getenv("GDAL_PATH"),
                        checkTools=T)
  }
}

utils.modis.traj_extent <- function(traj, buffer_km){
  tryCatch({
    sf::st_as_sf(traj, coords=c("lon","lat"), crs=4326) %>%
      sf::st_transform(crs=3857) %>%
      sf::st_buffer(buffer_km*1000) %>%
      sf::st_bbox() %>%
      sf::st_as_sfc()
  }, error=function(c){
    return(NA)
  })
}

utils.modis.union_extents <- function(l){
  do.call("st_union", as.list(l)) %>%
    st_bbox() %>%
    st_as_sfc()
}

utils.modis.date_rasters <- function(rasters){
  result <- tibble()
  for(d in names(rasters)){
    date_init <- lubridate::date(names(rasters[d]))
    r <- raster::unstack(raster::stack(as.character(rasters[d])))
    dates <- seq(date_init, date_init+lubridate::days(length(r)-1), by="d")
    result <- bind_rows(result,
                        tibble(date=dates, raster=r))
  }
  return(result)
}


utils.modis.geotiffs.old <- function(date_from, date_to, extent, ...){
  extent.sf <- sf::st_as_sf(sf::st_sfc(extent), crs=3857)
  begin=format(date_from,"%Y%j")
  end=format(date_to,"%Y%j")
  rasters <- MODIS::runGdal(job="MOD14A1.day", product="MOD14A1", extent=extent.sf, begin=begin, end=end, SDSstring="0010")[[1]]
  utils.modis.date_rasters(rasters)
}

utils.modis.geotiffs <- function(date_from, date_to, extent, ...){

  dir_processed <- file.path(Sys.getenv("DIR_MODIS"),"modisstp","processed")
  dir_tiff <- file.path(dir_processed, "ThermalAn_Fire_Daily_1Km_v6", "MaxFRP")
  dir_hdf <- file.path(Sys.getenv("DIR_MODIS"),"modisstp")

  # Need to extebd timeout
  r <- MODIStsp::MODIStsp(gui = FALSE,
                          start_date = format(lubridate::date(date_from)-lubridate::days(8), "%Y.%m.%d"), #Files are every 8 days. MODISstp doesn't find any file if inbetween
                          end_date   = format(lubridate::date(date_to), "%Y.%m.%d"),
                          out_folder = dir_processed,
                          out_folder_mod = dir_hdf,
                          spatmeth = "tiles",
                          # Tiles for India and Pakistan
                          start_x = 23,
                          end_x = 26,
                          start_y = 5,
                          end_y = 8,
                          selcat = "Land Cover Characteristics - Thermal Anomalies and Fire",
                          selprod = "ThermalAn_Fire_Daily_1Km (M*D14A1)",
                          prod_version = "6",
                          sensor = "Both",
                          bandsel = "MaxFRP",
                          download_server = "http",
                          user = Sys.getenv("EARTHDATA_USR"),
                          password = Sys.getenv("EARTHDATA_PWD"),
                          downloader = "http",
                          download_range = "Full",
                          out_projsel     = "User Defined",
                          output_proj = "3857",
                          out_res_sel = "Native",
                          out_res = NULL,
                          resampling = "max",
                          reprocess = T,
                          delete_hdf = F,
                          nodata_change = F,
                          scale_val = F,
                          out_format = "GTiff",
                          ts_format = NULL,#"R RasterStack",
                          compress = "LZW",
                          n_retries = 5,
                          verbose = TRUE)

  fs <- list.files(dir_tiff, "*.tif$", full.names = F)
  f <- tibble(file=fs) %>%
    ungroup() %>%
    tidyr::separate(file, c("product", "variable", "year","yday"), remove=F) %>%
    mutate(date_start=lubridate::date(paste0(year,"-01-01")) + lubridate::days(as.numeric(yday) -1)) %>%
    mutate(date_band=purrr::map(date_start, ~tibble(date=seq(.x, .x + lubridate::days(7), by="d"),
                                                             band=seq(1,8)
    ))) %>%
    tidyr::unnest(date_band) %>%
    dplyr::select(-c(date_start, yday, year)) %>%
    filter(date>=date_from,
               date<=date_to)

  rs <- f %>% distinct(file) %>%
    rowwise() %>%
    mutate(rasters = list(tibble(raster=raster::unstack(raster::stack(file.path(dir_tiff, file)))) %>%
                            mutate(band=dplyr::row_number()))) %>%
    tidyr::unnest(rasters)

  fr <- f %>%
    dplyr::left_join(rs, by=c("file","band")) %>%
    dplyr::select(date, raster)

  return(fr)
}

utils.modis.frp.average <- function(trajs.day, r, buffer_km){
  date <- lubridate::date(trajs.day$traj_dt) %>% unique()
  raster.day <- r %>% filter(date==!!date) %>% pull(raster)
  t <- st_union(st_buffer(st_transform(st_as_sf(trajs.day, coords=c("lon","lat"), crs=4326),crs=3857), buffer_km*1000))

  vs <- lapply(raster.day, function(x){
    max(0, x %>% exactextractr::exact_extract(t,fun="mean"), na.rm=T)
  })

  return(mean(unlist(vs)))

}

utils.modis.frp_at_traj <- function(trajs, country, location_id, ..., frp.rasters, buffer_km){

  tryCatch({
    traj.sf <- sf::st_as_sf(trajs, coords=c("lon","lat"), crs=4326) %>%
      st_transform(crs=3857)

    r <- frp.rasters %>% filter(country==!!country, location_id==!!location_id)

    frp.day <- trajs %>%
      group_by(date=lubridate::date(traj_dt)) %>%
      tidyr::nest(trajs=!date) %>%
      rowwise() %>%
      mutate(frp=utils.modis.frp.average(trajs,r=r, buffer_km=buffer_km),
             count=nrow(trajs))

    return(weighted.mean(frp.day$frp, frp.day$count))
  }, error=function(c){
    return(NA)
  })

}

