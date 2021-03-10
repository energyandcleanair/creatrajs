process <- function(city,
                    source,
                    date_from,
                    poll=c("pm25","pm10"),
                    date_to=lubridate::today(),
                    met_type="gdas1",
                    duration_hour=72,
                    height=500,
                    radius_km=200,
                    buffer_km=20,
                    fires=NULL,
                    add_fires=F,
                    powerplants=NULL,
                    folder=dir_results,
                    upload_results=F
                    ){


  # Primary data ------------------------------------------------------------
  l <- rcrea::locations("city",
                        city=city,
                        source=source,
                        with_geometry = T)

  dates <- seq(as.POSIXct(date_from, "UTC"),
               as.POSIXct(date_to, "UTC"),
               by="1 day")

  meas <- rcrea::measurements(city=city, source=source,
                              date_from=date_from, date_to=date_to,
                              poll=poll,
                              with_geometry=F, with_metadata=T) %>%
    mutate(date=lubridate::force_tz(date, "UTC")) %>%
    tidyr::nest(meas=c(poll, unit, value, process_id, source)) %>%
    select(location_id, date, meas)

  m <- tidyr::crossing(l %>% select(location_id=id),
                       date=dates) %>%
    # tidyr::crossing doesn't work with geometry columns in some versions
    left_join(l %>% select(location_id=id, location_name=name, geometry, country)) %>%
    left_join(meas)


  if(nrow(m)==0){
    stop("No measurement or location for that city")
  }

  m$geometry <- sf::st_centroid(m$geometry)


  # Fires -------------------------------------------------------------------
  if(add_fires & !is.null(fires)){
    mf <- creatrajs::utils.fires.attach(m, fires, radius_km=radius_km)
  }else{
    mf <- m %>%
      mutate(fires=NA)
  }


  # Trajectories ------------------------------------------------------------
  mft <- utils.attach.trajs(mf, met_type=met_type, duration_hour=duration_hour, height=height)

  # Only keep days with trajectories
  mft <- mft  %>% filter(!is.na(trajs) && nrow(trajs)>0)


  # Fire Radiative Power ----------------------------------------------------------
  # mft <- utils.attach.frp.raster(mft, buffer_km=buffer_km, duration_hour=duration_hour)


  # Plot --------------------------------------------------------------------
  mftb <- mft %>% utils.attach.basemaps(radius_km=radius_km, zoom_level=10)
  mftb.plotted <- mftb %>%
    rowwise() %>%
    mutate(filename=paste("map.trajs-fire-power",
                          gsub("-","",date),
                          country,
                          tolower(location_name),
                          paste0(radius_km,"km"),
                          paste0(height,"m"),
                          gsub("\\.","",met_type),
                          sep=".")
  ) %>%
    ungroup() %>%
    mutate(
      plot=purrr::pmap_chr(., map.trajs,
                           powerplants=powerplants,
                           duration_hour=duration_hour,
                           met_type=met_type,
                           height=height,
                           folder=folder,
                           add_fires=add_fires),
           meta=purrr::pmap_chr(., utils.save.meta,
                                duration_hour=duration_hour,
                                met_type=met_type,
                                height=height,
                                folder=folder))


  # Upload ------------------------------------------------------------------
  if(upload_results){

    if(Sys.getenv('GCS_AUTH_FILE')!=""){
      googleCloudStorageR::gcs_auth(Sys.getenv('GCS_AUTH_FILE'))
    }

    mftp.uploaded <- mftb.plotted %>%
      filter(!is.na(plot)) %>%
      rowwise() %>%
      mutate(plot_uploaded=list(googleCloudStorageR::gcs_upload(plot,
                                           bucket=trajs.bucket(),
                                           name=paste0(trajs.folder,"/",basename(plot)),
                                           predefinedAcl="default")),
             meta_uploaded=list(googleCloudStorageR::gcs_upload(meta,
                                           bucket=trajs.bucket(),
                                           name=paste0(trajs.folder,"/",basename(meta)),
                                           predefinedAcl="default")))
  }
}
