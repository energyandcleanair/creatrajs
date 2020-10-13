process.city <- function(cities, source, f, powerplants, met_type, duration_hour, height, radius_km, buffer_km, add_fires, date_from="2019-01-01", date_to="2020-10-31"){


  # Primary data ------------------------------------------------------------
  poll <- "pm25"
  m.base <- tidyr::crossing(region_id=tolower(cities), date=seq(lubridate::date(date_from), lubridate::date(date_to), by="d"), poll=poll)

  m <- rcrea::measurements(city=cities, source=source, date_from=date_from, date_to=date_to, poll=poll, with_geometry=T, with_metadata=T) %>%
    mutate(geometry=st_centroid(geometry),
           date=lubridate::date(date))

  if(length(unique(m$region_id))<length(cities)){
    # Need to find geometry
    # Trying from locations
    l <- plyr::ddply(rcrea::locations(city=cities) %>%
            mutate(region_id=tolower(city)),
          c("country","region_id"), summarise,
          geometry = st_centroid(st_union(geometry)))

    if(nrow(l)==0){
      stop("Couldn't find geometry")
    }

  }else{
    l <- m %>% ungroup() %>% distinct(region_id, geometry)
  }

  m.all <- m.base %>%
    left_join(l) %>%
    left_join(m %>% select(-c(geometry))) %>%
    filter(!sf::st_is_empty(geometry))


  # Fires -------------------------------------------------------------------
  if(add_fires){
    mf <- utils.attach.fires(m.all, f, radius_km=radius_km)
  }else{
    mf <- m.all %>%
      mutate(fires=NA)
  }


  # Trajectories ------------------------------------------------------------
  mft <- utils.attach.trajs(mf, met_type=met_type, duration_hour=duration_hour, height=height)

  # Plot --------------------------------------------------------------------
  mftb <- mft %>% utils.attach.basemaps(radius_km=radius_km, zoom_level=7)
  mftb.plotted <- mftb %>%
    rowwise() %>%
    mutate(filename=paste("map.trajs-fire-power",gsub("-","",date),
                        country, region_id, paste0(radius_km,"km"), gsub("\\.","",met_type), sep=".")
  ) %>%
    ungroup() %>%
    mutate(
      plot=purrr::pmap_chr(., map.trajs,
                           powerplants=powerplants,
                           duration_hour=duration_hour,
                           met_type=met_type,
                           height=height,
                           add_fires=add_fires),
           meta=purrr::pmap_chr(., utils.save.meta,
                                duration_hour=duration_hour,
                                met_type=met_type,
                                height=height))


  # Upload ------------------------------------------------------------------
  gcs_auth(Sys.getenv('GCS_AUTH_FILE'))
  mftp.uploaded <- mftb.plotted %>%
    filter(!is.na(plot)) %>%
    rowwise() %>%
    mutate(plot_uploaded=list(gcs_upload(plot,
                                    bucket=trajs.bucket,
                                    name=paste0(trajs.folder,"/",basename(plot)),
                                    predefinedAcl="default")),
           meta_uploaded=list(gcs_upload(meta,
                                         bucket=trajs.bucket,
                                         name=paste0(trajs.folder,"/",basename(meta)),
                                         predefinedAcl="default")))
}
