
process.city <- function(cities, source, f, powerplants, met_type, duration_hours, height, date_from="2019-09-01", date_to="2019-10-31"){

  m <- rcrea::measurements(city=cities, source="cpcb", date_from=date_from, date_to=date_to, poll="pm25", with_geometry=T, with_metadata=T)

  m$geometry <- st_centroid(m$geometry)
  powerplants$geometry <- st_centroid(powerplants$geometry)

  mf <- utils.attach.fires(m, f, radius_km=radius_km)

  mft <- utils.attach.trajs(mf, met_type=met_type, duration_hour=duration_hour, height=height)

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
                                height=height),
           meta=purrr::pmap_chr(., utils.save.meta,
                                duration_hour=duration_hour,
                                met_type=met_type,
                                height=height))

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
