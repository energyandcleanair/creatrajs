require(dplyr)
require(sf)
require(rcrea)
require(splitr)
require(pbmcapply)
# remove.packages("ggmap")
# devtools::install_github("dkahle/ggmap") # Needed for bbox and google basemaps to work
require(ggmap)
require(fuzzyjoin)
require(googleCloudStorageR)

source('./config.R')
source('./utils.R')
source('./plots.R')

# cities <- c("pune","jaipur", "hyderabad","mumbai","varanasi","bengaluru","bangalore","Ahmedabad","chennai","kolkata")
# cities <- c("mumbai","varanasi","bengaluru","bangalore","Ahmedabad","chennai","kolkata")
cities <- "delhi"

m <- bind_rows(
  rcrea::measurements(city=cities, source="cpcb", date_from="2019-09-01", date_to="2019-10-31", poll="pm25", with_geometry=T, with_metadata = T),
  rcrea::measurements(city=cities, source="cpcb", date_from="2020-09-01", date_to="2020-10-31", poll="pm25", with_geometry=T, with_metadata = T))

m$geometry <- st_centroid(m$geometry)

powerplants <- rcrea::locations(type='powerplant', with_meta = T, with_geometry = T)
powerplants$geometry <- st_centroid(powerplants$geometry)


f <- utils.read.fires()

mf <- utils.attach.fires(m, f, radius_km=radius_km)

mft <- utils.attach.trajs(mf, met_type=met_type, duration_hour=duration_hour, height=height)

mftb <- mft %>% utils.attach.basemaps(radius_km=radius_km, zoom_level=7)

mftb.plotted <- mftb %>%
  rowwise() %>%
  mutate(filename=paste("map.trajs-fire-power",gsub("-","",date),
                        country, region_id, paste0(radius_km,"km"), gsub("\\.","",met_type), sep=".")
) %>%
  ungroup() %>%
  mutate(plot=purrr::pmap_chr(., map.trajs, powerplants=powerplants),
         meta=purrr::pmap_chr(., utils.save.meta))



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
                                       predefinedAcl="default")),)
