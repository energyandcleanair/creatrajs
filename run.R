library(remotes)
remotes::install_github("energyandcleanair/creatrajs", upgrade_dependencies = FALSE, upgrade="never")
library(creatrajs)

###########################
# PARAMETERS
###########################
source <- "airvisual"
source_city <- list(
  "airvisual"=c("hong kong", "bangkok", "jakarta", "manila"),
  "openaq"=c("Rawalpindi", "Islamabad", "Lahore", "Karachi"),
  "cpcb"=c("lucknow","pune","jaipur","hyderabad","mumbai","varanasi","bengaluru","bangalore","Ahmedabad","chennai","kolkata", "chandigarh")
)

date_from <- lubridate::today() - 7
date_to <- lubridate::today()
buffer_km <- 20
radius_km <- 200
duration_hour <- 72 # Liu 2018
heights <- c(100, 500) # Liu 2018. Should be improved by considering PBL pre/post monsoon
met_types <- c("gdas1")
add_fires <- T
poll <- c("pm10")
region <- "South_Asia"

###########################
# PROCESSING
###########################
creatrajs::utils.fires.download(date_from=date_from, date_to=date_to, region=region)
f <- creatrajs::utils.fires.read(date_from=date_from, date_to=date_to, region=region)
powerplants <- NULL #rcrea::locations(type='powerplant', with_meta = T, with_geometry = T) %>% mutate(geometry=st_centroid(geometry))


lapply(names(source_city), function(source){

  cities <- source_city[[source]]

  for(met_type in met_types){
    for(height in heights){
      creatrajs::process(city = cities,
                         poll=poll,
                         fires = f,
                         powerplants=powerplants,
                         source=source,
                         date_from=date_from,
                         date_to=date_to,
                         met_type=met_type,
                         duration_hour=duration_hour,
                         add_fires=add_fires,
                         height=height,
                         radius_km=radius_km,
                         buffer_km=buffer_km,
                         upload_results = T
      )
    }
  }
})

