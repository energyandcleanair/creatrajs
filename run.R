source('./config.R')
source('./utils.R')
source('./plots.R')
source('./process.R')

###########################
# PARAMETERS
###########################
cities <- c("delhi","lucknow","pune","jaipur","hyderabad","mumbai","varanasi","bengaluru","bangalore","Ahmedabad","chennai","kolkata", "chandigarh")
source <- "cpcb"

duration_hour <- 72 # Liu 2018
height <- 500 # Liu 2018. Should be improved by considering PBL pre/post monsoon
met_types <- c("reanalysis","gdas1")
radius_km <- 200

###########################
# PROCESSING
###########################
f <- utils.read.fires()
powerplants <- rcrea::locations(type='powerplant', with_meta = T, with_geometry = T)

for(city in cities){
  for(met_type in met_types){
    tryCatch({
      process.city(cities = cities,
                   f = f,
                   powerplants=powerplants,
                   source=source,
                   date_from="2020-09-01",
                   date_to=lubridate::today(),
                   met_type=met_type,
                   duration_hour=duration_hour,
                   height=height
      )
    }, error=function(c){
      print(paste("Failed for city",city,":",c))
    })
  }
}
