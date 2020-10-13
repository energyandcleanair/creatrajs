source('./config.R')
source('./utils.R')
source('./plots.R')
source('./process.R')

###########################
# PARAMETERS
###########################
# cities <- c("singapore", "jakarta", "hanoi", "Ho Chi Minh City", "Bangkok", "chiang mai", "Kuala Lumpur", "Bataan", "Manila") #,"lucknow")#,"pune","jaipur","hyderabad","mumbai","varanasi","bengaluru","bangalore","Ahmedabad","chennai","kolkata", "chandigarh")
# cities <- c("Rawalpindi", "Islamabad", "Lahore", "Karachi")
cities <- c("delhi","lucknow","pune","jaipur","hyderabad","mumbai","varanasi","bengaluru","bangalore","Ahmedabad","chennai","kolkata", "chandigarh")
source <- "cpcb"
date_from <- "2019-01-01"
date_to <- "2019-12-31"
buffer_km <- 20
radius_km <- 200
duration_hour <- 72 # Liu 2018
height <- 500 # Liu 2018. Should be improved by considering PBL pre/post monsoon
met_type <- "reanalysis"
add_fires <- T

###########################
# PROCESSING
###########################
f <- utils.read.fires()
powerplants <- rcrea::locations(type='powerplant', with_meta = T, with_geometry = T) %>%
  mutate(geometry=st_centroid(geometry))


process.city(cities = cities,
             f = f,
             powerplants=powerplants,
             source=source,
             date_from=date_from,
             date_to=date_to,
             met_type=met_type,
             duration_hour=duration_hour,
             add_fires=add_fires,
             height=height,
             radius_km=radius_km,
             buffer_km=buffer_km
)


# met_types <- c("reanalysis","gdas1")
# for(city in cities){
#   for(met_type in met_types){
#     tryCatch({
#       process.city(cities = cities,
#                    f = f,
#                    powerplants=powerplants,
#                    source=source,
#                    date_from="2020-09-01",
#                    date_to=lubridate::today(),
#                    met_type=met_type,
#                    duration_hour=duration_hour,
#                    height=height
#       )
#     }, error=function(c){
#       print(paste("Failed for city",city,":",c))
#     })
#   }
# }
