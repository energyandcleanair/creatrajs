

# References
# Liu, T., Marlier, M. E., DeFries, R. S., Westervelt, D. M., Xia, K. R., Fiore, A. M., … Milly, G. (2018). Seasonal impact of regional outdoor biomass burning on air pollution in three Indian cities: Delhi, Bengaluru, and Pune. Atmospheric Environment, 172, 83–92. https://doi.org/10.1016/j.atmosenv.2017.10.024

duration_hour <- 72 # Liu 2018
radius_km <- 200
height <- 500 # Liu 2018. Should be improved by considering PBL pre/post monsoon
met_type <- "gdas0.5"

ggmap::register_google("AIzaSyAM2hj3VbXCSjAXIjLjLH_DfPPSiV8Bhg0")


dir_hysplit_output <- "hysplit_output"
dir.create(dir_hysplit_output, showWarnings = F, recursive = T)

dir_hysplit_met <- Sys.getenv("DIR_HYSPLIT_MET", here::here("hysplit_met"))
dir.create(dir_hysplit_met, showWarnings = F, recursive = T)

dir_results <- "results"
dir.create(dir_results, showWarnings = F, recursive = T)


trajs.bucket <- "crea-public"
trajs.folder <- "data/trajectories/plots"
