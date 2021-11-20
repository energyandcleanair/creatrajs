test_that("getting gfas works", {

  require(rcrea)
  require(testthat)

  date_from <- "2021-01-01"
  date_to <- "2021-01-05"
  creatrajs::gfas.download(date_from=date_from, date_to=date_to)

  rs <- creatrajs::gfas.read(
    date_from=date_from,
    date_to=date_to,
    extent.sp=NULL)

  expect_equal(raster::nlayers(rs), as.integer(lubridate::date(date_to)-lubridate::date(date_from)))


  # Testing that extent filtering works as well

  # Getting Delhi region box
  m <- rcrea::cities(name="Delhi",
                     with_geometry = T)

  extent.sp <- m$geometry[[1]] %>%
    sf::st_buffer(1) %>%
    as("Spatial")

  rs.delhi <- creatrajs::gfas.read(
    date_from=date_from,
    date_to=date_to,
    extent.sp=extent.sp)

  expect_equal(dim(rs.delhi) < dim(rs), c(T,T,F))
})



test_that("attaching gfas - trajectories", {

  require(rcrea)
  require(testthat)
  require(tictoc)
  require(tidyverse)

  date_from <- "2018-01-01"
  date_to <- "2018-01-03"
  buffer_km <- 50

  m <- rcrea::measurements(city="Bangkok",
                           poll="pm25",
                           source="air4thai",
                           date_from = date_from,
                           date_to = date_to,
                           process_id="city_day_mad",
                           with_geometry=T
  )

  creatrajs::gfas.download(
    date_from=as.Date(date_from)-1,
    date_to=date_to)

  # Get trajectories
  mt <- m %>%
    rowwise() %>%
    mutate(trajs=
             creatrajs::trajs.get(dates=date,
                                  geometry = geometry,
                                  location_id = location_id,
                                  met_type = "gdas1",
                                  heights = 500,
                                  duration_hour = 72,
                                  hours = seq(0,23),
                                  cache_folder = utils.get_cache_folder("trajs")
             )
    )

  # Attach GFAS
  tic()
  mtf <- creatrajs::gfas.attach_to_trajs(mt, buffer_km=buffer_km, split_days=F)
  expect_equal(
    lapply(mtf$fires, names) %>% unlist() %>% unique(),
    "pm25_emission")
  toc()

  tic()
  mtf <- creatrajs::gfas.attach_to_trajs(mt, buffer_km=buffer_km, split_days=T)
  expect_equal(
    lapply(mtf$fires, names) %>% unlist() %>% unique() %>% sort(),
    c("pm25_emission_dayminus0", "pm25_emission_dayminus1", "pm25_emission_dayminus2", "pm25_emission_dayminus3"))
  toc()



  expect_equal(nrow(mt), nrow(mtf))
  expect_true("fires" %in% names(mtf))
  expect_gt(mtf$fires[[1]]$pm25_emission, 0)

})



test_that("attaching fire - circular", {

  require(rcrea)
  require(testthat)
  m <- rcrea::measurements(city="Lahore",
                           poll="pm25",
                           source="openaq_government",
                           date_from = "2020-01-05",
                           date_to = "2020-01-07",
                           process_id="city_day_mad",
                           with_geometry=T
  )

  expect_equal(nrow(m), 3)

  # Build circle
  mt <- m %>%
    rowwise() %>%
    mutate(extent=
             trajs.circular_extent(geometry=geometry,
                                   buffer_km=200)
    )

  # Attach fire
  mtf <- fire.attach_to_extents(mt, delay_hour=72, terra_or_raster="terra")

  mtf <- fire.attach_to_extents(mt, delay_hour=72, terra_or_raster="raster")

  expect_gt(mtf$fires[[1]]$fire_count, 0)
  expect_gt(mtf$fires[[1]]$fire_frp, 0)
})
