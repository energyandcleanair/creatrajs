test_that("backward dispersion work", {

  # The main part of this test is graphical:
  # user should check dispersion and trajectories roughly overlap

  require(rcrea)
  require(testthat)
  require(tictoc)

  m <- rcrea::measurements(city="Lahore",
                           poll="pm25",
                           source="openaq_government",
                           date_from = "2020-01-01",
                           date_to = "2020-01-01",
                           process_id="city_day_mad",
                           with_geometry=T
  )

  expect_equal(nrow(m), 1)

  # Without cache
  date_from = "2020-01-04"
  date_to = "2020-01-05"
  duration_hour = 24
  dates = seq.Date(as.Date(date_from), as.Date(date_to), by="day")
  timezone = unique(m$timezone)

  tic()
  d <- creatrajs::dispersion.get(dates=dates,
                            geometry = m$geometry,
                            location_id = m$location_id,
                            country = m$country,
                            met_type = "gdas1",
                            heights = 500,
                            duration_hour = duration_hour,
                            timezone = timezone,
                            res_deg=0.01,
                            cache_folder = NULL,
                            parallel = T
  )
  toc()

  expect_false(any(is.na(d)))
  expect_equal(class(d), "list")
  expect_equal(class(d[[1]])[1], "RasterLayer")


  # Compare with trajectories (the whole backward thing can be tricky
  tic()
  t <- creatrajs::trajs.get(dates=dates,
                            geometry = m$geometry,
                            location_id = m$location_id,
                            country = m$country,
                            met_type = "gdas1",
                            heights = 500,
                            duration_hour = duration_hour,
                            timezone = timezone,
                            cache_folder = NULL)
  toc()

  tic()
  t24 <- creatrajs::trajs.get(dates=dates,
                            geometry = m$geometry,
                            location_id = m$location_id,
                            country = m$country,
                            met_type = "gdas1",
                            heights = 500,
                            duration_hour = duration_hour,
                            timezone = timezone,
                            hours=seq(1,23),
                            cache_folder = NULL)
  toc()


  i=2


  ti.sf <- t24[[i]] %>% sf::st_as_sf(coords=c("lon","lat"))
  raster::plot(d[[i]], ext=sf::st_bbox(ti.sf))
  raster::plot(as(ti.sf, "Spatial"), add=T)

})

test_that("attach_to_disp work", {

  require(rcrea)
  require(testthat)
  require(tictoc)

  date_from = "2020-01-04"
  date_to = "2020-01-04"
  duration_hour = 24
  dates = seq.Date(as.Date(date_from), as.Date(date_to), by="day")
  timezone = unique(m$timezone)

  m <- rcrea::measurements(city="Lahore",
                           poll="pm25",
                           source="openaq_government",
                           date_from = date_from,
                           date_to = date_to,
                           process_id="city_day_mad",
                           with_geometry=T
  )

  tic()
  d <- creatrajs::dispersion.get(dates=dates,
                                 geometry = m$geometry,
                                 location_id = m$location_id,
                                 country = m$country,
                                 met_type = "gdas1",
                                 heights = 500,
                                 duration_hour = duration_hour,
                                 timezone = timezone,
                                 res_deg=0.01,
                                 cache_folder = NULL,
                                 parallel = T
  )
  toc()

  expect_false(any(is.na(d)))
  expect_equal(class(d), "list")
  expect_equal(class(d[[1]])[1], "RasterLayer")


  # Compare with trajectories (the whole backward thing can be tricky
  tic()
  t <- creatrajs::trajs.get(dates=dates,
                            geometry = m$geometry,
                            location_id = m$location_id,
                            country = m$country,
                            met_type = "gdas1",
                            heights = 500,
                            duration_hour = duration_hour,
                            timezone = timezone,
                            cache_folder = NULL)
  toc()

  tic()
  t24 <- creatrajs::trajs.get(dates=dates,
                              geometry = m$geometry,
                              location_id = m$location_id,
                              country = m$country,
                              met_type = "gdas1",
                              heights = 500,
                              duration_hour = duration_hour,
                              timezone = timezone,
                              hours=seq(1,23),
                              cache_folder = NULL)
  toc()


  i=2


  ti.sf <- t24[[i]] %>% sf::st_as_sf(coords=c("lon","lat"))
  raster::plot(d[[i]], ext=sf::st_bbox(ti.sf))
  raster::plot(as(ti.sf, "Spatial"), add=T)

})


test_that("parallel works", {

  require(tictoc)
  require(rcrea)
  require(testthat)

  date_from = "2020-01-04"
  date_to = "2020-01-06"
  duration_hour = 8
  dates = seq.Date(as.Date(date_from), as.Date(date_to), by="day")

  m <- rcrea::measurements(city="Lahore",
                           poll="pm25",
                           source="openaq_government",
                           date_from = date_from,
                           date_to = date_to,
                           process_id="city_day_mad",
                           with_geometry=T
  )

  # Run no parallel
  start <- Sys.time()
  disp.noparallel <- creatrajs::dispersion.get(dates=dates,
                                           geometry = m$geometry,
                                           location_id = m$location_id,
                                           country = m$country,
                                           met_type = "gdas1",
                                           heights = 500,
                                           duration_hour = duration_hour,
                                           cache_folder = NULL,
                                           parallel=F
  )
  t.duration.noparallel <- Sys.time() - start
  expect_equal(length(disp.noparallel), length(dates))

  # Run parallel
  start <- Sys.time()
  disp.parallel <- creatrajs::dispersion.get(dates=dates,
                                         geometry = m$geometry,
                                         location_id = m$location_id,
                                         country = m$country,
                                         met_type = "gdas1",
                                         heights = 500,
                                         duration_hour = duration_hour,
                                         cache_folder = NULL,
                                         parallel=T
  )
  t.duration.parallel <- Sys.time() - start
  expect_equal(length(disp.parallel), length(dates))

  # Yield same results
  lapply(seq_along(disp.parallel),
         function(i){
           comp <- disp.parallel[[i]]==disp.noparallel[[i]]
           expect_equal(raster::minValue(comp),1)
         })
})
