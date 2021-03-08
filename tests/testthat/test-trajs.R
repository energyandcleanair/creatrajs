test_that("building trajectories work", {

  require(tictoc)
  require(rcrea)
  require(testthat)

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
  date <- "2020-01-05"
  t <- creatrajs::trajs.get(dates=date,
                 geometry = m$geometry,
                 location_id = m$location_id,
                 country = m$country,
                 met_type = "gdas1",
                 heights = 500,
                 duration_hour = 72,
                 cache_folder = NULL
                 )
  expect_false(is.na(t))
  expect_true(all(
    c("traj_dt","traj_dt_i","lat","lon","height","date") %in% names(t)
  ))

  expect_equal(unique(lubridate::date(t$traj_dt_i)), lubridate::date(date))
})




test_that("trajectories cache system works", {


  # Run first time
  creatrajs::trajs.get(dates="2020-01-05",
                       geometry = m$geometry,
                       location_id = m$location_id,
                       country = m$country,
                       met_type = "gdas1",
                       heights = 500,
                       duration_hour = 72,
                       cache_folder = utils.get_cache_folder()
  )

  # Run second time
  start <- Sys.time()
  t.cache <- creatrajs::trajs.get(dates="2020-01-05",
                                  geometry = m$geometry,
                                  location_id = m$location_id,
                                  country = m$country,
                                  met_type = "gdas1",
                                  heights = 500,
                                  duration_hour = 72,
                                  cache_folder = utils.get_cache_folder()
  )
  t.duration <- Sys.time() - start

  expect_false(is.na(t.cache))
  expect_true(t.duration<1)

})

