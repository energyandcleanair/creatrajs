test_that("building trajectories work", {


  require(rcrea)
  require(testthat)

  l <- rcrea::cities(name="Bangkok", with_geometry=T)

  get_trajs <- function(use_cache){
    dates <- seq.Date(as.Date("2020-06-01"), as.Date("2020-06-10"), by="day")
    creatrajs::trajs.get(dates=dates,
                              geometry = l$geometry,
                              location_id = l$id,
                              met_type = "gdas1",
                              height = 10,
                              duration_hour = 120,
                              use_cache=use_cache,
                              save_to_cache=T
    )
  }

  # Without cache
  start_time <- Sys.time()
  t <- get_trajs(use_cache=F)
  end_time <- Sys.time()
  elapsed <- end_time - start_time

  expect_false(any(is.na(t) || is.null(t)))
  expect_true(all(
    c("traj_dt","traj_dt_i","lat","lon","height","date_recept") %in% names(t[[1]])
  ))
  expect_equal(unique(lubridate::date(t[[1]]$traj_dt_i)), lubridate::date(date))


  # With cache
  start_time <- Sys.time()
  t_cache <- get_trajs(use_cache=T)
  end_time <- Sys.time()
  elapsed_cache <- end_time - start_time

  expect_true(elapsed_cache < elapsed / 10)
  expect_equal(length(t_cache), length(t))
  expect_false(any(is.na(t_cache)) | any(sapply(t_cache, is.null)))

  expect_true(all(sapply(seq(length(t)), function(i){
    all(t[[i]]==t_cache[[i]])
  })))

})


test_that("parallel works", {

  require(tictoc)
  require(rcrea)
  require(testthat)

  date_from = "2020-01-04"
  date_to = "2020-01-10"
  dates = seq.Date(as.Date(date_from), as.Date(date_to), by="day")

  m <- rcrea::measurements(city="Delhi",
                           poll="pm25",
                           source="cpcb",
                           date_from = date_from,
                           date_to = date_to,
                           process_id="city_day_mad",
                           with_geometry=T
  )

  # Run no parallel
  start <- Sys.time()
  trajs.noparallel <- creatrajs::trajs.get(dates=dates,
                                           geometry = m$geometry,
                                           location_id = m$location_id,
                                           country = m$country,
                                           met_type = "gdas1",
                                           heights = 500,
                                           duration_hour = 72,
                                           use_cache=F,
                                           parallel=F
  )
  t.duration.noparallel <- Sys.time() - start
  expect_equal(length(trajs.noparallel), length(dates))

  # Run parallel
  start <- Sys.time()
  trajs.parallel <- creatrajs::trajs.get(dates=seq.Date(as.Date(date_from),as.Date(date_to),by="day"),
                       geometry = m$geometry,
                       location_id = m$location_id,
                       country = m$country,
                       met_type = "gdas1",
                       heights = 500,
                       duration_hour = 72,
                       use_cache=F,
                       parallel=T
  )
  t.duration.parallel <- Sys.time() - start
  expect_equal(length(trajs.parallel), length(dates))


  # Yield same results?
  # Parallel doesn't work
  expect_equal(
    nrow(dplyr::bind_rows(trajs.noparallel)),
    nrow(dplyr::bind_rows(trajs.parallel))
  )


})



test_that("vectorization works", {

  require(rcrea)
  require(testthat)

  date_from = "2020-01-04"
  date_to = "2020-01-06"
  dates = seq.Date(as.Date(date_from), as.Date(date_to), by="day")

  m <- rcrea::measurements(city=c("Delhi","Hyderabad"),
                           poll="pm25",
                           source="cpcb",
                           date_from = date_from,
                           date_to = date_to,
                           process_id="city_day_mad",
                           with_geometry=T
  )

  expect_equal(
    nrow(m),
    2*length(dates)
  )

  trajs <- creatrajs::trajs.get(dates=m$date,
                                 geometry = m$geometry,
                                 location_id = m$location_id,
                                 country = m$country,
                                 met_type = "gdas1",
                                 heights = 500,
                                 duration_hour = 72,
                                 use_cache=F,
                                 parallel=F
  )

  # Test that trajectories correspond to the right geometry
  for(i in seq(1, nrow(m))){
    expect_equal(
      round(trajs[[i]]$lon[1],3),
      round(sf::st_coordinates(m$geometry[i])[1],3))
  }

})

test_that("trajectories cache system works", {

  require(tictoc)
  require(rcrea)
  require(testthat)



  # Run first time
  creatrajs::trajs.get(dates= m$date,
                       geometry = m$geometry,
                       location_id = m$location_id,
                       country = m$country,
                       met_type = "gdas1",
                       heights = 500,
                       duration_hour = 72,
                       use_cache=F,
                       save_to_cache=F,
                       parallel=T
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
                                  use_cache=T,
                                  parallel=T
  )
  t.duration <- Sys.time() - start

  expect_false(length(t.cache)==1 && is.na(t.cache))
  expect_true(t.duration<1)

})
