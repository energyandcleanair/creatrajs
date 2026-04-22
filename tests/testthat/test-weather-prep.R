test_that("trajs.compute prepares weather once per multi-location job", {
  weather_calls <- 0
  download_met_flags <- list()

  local_mocked_bindings(
    download_weather = function(...) {
      weather_calls <<- weather_calls + 1
      invisible(NULL)
    },
    trajs.get = function(..., download_met = TRUE) {
      download_met_flags[[length(download_met_flags) + 1]] <<- download_met
      list(NA)
    },
    .package = "creatrajs"
  )

  suppressWarnings(
    creatrajs::trajs.compute(
      location_id = c("ambon_idn.19_1_id", "balikpapan_idn.15_1_id"),
      date_from = "2026-04-01",
      date_to = "2026-04-02",
      use_cache = FALSE,
      save_to_cache = FALSE,
      debug = FALSE
    )
  )

  expect_equal(weather_calls, 1)
  expect_true(all(unlist(download_met_flags) == FALSE))
})

test_that("trajs.get still prepares weather by default", {
  weather_calls <- 0
  fake_traj <- tibble::tibble(
    run = 1,
    hour_along = 0,
    traj_dt = as.POSIXct("2026-04-01 00:00:00", tz = "UTC"),
    traj_dt_i = as.POSIXct("2026-04-01 00:00:00", tz = "UTC"),
    lat = 0,
    lon = 0,
    height = 10
  )

  local_mocked_bindings(
    check_configuration = function() TRUE,
    download_weather = function(...) {
      weather_calls <<- weather_calls + 1
      invisible(NULL)
    },
    hysplit.trajs = function(...) fake_traj,
    .package = "creatrajs"
  )

  dates <- as.POSIXct(c("2026-04-01", "2026-04-02"), tz = "UTC")
  geometry <- sf::st_sfc(sf::st_point(c(100, 13)), crs = 4326)

  suppressWarnings(
    creatrajs::trajs.get(
      dates = dates,
      location_id = "test_location",
      geometry = geometry,
      met_type = "gdas1",
      height = 10,
      duration_hour = 96,
      use_cache = FALSE,
      save_to_cache = FALSE,
      complete_only = FALSE,
      debug = FALSE
    )
  )
  expect_equal(weather_calls, 1)

  weather_calls <- 0
  suppressWarnings(
    creatrajs::trajs.get(
      dates = dates,
      location_id = "test_location",
      geometry = geometry,
      met_type = "gdas1",
      height = 10,
      duration_hour = 96,
      use_cache = FALSE,
      save_to_cache = FALSE,
      download_met = FALSE,
      complete_only = FALSE,
      debug = FALSE
    )
  )
  expect_equal(weather_calls, 0)
})
