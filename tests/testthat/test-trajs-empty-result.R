test_that("zero-row trajectory frames are treated as failed computations", {
  empty <- data.frame(
    traj_dt = as.POSIXct(character(), tz = "UTC"),
    lat = numeric(),
    lon = numeric()
  )

  expect_true(creatrajs:::trajs.is_empty_result(empty))
  expect_true(creatrajs:::trajs.is_empty_result(tibble::as_tibble(empty)))
})

test_that("other empty result shapes are detected", {
  expect_true(creatrajs:::trajs.is_empty_result(NULL))
  expect_true(creatrajs:::trajs.is_empty_result(NA))
  expect_true(creatrajs:::trajs.is_empty_result(list()))
})

test_that("a populated trajectory frame is not empty", {
  result <- data.frame(
    traj_dt = as.POSIXct("2026-09-23 00:00:00", tz = "UTC"),
    lat = 39.953,
    lon = 116.466
  )

  expect_false(creatrajs:::trajs.is_empty_result(result))
})

test_that("db.upload_trajs rejects an empty frame before opening GridFS", {
  empty <- data.frame(traj_dt = as.POSIXct(character(), tz = "UTC"))

  expect_warning(
    expect_false(creatrajs:::db.upload_trajs(
      trajs = empty,
      location_id = "example",
      met_type = "gdas1",
      height = 10,
      duration_hour = 96,
      hours = c(0, 4, 8, 12),
      date = as.Date("2026-09-23")
    )),
    "Refusing to upload an empty trajectory result"
  )
})
