# Unit tests for trajs.drop_failed_rows() — regression for a stored Beijing file
# (2026-06-05) whose all-NA stub row broke pyreadr on read and 500'd the API.

# Frame shaped like splitr output: one good run + one all-NA stub row (run 6).
make_trajs <- function(){
  good <- data.frame(
    run = 1L,
    hour_along = 0:-2,
    traj_dt = as.POSIXct(c("2026-06-05 00:00", "2026-06-04 23:00", "2026-06-04 22:00"), tz = "UTC"),
    lat = c(39.95, 40.0, 40.1),
    lon = c(116.47, 116.44, 116.41),
    height = c(10, 250, 480),
    traj_dt_i = as.POSIXct("2026-06-05 00:00", tz = "UTC"),
    lat_i = 39.95, lon_i = 116.47, height_i = 10,
    stringsAsFactors = FALSE
  )
  stub <- data.frame(
    run = 6L,
    hour_along = NA_integer_,
    traj_dt = as.POSIXct(NA, tz = "UTC"),
    lat = NA_real_,
    lon = NA_real_,
    height = NA_real_,
    traj_dt_i = as.POSIXct(NA, tz = "UTC"),
    lat_i = 39.953352, lon_i = 116.466258, height_i = 10,
    stringsAsFactors = FALSE
  )
  rbind(good, stub)
}

test_that("drops the all-NA stub row and keeps valid rows", {
  out <- creatrajs:::trajs.drop_failed_rows(make_trajs())

  expect_equal(nrow(out), 3)
  expect_false(any(is.na(out$traj_dt)))
  expect_false(any(is.na(out$lat)))
  expect_false(any(is.na(out$lon)))
  expect_equal(sort(unique(out$run)), 1L)
})

test_that("a fully-failed run is removed entirely", {
  t <- make_trajs()
  t <- t[t$run == 6L, , drop = FALSE] # only the stub
  out <- creatrajs:::trajs.drop_failed_rows(t)

  expect_equal(nrow(out), 0)
  expect_true(is.data.frame(out))
})

test_that("a clean frame is returned unchanged", {
  t <- make_trajs()
  t <- t[t$run == 1L, , drop = FALSE]
  out <- creatrajs:::trajs.drop_failed_rows(t)

  expect_equal(nrow(out), 3)
  expect_equal(out, t)
})

test_that("drops a row with NA datetime even if lat/lon are present", {
  # Defensive: the datetime is the column that breaks pyreadr, so a row must be
  # dropped on NA traj_dt regardless of whether coordinates happen to be set.
  t <- make_trajs()[1:1, ]
  t$traj_dt <- as.POSIXct(NA, tz = "UTC")
  out <- creatrajs:::trajs.drop_failed_rows(t)

  expect_equal(nrow(out), 0)
})

test_that("handles NA / NULL / empty inputs gracefully", {
  expect_true(is.na(creatrajs:::trajs.drop_failed_rows(NA)))
  expect_null(creatrajs:::trajs.drop_failed_rows(NULL))

  empty <- make_trajs()[0, ]
  out <- creatrajs:::trajs.drop_failed_rows(empty)
  expect_equal(nrow(out), 0)
  expect_true(is.data.frame(out))
})
