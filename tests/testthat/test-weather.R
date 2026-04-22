if (!exists("gdas1_read_header_date")) {
  source(file.path("..", "..", "R", "weather.R"))
}

test_that("gdas1 header date is parsed correctly", {
  tmpdir <- tempfile("gdas-weather-")
  dir.create(tmpdir, recursive = TRUE)
  test_file <- file.path(tmpdir, "current7days")

  con <- file(test_file, "wb")
  writeBin(charToRaw("26 4 15 0 0 099INDX"), con)
  close(con)

  parsed <- gdas1_read_header_date(test_file)
  expect_equal(parsed, as.Date("2026-04-15"))
})

test_that("current7days maps to week file based on header date", {
  tmpdir <- tempfile("gdas-weather-")
  dir.create(tmpdir, recursive = TRUE)
  current_file <- file.path(tmpdir, "current7days")

  con <- file(current_file, "wb")
  writeBin(charToRaw("26 4 15 0 0 099INDX"), con)
  close(con)

  gdas1_map_current7days_to_week(tmpdir)

  mapped <- file.path(tmpdir, "gdas1.apr26.w3")
  expect_true(file.exists(mapped))
  expect_gt(file.info(mapped)$size, 0)
})

test_that("existing target week file is not overwritten", {
  tmpdir <- tempfile("gdas-weather-")
  dir.create(tmpdir, recursive = TRUE)
  current_file <- file.path(tmpdir, "current7days")
  target_file <- file.path(tmpdir, "gdas1.apr26.w3")

  con_current <- file(current_file, "wb")
  writeBin(charToRaw("26 4 15 0 0 099INDX current"), con_current)
  close(con_current)

  con_target <- file(target_file, "wb")
  writeBin(charToRaw("existing-target-content"), con_target)
  close(con_target)

  before <- readBin(target_file, what = "raw", n = 64)
  gdas1_map_current7days_to_week(tmpdir)
  after <- readBin(target_file, what = "raw", n = 64)

  expect_equal(after, before)
})
