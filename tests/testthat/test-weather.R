if (!exists("gdas1_read_header_date")) {
  source(file.path("..", "..", "R", "weather.R"))
}

# gdas1/ARL index labels are fixed-width: each of YY, MM, DD, HH is 2 chars,
# right-justified and space-padded. e.g. 2026-06-01 -> "26 6 1", 2026-06-15 ->
# "26 615", 2025-10-15 -> "251015". These helpers build *real* bytes so the
# tests can't drift back to the space-delimited form that silently misparsed
# two-digit days in production.
fw_header <- function(y2, m, d){
  sprintf("%2d%2d%2d 0 0 099INDX", y2, m, d)
}
write_met <- function(path, y2, m, d, n_bytes = 0){
  raw <- charToRaw(fw_header(y2, m, d))
  if(n_bytes > length(raw)){
    raw <- c(raw, as.raw(rep(0, n_bytes - length(raw))))
  }
  con <- file(path, "wb"); writeBin(raw, con); close(con)
}
new_dir <- function(){
  d <- tempfile("gdas-weather-"); dir.create(d, recursive = TRUE); d
}
# small enough that any non-empty test file counts as a "full week"
FULL_MIN <- 4L

test_that("gdas1 header date parses fixed-width fields (regression backstop)", {
  # Exact NOAA byte layouts observed from the archive. Locks the field parsing
  # so a future whitespace-split regression (which broke two-digit days) fails.
  tmpdir <- new_dir()
  cases <- list(
    c("26 6 1 0 0 099INDX", "2026-06-01"),  # 1-digit month, 1-digit day
    c("26 615 0 0 099INDX", "2026-06-15"),  # 1-digit month, 2-digit day  <- the bug
    c("251015 0 0 099INDX", "2025-10-15"),  # 2-digit month, 2-digit day
    c("2512 1 0 0 099INDX", "2025-12-01")   # 2-digit month, 1-digit day
  )
  for(cc in cases){
    f <- file.path(tmpdir, "current7days")
    con <- file(f, "wb"); writeBin(charToRaw(cc[[1]]), con); close(con)
    expect_equal(gdas1_read_header_date(f), as.Date(cc[[2]]))
  }
})

test_that("current7days with a two-digit header day maps (was a silent no-op)", {
  # Pre-fix this returned NA and mapped nothing for ~3 weeks of every month.
  tmpdir <- new_dir()
  write_met(file.path(tmpdir, "current7days"), 26, 6, 15)  # 2026-06-15 -> w3
  gdas1_map_current7days_to_week(tmpdir, full_week_min_bytes = FULL_MIN)
  expect_true(file.exists(file.path(tmpdir, "gdas1.jun26.w3")))
})

test_that("a full current7days inside one week maps only that week", {
  # Oldest day 15 is a week-start, so the 7-day span 06-15..06-21 stays in w3.
  tmpdir <- new_dir()
  write_met(file.path(tmpdir, "current7days"), 26, 6, 15)
  gdas1_map_current7days_to_week(tmpdir, full_week_min_bytes = FULL_MIN)
  expect_true(file.exists(file.path(tmpdir, "gdas1.jun26.w3")))
  expect_false(file.exists(file.path(tmpdir, "gdas1.jun26.w4")))
})

test_that("current7days straddling a week boundary maps both weeks", {
  # Oldest day 16 => span 06-16..06-22 covers w3 (15-21) and w4 (22-28). w4 holds
  # the most recent date and has no real file yet, so it must be created.
  tmpdir <- new_dir()
  write_met(file.path(tmpdir, "current7days"), 26, 6, 16)
  gdas1_map_current7days_to_week(tmpdir, full_week_min_bytes = FULL_MIN)
  expect_true(file.exists(file.path(tmpdir, "gdas1.jun26.w3")))
  expect_true(file.exists(file.path(tmpdir, "gdas1.jun26.w4")))
})

test_that("a short/partial current7days does not fabricate the next week", {
  # Same straddling header, but the file is below the full-week size, so we must
  # not create a w4 the file doesn't actually fully contain.
  tmpdir <- new_dir()
  write_met(file.path(tmpdir, "current7days"), 26, 6, 16, n_bytes = 18)
  gdas1_map_current7days_to_week(tmpdir, full_week_min_bytes = 1e9)
  expect_true(file.exists(file.path(tmpdir, "gdas1.jun26.w3")))
  expect_false(file.exists(file.path(tmpdir, "gdas1.jun26.w4")))
})

test_that("a real weekly file (header on the week start) is never overwritten", {
  # w3's real file has header 06-15 (== week start); current7days at 06-16 must
  # leave it alone but still fill the empty w4.
  tmpdir <- new_dir()
  write_met(file.path(tmpdir, "current7days"), 26, 6, 16)
  w3 <- file.path(tmpdir, "gdas1.jun26.w3")
  write_met(w3, 26, 6, 15, n_bytes = 40)

  before <- readBin(w3, what = "raw", n = 64)
  gdas1_map_current7days_to_week(tmpdir, full_week_min_bytes = FULL_MIN)
  after <- readBin(w3, what = "raw", n = 64)

  expect_equal(after, before)
  expect_true(file.exists(file.path(tmpdir, "gdas1.jun26.w4")))
})

test_that("a stale current7days copy in the in-progress week is refreshed", {
  # w4 holds an older current7days copy (header 06-16, off the 06-22 week start).
  # A newer current7days (06-17) must overwrite it so the week tracks forward.
  tmpdir <- new_dir()
  write_met(file.path(tmpdir, "current7days"), 26, 6, 17)
  w4 <- file.path(tmpdir, "gdas1.jun26.w4")
  write_met(w4, 26, 6, 16)

  gdas1_map_current7days_to_week(tmpdir, full_week_min_bytes = FULL_MIN)

  expect_equal(gdas1_read_header_date(w4), as.Date("2026-06-17"))
})

test_that("an equally-fresh current7days copy is not rewritten", {
  # Same header on both -> nothing to gain; leave the existing file untouched.
  tmpdir <- new_dir()
  write_met(file.path(tmpdir, "current7days"), 26, 6, 16)
  w4 <- file.path(tmpdir, "gdas1.jun26.w4")
  write_met(w4, 26, 6, 16, n_bytes = 40)

  before <- readBin(w4, what = "raw", n = 64)
  gdas1_map_current7days_to_week(tmpdir, full_week_min_bytes = FULL_MIN)
  after <- readBin(w4, what = "raw", n = 64)

  expect_equal(after, before)
})

test_that("current7days spanning a month boundary maps both months", {
  # Oldest day 05-29 => span 05-29..06-04 covers may w5 and jun w1.
  tmpdir <- new_dir()
  write_met(file.path(tmpdir, "current7days"), 26, 5, 29)
  gdas1_map_current7days_to_week(tmpdir, full_week_min_bytes = FULL_MIN)
  expect_true(file.exists(file.path(tmpdir, "gdas1.may26.w5")))
  expect_true(file.exists(file.path(tmpdir, "gdas1.jun26.w1")))
})
