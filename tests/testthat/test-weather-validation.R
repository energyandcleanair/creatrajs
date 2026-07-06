if (!exists("gdas1_file_is_valid")) {
  source(file.path("..", "..", "R", "weather.R"))
}

# Build a gdas1-like met file as `n_periods` fixed-length blocks, each opening
# with a real ARL index label ("...INDX") so gdas1_period_stride finds the
# per-period stride exactly as it does on NOAA files. `extra_bytes` appends
# trailing junk to mimic a wget -c resume that ran past a record boundary (the
# gdas1.jun26.w4 corruption: an over-size file that is no longer a whole number
# of periods).
write_periods <- function(path, n_periods, stride, extra_bytes = 0,
                          y2 = 26, m = 6, d = 15){
  label <- charToRaw(sprintf("%2d%2d%2d 0 0 099INDX", y2, m, d))
  stopifnot(stride >= length(label))
  block <- c(label, as.raw(rep(0L, stride - length(label))))
  con <- file(path, "wb")
  on.exit(close(con))
  for(i in seq_len(n_periods)){
    writeBin(block, con)
  }
  if(extra_bytes > 0){
    writeBin(as.raw(rep(1L, extra_bytes)), con)
  }
}

STRIDE <- 512L     # tiny stand-in for the ~10.9 MB real period size
FLOOR_OFF <- 0     # bypass the MB size floor to exercise the structural checks

test_that("gdas1_period_stride measures the gap between index records", {
  f <- tempfile()
  write_periods(f, n_periods = 4, stride = STRIDE)
  expect_equal(gdas1_period_stride(f), STRIDE)
})

test_that("gdas1_period_stride returns NA when it can't find two index records", {
  f <- tempfile()
  write_periods(f, n_periods = 1, stride = STRIDE)
  expect_true(is.na(gdas1_period_stride(f)))
})

test_that("a clean, record-aligned file is valid", {
  f <- tempfile()
  write_periods(f, n_periods = 8, stride = STRIDE)
  expect_true(gdas1_file_is_valid(f, "gdas1.jun26.w3", min_size_mb = FLOOR_OFF))
})

test_that("an over-size, misaligned file (resume/append corruption) is rejected", {
  # 8 clean periods + a partial trailing record: the size is no longer a whole
  # multiple of the stride -- exactly the gdas1.jun26.w4 corruption signature.
  f <- tempfile()
  write_periods(f, n_periods = 8, stride = STRIDE, extra_bytes = STRIDE %/% 3)
  expect_false(gdas1_file_is_valid(f, "gdas1.jun26.w3", min_size_mb = FLOOR_OFF))
})

test_that("an aligned file with more periods than a week can hold is rejected", {
  # A duplicated download can stay stride-aligned; a full week is at most 56
  # periods, so a larger period count is still corrupt.
  f <- tempfile()
  write_periods(f, n_periods = GDAS1_MAX_PERIODS + 1L, stride = STRIDE)
  expect_false(gdas1_file_is_valid(f, "gdas1.jun26.w3", min_size_mb = FLOOR_OFF))
})

test_that("a below-floor file is rejected on size alone", {
  # Real thresholds are hundreds of MB; a 2 KB file can't clear the w-file floor.
  f <- tempfile()
  write_periods(f, n_periods = 4, stride = STRIDE)
  expect_false(gdas1_file_is_valid(f, "gdas1.jun26.w3"))
})

test_that("a structurally-unintrospectable file falls back to the size floor", {
  # A single index record => stride unmeasurable => trust the (satisfied) floor
  # rather than delete a file we couldn't introspect.
  f <- tempfile()
  write_periods(f, n_periods = 1, stride = STRIDE)
  expect_true(gdas1_file_is_valid(f, "gdas1.jun26.w3", min_size_mb = FLOOR_OFF))
})

test_that("a missing file is invalid", {
  expect_false(gdas1_file_is_valid(tempfile(), "gdas1.jun26.w3", min_size_mb = FLOOR_OFF))
})
