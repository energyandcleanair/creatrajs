test_that("building trajectories work", {


  require(rcrea)
  require(testthat)

  tmp_folder <- tempdir()

  date_from <- "2021-03-10"
  date_to <- "2021-03-14"

  t <- creatrajs::process(city ="beijing",
                     poll="pm10",
                     fires = NULL,
                     powerplants=NULL,
                     source="mee",
                     date_from=date_from,
                     date_to=date_to,
                     met_type="gdas1",
                     duration_hour=72,
                     add_fires=F,
                     height=500,
                     radius_km=200,
                     buffer_km=200,
                     zoom_level=8,
                     upload_results = F,
                     folder=tmp_folder
  )

  fs <- list.files(tmp_folder, "*.jpg")
  # One map per day
  test_equal(length(fs),
             date_from-date_to)

})
