test_that("FIRMS file names follow each satellite's naming", {
  expect_equal(fire.nrt_filename("suomi-npp-viirs-c2", "Global", as.Date("2018-01-04")),
               "SUOMI_VIIRS_C2_Global_VNP14IMGTDL_NRT_2018004.txt")
  expect_equal(fire.nrt_filename("noaa-20-viirs-c2", "Global", as.Date("2018-01-05")),
               "J1_VIIRS_C2_Global_VJ114IMGTDL_NRT_2018005.txt")
})


test_that("dates switch from Suomi NPP to NOAA-20 on FIRMS_NOAA20_FROM", {
  expect_equal(fire.satellite_for_date(as.Date(c("2018-01-04", "2018-01-05", "2018-01-06"))),
               c("suomi-npp-viirs-c2", "noaa-20-viirs-c2", "noaa-20-viirs-c2"))
})


test_that("fire.read takes each date from one satellite only", {
  dir <- withr::local_tempdir()
  withr::local_envvar(DIR_FIRMS = dir)

  write_fires <- function(satellite, date, frp){
    folder <- utils.get_firms_subfolder(region="Global", satellite=satellite)
    dir.create(folder, recursive = TRUE, showWarnings = FALSE)
    data.table::fwrite(
      data.frame(latitude=10, longitude=100, acq_date=as.character(date), acq_time="0600", version="2.0NRT", frp=frp),
      file.path(folder, fire.nrt_filename(satellite, "Global", date)))
  }
  # Both satellites have files around the switch
  for(date in c("2018-01-04", "2018-01-05")){
    write_fires("suomi-npp-viirs-c2", as.Date(date), frp=1)
    write_fires("noaa-20-viirs-c2", as.Date(date), frp=20)
  }
  write_fires("noaa-20-viirs-c2", as.Date("2018-01-06"), frp=20)

  fires <- fire.read(date_from="2018-01-04", date_to="2018-01-06", sf_or_sp="sf", parallel=FALSE) %>%
    dplyr::arrange(acq_date)

  expect_equal(as.character(fires$acq_date), c("2018-01-04", "2018-01-05", "2018-01-06"))
  expect_equal(fires$frp, c(1, 20, 20))
})
