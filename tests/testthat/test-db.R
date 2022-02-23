test_that("building trajectories work", {


  require(rcrea)
  require(testthat)

  l <- rcrea::cities(name="Bangkok", with_geometry=T)
  date <- "2020-06-01"
  met_type <- "gdas1"
  height <- 10
  duration_hour <- 120
  location_id <- "test_bangkok_tha.3_1_th"

  # Clean first
  db.remove_trajs(location_id=location_id)

  # Without cache
  t <- creatrajs::trajs.get(dates=date,
                 geometry = l$geometry,
                 location_id = l$location_id,
                 met_type = met_type,
                 heights = height,
                 duration_hour = duration_hour,
                 cache_folder = NULL
                 )

  trajs <- t[[1]]
  existing <- db.find_trajs(location_id=location_id)
  expect_true(nrow(existing)==0)


  # Upload
  db.upload_trajs(trajs=trajs,
                  date=date,
                  location_id=location_id,
                  met_type=met_type,
                  duration_hour=duration_hour,
                  height=height)

  existing <- db.find_trajs(location_id=location_id)
  expect_true(nrow(existing)==1)

  # Uplaod same should replace it
  db.upload_trajs(trajs=trajs,
                  date=date,
                  location_id=location_id,
                  met_type=met_type,
                  duration_hour=duration_hour,
                  height=height)

  existing <- db.find_trajs(location_id=location_id)
  expect_true(nrow(existing)==1)

  db.upload_trajs(trajs=trajs,
                  date=date,
                  location_id=location_id,
                  met_type=met_type,
                  duration_hour=duration_hour+1,
                  height=height)

  existing <- db.find_trajs(location_id=location_id)
  expect_true(nrow(existing)==2)

  downloaded <- db.download_trajs(location_id,
                                 met_type=met_type,
                                 height=height,
                                 date=date)
  expect_true(nrow(downloaded)==2)
  expect_equal(
    sort(unique(downloaded$duration_hour)),
    c(duration_hour, duration_hour+1))
})
