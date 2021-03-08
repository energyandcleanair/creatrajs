test_that("getting fire works", {

  require(rcrea)
  require(testthat)

  creatrajs::fire.download(date_from="2020-05-05", date_to="2020-05-10")

  f <- creatrajs::fire.read(
    date_from="2020-05-05",
    date_to="2020-05-10",
    region="Global",
    extent.sp=NULL)

  expect_gt(nrow(f), 190000)


  # Testing that extent filtering works as well

  # Getting Delhi region box
  m <- rcrea::measurements(city="Delhi",country="IN",
                           date_from="2020-01-05",
                           date_to="2020-01-05",
                           poll="pm25",
                           with_geometry = T)

  extent.sp <- m$geometry[[1]] %>%
    sf::st_buffer(1) %>%
    as("Spatial")

  f.delhi <- creatrajs::fire.read(date_from="2020-05-05",
                                 date_to="2020-05-10",
                                 region="Global",
                                 extent.sp=extent.sp)

  expect_gt(nrow(f.delhi), 500)
  expect_lt(nrow(f.delhi), 1000)
})



test_that("attaching fire - trajectories", {

  require(rcrea)
  require(testthat)
  m <- rcrea::measurements(city="Lahore",
                           poll="pm25",
                           source="openaq_government",
                           date_from = "2020-01-05",
                           date_to = "2020-01-07",
                           process_id="city_day_mad",
                           with_geometry=T
  )

  expect_equal(nrow(m), 3)

  # Get trajectories
  mt <- m %>%
    rowwise() %>%
    mutate(trajs=list(
      creatrajs::trajs.get(dates=date,
                            geometry = geometry,
                            location_id = location_id,
                            met_type = "gdas1",
                            heights = 500,
                            duration_hour = 72,
                            cache_folder = utils.get_cache_folder("trajs")
                           )
      ))

  # Attach fire
  mtf <- fire.attach_to_trajs(mt, buffer_km=10)

  expect_gt(mtf$fires[[1]]$fire_count, 0)
  expect_gt(mtf$fires[[1]]$fire_frp, 0)
})



test_that("attaching fire - circular", {

  require(rcrea)
  require(testthat)
  m <- rcrea::measurements(city="Lahore",
                           poll="pm25",
                           source="openaq_government",
                           date_from = "2020-01-05",
                           date_to = "2020-01-07",
                           process_id="city_day_mad",
                           with_geometry=T
  )

  expect_equal(nrow(m), 3)

  # Build circle
  mt <- m %>%
    rowwise() %>%
    mutate(extent=
      trajs.circular_extent(geometry=geometry,
                                       buffer_km=200)
      )

  # Attach fire
  mtf <- fire.attach_to_extents(mt, delay_hour=72)

  expect_gt(mtf$fires[[1]]$fire_count, 0)
  expect_gt(mtf$fires[[1]]$fire_frp, 0)
})
