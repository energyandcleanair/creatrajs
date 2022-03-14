test_that("getting fire works", {

  require(rcrea)
  require(testthat)

  date_from <- lubridate::today() - 10
  date_to <- lubridate::today() - 5

  creatrajs::fire.download(date_from=date_from, date_to=date_to)

  f <- creatrajs::fire.read(
    date_from=date_from,
    date_to=date_to,
    region="Global",
    extent.sp=NULL)

  expect_gt(nrow(f), 190000)


  # Testing that extent filtering works as well

  # Getting Delhi region box
  m <- rcrea::measurements(city="Delhi",country="IN",
                           date_from=date_from,
                           date_to=date_to,
                           poll="pm25",
                           with_geometry = T)

  extent.sp <- m$geometry[[1]] %>%
    sf::st_buffer(1) %>%
    as("Spatial")

  f.delhi <- creatrajs::fire.read(date_from=date_from,
                                 date_to=date_to,
                                 region="Global",
                                 extent.sp=extent.sp)

  expect_gt(nrow(f.delhi), 500)
  expect_lt(nrow(f.delhi), 1000)
})



test_that("attaching fire - trajectories. Both vector and raster", {

  library(rcrea)
  library(testthat)
  library(tictoc)
  library(tidyverse)

  date_from <- lubridate::today() - 10
  date_to <- lubridate::today() - 5

  dates <- seq(as.Date(date_from), as.Date(date_to), by="day")
  buffer_km <- 50

  l <- rcrea::locations(city="Delhi", with_geometry=T)
  m <- rcrea::measurements(city="Delhi",
                           poll="pm25",
                           source="cpcb",
                           date_from = date_from,
                           date_to = date_to,
                           process_id="city_day_mad",
                           with_geometry=T
  )

  creatrajs::fire.download(
    date_from=date_from,
    date_to=date_to,
    region="Global")

  expect_equal(nrow(m),
               as.numeric(date_to-date_from, unit="days")+1)


  # Get trajectories
  mt <- m %>%
    rowwise() %>%
    mutate(trajs=creatrajs::trajs.get(dates=date,
                            geometry = unique(m$geometry),
                            location_id = unique(m$location_id),
                            met_type = "gdas1",
                            height = 10,
                            duration_hour = 120,
                            hours = c(0,3,6), #seq(0,23)
                           ))


  # Attach fire: vector method
  tic()
  mtf <- creatrajs::fire.attach_to_trajs(mt, buffer_km=buffer_km, split_days=F)
  expect_equal(
    lapply(mtf$fires, names) %>% unlist() %>% unique() %>% sort(),
    c("fire_count", "fire_frp"))
  names(mtf$fires[[1]])


  toc()

  tic()
  mtf <- creatrajs::fire.attach_to_trajs(mt, buffer_km=buffer_km, split_days=T)
  expect_equal(
    lapply(mtf$fires, names) %>% unlist() %>% unique() %>% sort(),
    c("fire_count_dayminus0", "fire_count_dayminus1", "fire_count_dayminus2", "fire_count_dayminus3",
      "fire_frp_dayminus0", "fire_frp_dayminus1", "fire_frp_dayminus2", "fire_frp_dayminus3")
    )
  toc()

  # Attach fire when trajectories are wrong
  tic()
  mt_onetraj <- mt %>% rowwise() %>% mutate(trajs=list(head(trajs, 1)))
  mtf_onetraj <- creatrajs::fire.attach_to_trajs(mt_onetraj, buffer_km=buffer_km)
  toc()

  # A larger one
  mt_large <- lapply(seq(1,20), function(d) mt %>% mutate(date=date - d*(max(date)-min(date)))) %>%
    do.call(bind_rows, .)

  tic()
  mtf <- creatrajs::fire.attach_to_trajs(mt_large, buffer_km=buffer_km)
  toc()


  expect_equal(nrow(mt), nrow(mtf))
  expect_true("fires" %in% names(mtf))
  expect_gt(mtf$fires[[1]]$fire_count, 0)
  expect_gt(mtf$fires[[1]]$fire_frp, 0)



  # Attach fire: raster stack method
  tic()
  rs <- mt %>%
    rowwise() %>%
    mutate(trajs_rs=list(creatrajs::trajs.to_rasterstack(trajs, buffer_km=buffer_km, res_deg=0.1)))
  toc()
  tic()
  rsf <- creatrajs::fire.attach_to_trajs_rs(rs)
  toc()



  # Visual confirmation
  i=2
  ti.sf <- mtf$trajs[[i]] %>% sf::st_as_sf(coords=c("lon","lat"))
  ggplot(ti.sf) + geom_sf(aes(color=lubridate::date(traj_dt))) + facet_wrap(~run, ncol=3)
  raster::plot(rs$trajs_rs[[i]], ext=sf::st_bbox(ti.sf))
  raster::plot(subset(rs$trajs_rs[[i]],1), ext=sf::st_bbox(ti.sf))
  raster::plot(subset(rs$trajs_rs[[i]],2), ext=sf::st_bbox(ti.sf), add=T)
  raster::plot(subset(rs$trajs_rs[[i]],3), ext=sf::st_bbox(ti.sf), add=T)
  raster::plot(subset(rs$trajs_rs[[i]],4), ext=sf::st_bbox(ti.sf), add=T)
  raster::plot(as(ti.sf, "Spatial"), add=T)


  bind_rows(
    mtf %>% tidyr::unnest(fires) %>% mutate(version="vector"),
    rsf %>% tidyr::unnest(fires) %>% mutate(version="raster")
  ) %>%
    ggplot() +
    geom_bar(aes(date, fire_count, fill=version),
             stat="identity",
             position="dodge")


})


test_that("attaching fire with split_regions", {

  library(rcrea)
  library(testthat)
  library(tictoc)
  library(tidyverse)

  date_from  <- "2020-01-05"
  date_to  <- "2020-01-10"
  dates <- seq(as.Date(date_from), as.Date(date_to), by="day")
  buffer_km <- 50

  l <- rcrea::locations(city="Delhi", with_geometry=T)

  creatrajs::fire.download(
    date_from=as.Date(date_from)-1,
    date_to=date_to,
    region="Global")


  # Get trajectories
  trajs <- creatrajs::trajs.get(dates=dates,
                                geometry = l$geometry,
                                location_id = l$id,
                                met_type = "gdas1",
                                height = 10,
                                duration_hour = 120,
                                hours = c(0,3,6)) #seq(0,23)

  # Attach fire: vector method
  tic()
  mtf <- creatrajs::fire.attach_to_trajs(mt, buffer_km=buffer_km, split_days=F, split_regions="gadm_0")
  expect_equal(
    lapply(mtf$fires, names) %>% unlist() %>% unique() %>% sort(),
    c("fire_count", "fire_frp"))
  names(mtf$fires[[1]])


  toc()

  tic()
  mtf <- creatrajs::fire.attach_to_trajs(mt, buffer_km=buffer_km, split_days=T)
  expect_equal(
    lapply(mtf$fires, names) %>% unlist() %>% unique() %>% sort(),
    c("fire_count_dayminus0", "fire_count_dayminus1", "fire_count_dayminus2", "fire_count_dayminus3",
      "fire_frp_dayminus0", "fire_frp_dayminus1", "fire_frp_dayminus2", "fire_frp_dayminus3")
  )
  toc()

  # Attach fire when trajectories are wrong
  tic()
  mt_onetraj <- mt %>% rowwise() %>% mutate(trajs=list(head(trajs, 1)))
  mtf_onetraj <- creatrajs::fire.attach_to_trajs(mt_onetraj, buffer_km=buffer_km)
  toc()

  # A larger one
  mt_large <- lapply(seq(1,20), function(d) mt %>% mutate(date=date - d*(max(date)-min(date)))) %>%
    do.call(bind_rows, .)

  tic()
  mtf <- creatrajs::fire.attach_to_trajs(mt_large, buffer_km=buffer_km)
  toc()


  expect_equal(nrow(mt), nrow(mtf))
  expect_true("fires" %in% names(mtf))
  expect_gt(mtf$fires[[1]]$fire_count, 0)
  expect_gt(mtf$fires[[1]]$fire_frp, 0)



  # Attach fire: raster stack method
  tic()
  rs <- mt %>%
    rowwise() %>%
    mutate(trajs_rs=list(creatrajs::trajs.to_rasterstack(trajs, buffer_km=buffer_km, res_deg=0.1)))
  toc()
  tic()
  rsf <- creatrajs::fire.attach_to_trajs_rs(rs)
  toc()



  # Visual confirmation
  i=2
  ti.sf <- mtf$trajs[[i]] %>% sf::st_as_sf(coords=c("lon","lat"))
  ggplot(ti.sf) + geom_sf(aes(color=lubridate::date(traj_dt))) + facet_wrap(~run, ncol=3)
  raster::plot(rs$trajs_rs[[i]], ext=sf::st_bbox(ti.sf))
  raster::plot(subset(rs$trajs_rs[[i]],1), ext=sf::st_bbox(ti.sf))
  raster::plot(subset(rs$trajs_rs[[i]],2), ext=sf::st_bbox(ti.sf), add=T)
  raster::plot(subset(rs$trajs_rs[[i]],3), ext=sf::st_bbox(ti.sf), add=T)
  raster::plot(subset(rs$trajs_rs[[i]],4), ext=sf::st_bbox(ti.sf), add=T)
  raster::plot(as(ti.sf, "Spatial"), add=T)


  bind_rows(
    mtf %>% tidyr::unnest(fires) %>% mutate(version="vector"),
    rsf %>% tidyr::unnest(fires) %>% mutate(version="raster")
  ) %>%
    ggplot() +
    geom_bar(aes(date, fire_count, fill=version),
             stat="identity",
             position="dodge")


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
