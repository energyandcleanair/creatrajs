test_that("getting fire works", {

  library(rcrea)
  library(testthat)
  library(tictoc)

  # Pick a fire season to have enough fires to test performance
  date_from <- "2022-10-01"
  date_to <- "2022-10-30"

  creatrajs::fire.download(date_from=date_from, date_to=date_to)

  tic()
  f_sf <- creatrajs::fire.read(
    date_from=date_from,
    date_to=date_to,
    region="Global",
    extent.sp=NULL)
  toc()

  tic()
  f_sp <- creatrajs::fire.read(
    date_from=date_from,
    date_to=date_to,
    sf_or_sp = "sp",
    region="Global",
    extent.sp=NULL)
  toc()

  expect_gt(nrow(f), 190000)



  # Testing that extent filtering works as well

  # Getting Delhi region box
  m <- rcrea::measurements(city="Delhi",country="IN",
                           date_from=date_from,
                           date_to=date_to,
                           poll="pm25",
                           with_geometry = T)

  extent.sp <- m$geometry[[1]] %>%
    sf::st_buffer(2) %>%
    as("Spatial")

  tic()
  f.delhi <- creatrajs::fire.read(date_from=date_from,
                                 date_to=date_to,
                                 region="Global",
                                 extent.sp=extent.sp
                                 )
  toc()

  expect_gt(nrow(f.delhi), 500)
  expect_lt(nrow(f.delhi), 1000)
})



test_that("attaching fire - trajectories. Both vector and raster", {

  library(rcrea)
  library(testthat)
  library(tictoc)
  library(tidyverse)

  date_from <- lubridate::today() - 20
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

  # Should be recent enough to have fire
  # old enough to have weather
  date_from  <- "2022-10-05"
  date_to  <-"2022-10-15"
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
                                hours = c(6)) #seq(0,23)

  mt = tibble(location_id=unique(l$id),
              date=dates,
              trajs=trajs)

  # Attach fire
  library(profvis)

  tic()
  profvis({
    mtf_0_sf <- creatrajs::fire.attach_to_trajs(mt, buffer_km=buffer_km, split_days=F, split_regions="gadm_0", parallel=F, sf_or_sp = "sf")
  })
  toc()

  tic()
  profvis({
    mtf_0_sf <- creatrajs::fire.attach_to_trajs(mt, buffer_km=buffer_km, split_days=F, split_regions="gadm_0", adm_res='high', parallel=F, sf_or_sp = "sf")
  })
  toc()

  tic()
  profvis({
    mtf_0_sp <- creatrajs::fire.attach_to_trajs(mt, buffer_km=buffer_km, split_days=F, split_regions="gadm_0", parallel=F, sf_or_sp = "sp")
  })
  toc()

  profvis({
    mtf_1 <- creatrajs::fire.attach_to_trajs(mt, buffer_km=buffer_km, split_days=F, split_regions="gadm_1", parallel=F)
  })

  profvis({
    mtf_2 <- creatrajs::fire.attach_to_trajs(mt, buffer_km=buffer_km, split_days=F, split_regions="gadm_2", parallel=F)
  })

  fire_comparison <- bind_rows(
    mtf_0 %>% mutate(level='0'),
    mtf_1 %>% mutate(level='1'),
    mtf_2 %>% mutate(level='2')) %>%
  tidyr::unnest(fires) %>%
    select_if(grepl("date|level|fire_.*", names(.))) %>%
    tidyr::pivot_longer(cols=-c(level, date)) %>%
    mutate(name=stringr::str_extract(name, '(fire_[^\\_]*)')) %>%
    group_by(date, level, name) %>%
    summarise(value=sum(value, na.rm=T)) %>%
    pivot_wider(names_from=level, values_from=value, names_prefix='value_')

  expect_equal(fire_comparison$value_0, fire_comparison$value_1)
  expect_equal(fire_comparison$value_0, fire_comparison$value_2)

  ggplot(fire_comparison) +
    geom_line(aes(date, value, col=level)) +
    facet_wrap(~name + level)


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
