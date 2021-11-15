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



test_that("attaching fire - trajectories. Both vector and raster", {

  library(rcrea)
  library(testthat)
  library(tictoc)
  library(tidyverse)

  date_from  <- "2020-01-05"
  date_to  <- "2020-01-10"
  buffer_km <- 10

  m <- rcrea::measurements(city="Delhi",
                           poll="pm25",
                           source="cpcb",
                           date_from = date_from,
                           date_to = date_to,
                           process_id="city_day_mad",
                           with_geometry=T
  )

  creatrajs::fire.download(
    date_from=as.Date(date_from)-1,
    date_to=date_to,
    region="Global")

  expect_equal(nrow(m),
               as.numeric(as.Date(date_to)-as.Date(date_from)+1, unit="days"))

  # Get trajectories
  mt <- m %>%
    rowwise() %>%
    mutate(trajs=
      creatrajs::trajs.get(dates=date,
                            geometry = geometry,
                            location_id = location_id,
                            met_type = "gdas1",
                            heights = 500,
                            duration_hour = 72,
                            hours = c(0,3,6), #seq(0,23)
                            # cache_folder = utils.get_cache_folder("trajs")
                           )
      )

  # Attach fire: vector method
  tic()
  mtf <- creatrajs::fire.attach_to_trajs(mt, buffer_km=buffer_km)
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


test_that("summarising fires", {

  library(creahelpers)

  g <- creahelpers::get_adm(level=1, iso2s="IN")
  date_from <- "2021-11-01"
  date_to <- "2021-11-03"



