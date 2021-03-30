

#' Map trajectories
#'
#' @param trajs
#' @param location_id
#' @param location_name
#' @param date
#' @param meas
#' @param filename
#' @param met_type
#' @param duration_hour
#' @param height
#' @param fires
#' @param basemap
#' @param add_fires
#' @param fire_raster
#' @param powerplants
#' @param add_plot
#' @param folder
#' @param ...
#'
#' @return
#' @export
#'
#' @examples
map.trajs <- function(trajs, location_id, location_name, date,
                      meas, filename, met_type, duration_hour, height,
                      fires=NULL, basemap=NULL,
                      add_fires=F, fire_raster=NULL, powerplants=NULL, add_plot=NULL, ...){

  if(!is.null(powerplants)){
    powerplants$geometry <- st_centroid(powerplants$geometry)
  }

  tryCatch({
    source_legend <- if(!is.null(meas)) paste0(rcrea::source_str(unique(meas$source)), collapse=",") else ""


    # For powerplants and active fires
    dot_values <- c()
    dot_colors <- c()

    # Get measurements values in subtitle
    subtitle_poll <- ifelse(!is.null(meas),
                            paste0(rcrea::poll_str(meas$poll)," level: ",round(meas$value)," ",meas$unit,collapse=". "),
                            "")

    subtitle <- paste0(date, ". ", subtitle_poll)

    m <- ggmap(basemap) +
       coord_cartesian() +
       # geom_point(data=wri_power %>% dplyr::filter(country=="IDN"), inherit.aes = F, aes(x=longitude,y=latitude),
       #            shape=2, stroke=1.5, color='darkred') +
       # geom_point(data=ct %>% dplyr::filter(country=="Indonesia"), inherit.aes = F, aes(x=lng,y=lat),
       #            shape=2, stroke=1.5, color='darkred') +

       # Cluster trajectories
       geom_path(data = trajs %>%
                   dplyr::arrange(hour_along) %>%
                   mutate(subcluster=paste(traj_dt_i, hour_along %/% 8)),
                 arrow = ggplot2::arrow(angle=18, length=ggplot2::unit(0.1,"inches")),
                 aes(x = lon, y = lat, group=subcluster), color="darkred", alpha=0.6) +

       geom_path(data = trajs,
                 aes(x = lon, y = lat, group=traj_dt_i), color="darkred", alpha=0.6) +

       # geom_line(data = trajs_meas %>% dplyr::filter(value>=threshold) , aes(x = lon, y = lat, group=traj_dt_i), alpha=0.6, color='darkred')+

       rcrea::theme_crea() +
       ggplot2::theme(panel.background = element_rect(fill='lightgray'),
             panel.border = element_rect(color='black', fill=NA),
             panel.grid = element_line(color=NA),
             plot.caption = element_text(lineheight = 0.9),
             # legend.key = element_rect(fill='white'),
             legend.position = "bottom",
             legend.direction = "horizontal",
             legend.margin=margin(0,0,0,0),
             legend.box.margin=margin(-20,0,10,0)) +
       scale_shape_manual(name="Sector", values=c(0,1,2,3,4,5)) +
       labs(title=paste0("Sources of air flowing into ", location_name),
            subtitle = subtitle,
            x='', y='',
            caption=paste0("CREA based on ",source_legend, ", VIIRS and HYSPLIT.\nSize reflects the maximum fire intensity.\n",
                           "HYSPLIT parameters: ", duration_hour,"h | ",met_type," | ",height,"m." ))

    if(add_fires){

     if(!is.null(fire_raster)){
      bb <- ggmap::bb2bbox(attr(basemap, "bb"))
      bb <- as.numeric(bb)
      names(bb) <- c("xmin","ymin","xmax","ymax")
      crop <- sf::st_as_sfc(st_bbox(bb)) %>% sf::st_set_crs(4326) %>% sf::st_transform(crs=attr(fire_raster,"crs"))
      r.cropped <- raster::crop(fire_raster, as(crop, 'Spatial'))

      # r.cropped.max <- calc(r.cropped, function(x) max(x, na.rm = TRUE))
      fire_pol <- do.call("rbind", lapply(unstack(r.cropped),
                                          FUN=function(x){tryCatch({p <- rasterToPolygons(x); names(p)="fire"; p},
                                                                   error=function(c){NULL})}))


      if(!is.null(fire_pol)){
        m <- m + geom_sf(data=st_as_sf(fire_pol),
                         inherit.aes = F,
                         fill="blue",
                         color="blue")
      }
     }

      if(nrow(fires %>% filter(!is.na(acq_date)))>0){
        frp.min <- 0
        frp.max <- 8

        fires$frp <- min(fires$frp, frp.max)
        fires$frp <- max(fires$frp, frp.min)

        m <- m + geom_point(data=fires, inherit.aes = F,
                            aes(x=st_coordinates(geometry.fire)[,1],
                                y=st_coordinates(geometry.fire)[,2],
                                size=frp,
                                color="Active fire"),
                            fill="orange",
                            shape="triangle",
                            stroke=1,
                            alpha=0.8,
                            position="jitter") +
          scale_size_continuous(range=c(1,9), limits=c(frp.min, frp.max), guide="none")

        dot_values <- c(dot_values, "Active fire")
        dot_colors <- c(dot_colors, "red")
      }
    }

    if(!is.null(powerplants)){

      m <- m + geom_point(data=powerplants, inherit.aes = F,
                          aes(x=st_coordinates(st_centroid(geometry))[,1],
                              y=st_coordinates(st_centroid(geometry))[,2],
                              size=frp,
                              color="Thermal power plant"),
                          shape="triangle",
                          size=2,
                          stroke=1,
                          alpha=0.8,
                          position="jitter", show.legend = T)

      dot_values <- c(dot_values, "Thermal power plant")
      dot_colors <- c(dot_colors, "black")
    }

    m <- m +
      scale_color_manual(name=NULL,
                         values=dot_colors,
                         breaks=dot_values)

    if(!is.null(add_plot)){
      m <- m + add_plot
    }


    ggsave(plot=m,
           filename = filename,
           width=8,
           height=7)

    return(filename)
  }, error=function(c){
    warning(paste("Error on  ", location_id, date))
    return(NA)
  })
}



