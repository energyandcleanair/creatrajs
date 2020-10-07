
map.trajs <- function(basemap, fires, trajs, region_id, source, date, value, unit, filename, powerplants, ..., add_plot=NULL){

  tryCatch({
    region_name <- tools::toTitleCase(region_id)
    source <- toupper(source)

    subtitle <- paste0(date,". PM2.5 level: ",round(value)," ",unit)
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
                 arrow = arrow(angle=18, length=unit(0.1,"inches")),
                 aes(x = lon, y = lat, group=subcluster), color="darkred", alpha=0.6) +

       geom_path(data = trajs,
                 aes(x = lon, y = lat, group=traj_dt_i), color="darkred", alpha=0.6) +

       # geom_line(data = trajs_meas %>% dplyr::filter(value>=threshold) , aes(x = lon, y = lat, group=traj_dt_i), alpha=0.6, color='darkred')+

       theme_crea() +
       theme(panel.background = element_rect(fill='lightgray'),
             panel.border = element_rect(color='black', fill=NA),
             panel.grid = element_line(color=NA),
             plot.caption = element_text(lineheight = 0.9),
             # legend.key = element_rect(fill='white'),
             legend.position = "bottom",
             legend.direction = "horizontal",
             legend.margin=margin(0,0,0,0),
             legend.box.margin=margin(-20,0,10,0)) +
       scale_shape_manual(name="Sector", values=c(0,1,2,3,4,5))+
       scale_color_manual(name=NULL, values=c("red","black"),
                          breaks=c("Active fire", "Thermal power plant"))+
       labs(title=paste0("Sources or air flowing into ", region_name),
            subtitle = subtitle,
            x='', y='',
            caption=paste0("CREA based on ",source, ", VIIRS and HYSPLIT.\nSize reflects the maximum fire intensity."))

    if(!is.null(fires) & nrow(fires %>% filter(!is.na(date.fire)))){

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
    }

    if(!is.null(add_plot)){
      m <- m + add_plot
    }

    filepath <- file.path(dir_results, paste0(filename,".jpg"))
    ggsave(plot=m, filename=filepath,
           width=8,
           height=7)



    return(filepath)
  }, error=function(c){
    warning(paste("Error on  ", region_id, date))
    return(NA)
  })
}



