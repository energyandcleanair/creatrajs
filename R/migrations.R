##########################################################################
## Functions to update existing data to new structure e.g. adding a field
##########################################################################

migrations.fill_missing_fields <- function(start_from=1){
  library(creatrajs)
  found <- creatrajs::db.find_trajs(location_id=NULL)

  # filling hours field
  fs <- creatrajs::db.get_gridfs()
  fs.files <- db.get_gridfs_files()
  ids <- paste0("id:",found$id)

  trajs <- pbapply::pblapply(seq(start_from, nrow(found)), function(i){
    print(i)
    id <- found[i,]$id
    metadata <- jsonlite::parse_json(found[i,]$metadata)

    filepath <- tempfile()
    trajs <- tryCatch({
      fs$download(paste0("id:", id), filepath)
      return(readRDS(filepath))
    }, error=function(e){
      return(NULL)
    })

    if(is.null(trajs)){
      print("Failed to read trajs. Ignoring")
      return(NULL)
    }

    if(nrow(trajs)==1){
      fs$remove(paste0("id:", id))
      return(NULL)
    }

    heights <- unique(trajs[trajs$hour_along==0,]$height_i)


    if(length(heights)==1 && heights != (metadata$height)){
      fs.files$update(sprintf('{"_id": {"$oid": "%s"}}', id), sprintf('{"$set":{"metadata.height": %s}}', heights))
    }

    hours <- trajs %>%
      dplyr::filter(hour_along==0) %>%
      dplyr::mutate(diff=traj_dt-min(traj_dt)) %>%
      dplyr::pull(diff) %>%
      as.numeric(unit='hours') %>%
      paste0(collapse=",")

    if(is.null(metadata$hours)){
      fs.files$update(sprintf('{"_id": {"$oid": "%s"}}', id), sprintf('{"$set":{"metadata.hours": "%s"}}', hours))
    }

    # fs.files$find(sprintf('{"_id": {"$oid": "%s"}}', id))
    file.remove(filepath)
    return(trajs)
  })



}

#' Uplaod trajectories cached using previous system (i.e. on disk)
#'
#' @return
#' @export
#'
#' @examples
migrations.upload_filecached <- function(){
  library(creatrajs)
  paths <- list.files(utils.get_cache_folder("trajs"), full.names = T)
  names <- list.files(utils.get_cache_folder("trajs"))

  files <- tibble(name=basename(names), path=paths) %>%
    filter(stringr::str_detect(name, "gdas1")) %>%
    tidyr::separate(name, c("location_id", "details"), sep="\\.gdas1\\.") %>%
    tidyr::separate(details, c("height","duration_hour","date","shouldbeRDS"), sep="\\.") %>%
    filter(shouldbeRDS=="RDS")

  f$duration_hour <- as.numeric(f$duration_hour)
  files$size <- file.info(files$path)$size
  files$height <- as.numeric(files$height)
  files$date <- strptime(files$date, "%Y%m%d")
  files$met_type <- "gdas1"
  files <- files %>% filter(size>60)
  files <- files %>% filter(height==10)
  files <- files %>% filter(!location_id %in% c("3f21f025-610c-4471-8fec-0001718dd05e",
                                                "e710800e-41de-4c3d-ada8-6931b23d94da"))

  for(i in seq(nrow(files))){
    print(sprintf("%d/%d",i,nrow(files)))
    f <- files[i,]
    db.upload_trajs(trajs=readRDS(f$path),
                    date=f$date,
                    location_id=f$location_id,
                    met_type=f$met_type,
                    duration_hour=f$duration_hour,
                    height=f$height
    )
  }
}
