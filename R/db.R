db.get_collection <- function(collection_name){
  readRenviron(".Renviron")
  connection_string=Sys.getenv("CREA_MONGODB_URL")
  mongolite::mongo(collection=collection_name, db="creatrajs", url=connection_string)
}

db.get_gridfs <- function(){
  readRenviron(".Renviron")
  connection_string=Sys.getenv("CREA_MONGODB_URL")
  mongolite::gridfs(db="creatrajs", prefix="trajectories", url=connection_string)
}


db.get_unique_columns <- function(){
  c("location_id","date","duration_hour", "height", "met_type", "format")
}


db.create_index <- function(collection_name, columns, index_name, unique=T){
  cmd <- sprintf('{"createIndexes":"%s",
        "indexes":[{"key":{%s},
        "name":"%s","unique": %s}]}',
                 collection_name,
                 paste(sprintf("\"%s\":1",columns), collapse=","),
                 index_name,
                 ifelse(unique, "true","false"))

  m <- db.get_collection(collection_name)
  m$run(cmd)
}


db.setup_db <- function(){
  db.create_index(collection_name="trajectories.files",
                     columns=paste0("metadata.", db.get_unique_columns()),
                     index_name="trajs_unique_index",
                     unique=T)
}


db.upload_trajs <- function(trajs,
                            location_id, met_type, height, duration_hour, date){
  fs <- db.get_gridfs()
  tmpdir <- tempdir()
  filepath <- file.path(tmpdir, "trajs.RDS")
  saveRDS(trajs, filepath)
  date <- strftime(as.Date(date),"%Y-%m-%d")

  metadata <- list(location_id=location_id,
                   duration_hour=duration_hour,
                   height=height,
                   met_type=met_type,
                   date=date,
                   format="rds")

  # Remove first if exists
  filter <- metadata[db.get_unique_columns()]
  names(filter) <- paste0("metadata.", names(filter))
  found <- fs$find(jsonlite::toJSON(filter,auto_unbox=T))
  if(nrow(found)>0){
    print("Trajs already exist. Replacing them")
    fs$remove(paste0("id:", found$id))
  }

  # And then upload
  fs$upload(filepath, name=basename(filepath), content_type=NULL,
            metadata=jsonlite::toJSON(metadata, auto_unbox=T))
}


db.find_trajs <- function(location_id, met_type=NULL, height=NULL, duration_hour=NULL, date=NULL, format="rds"){
  fs <- db.get_gridfs()

  filter <- list(metadata.location_id=location_id,
                   metadata.duration_hour=duration_hour,
                   metadata.height=height,
                   metadata.met_type=met_type,
                   metadata.date=date,
                   metadata.format=format)

  filter <- filter[!unlist(lapply(filter, is.null))]
  fs$find(jsonlite::toJSON(filter,auto_unbox=T))
}


db.remove_trajs <- function(location_id, met_type=NULL, height=NULL, duration_hour=NULL, date=NULL, format="rds"){
  fs <- db.get_gridfs()
  found <- db.find_trajs(location_id=location_id, met_type=met_type, height=height, duration_hour=duration_hour, date=date, format=format)

  if(nrow(found)>0) fs$remove(paste0("id:", found$id))
  print(sprintf("%d row(s) removed", nrow(found)))
}


db.available_dates <- function(location_id, met_type, height, duration_hour, format="rds"){
  found <- db.find_trajs(location_id=location_id, met_type=met_type, height=height, duration_hour=duration_hour, format=format)
  return(found$date)
}


db.download_trajs <- function(location_id=NULL, met_type=NULL, height=NULL, duration_hour=NULL, date=NULL, format="rds"){
  fs <- db.get_gridfs()
  found <- db.find_trajs(location_id=location_id, met_type=met_type, height=height, duration_hour=duration_hour, date=date, format=format)

  if(nrow(found)==0) return(NULL)

  result <- lapply(found$metadata, function(x) as.data.frame(jsonlite::fromJSON(x))) %>%
    do.call(bind_rows, .)

  ids <- paste0("id:",found$id)
  trajs <- lapply(ids, function(id){
    filepath <- tempfile()
    fs$download(id, filepath)
    trajs <- readRDS(filepath)
    file.remove(filepath)
    return(trajs)
  })

  result$trajs <- trajs
  tibble(result)
}



#' Uplaod trajectories cached using previous system (i.e. on disk)
#'
#' @return
#' @export
#'
#' @examples
db.upload_filecached <- function(){
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

