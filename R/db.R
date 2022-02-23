db.get_collection <- function(collection_name){
  readRenviron(".Renviron")
  connection_string=Sys.getenv("CREA_MONGODB_URL")
  mongolite::mongo(collection=collection_name, db="creatrajs", url=connection_string)
}

db.get_gridfs <- function(){
  readRenviron(".Renviron")
  connection_string=Sys.getenv("CREA_MONGODB_URL")
  gridfs(db="creatrajs", prefix="trajectories", url=connection_string)
}

db.get_trajs_collection <- function(){
  mongo.get_collection("trajectories")
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
