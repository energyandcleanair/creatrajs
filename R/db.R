db.get_collection <- function(collection_name){
  readRenviron(".Renviron")
  connection_string=Sys.getenv("CREA_MONGODB_URL")
  mongolite::mongo(collection=collection_name, db="creatrajs", url=connection_string)
}

db.get_trajs_collection <- function(){
  mongo.get_collection("trajectories")
}



db.create_index <- function(collection_name, columns, index_name, unique=T){
  cmd <- sprintf('{"createIndexes":"%s",
        "indexes":[{"key":{%s},
        "name":"%s","unique": %s}]}',
                 collection_name,
                 paste(sprintf("\"%s\":1",columns), collapse=","),
                 index_name,
                 ifelse(unique, "true","false"))

  m <- mongo.get_collection(collection_name)
  m$run(cmd)
}


db.setup_db <- function(){
  mongo.create_index(collection_name="trajectories",
                     columns=c("location_id","date","duration_hour","height"),
                     index_name="trajs_unique_index",
                     unique=T)
}



db.upload_trajs <- function(trajs,
                            location_id, met_type, height, duration_hour, date){
  fs <- gridfs(db="creatrajs", prefix="trajectories", url=connection_string)
  tmpdir <- tempdir()
  filepath <- file.path(tmpdir, "trajs.RDS")
  saveRDS(trajs, filepath)

  metadata <- list(location_id=location_id,
                   duration_hour=duration_hour,
                   height=height,
                   met_type=met_type,
                   date=date,
                   format="rds") %>%
    jsonlite::toJSON(metadata, auto_unbox=T)

  fs$upload(path, name = basename(path), content_type=NULL, metadata=metadata)
}

db.remove_trajs <- function(location_id, met_type, height, duration_hour, date, format="rds"){
  fs <- gridfs(db="creatrajs", prefix="trajectories", url=connection_string)

  metadata <- list(metadata=list(location_id=location_id,
                   duration_hour=duration_hour,
                   height=height,
                   met_type=met_type,
                   date=date,
                   format=format)) %>%
    jsonlite::toJSON(metadata, auto_unbox=T)

  fs$find(metadata)
}
