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

db.get_gridfs_files <- function(){
  readRenviron(".Renviron")
  connection_string=Sys.getenv("CREA_MONGODB_URL")
  mongolite::mongo(db="creatrajs", collection="trajectories.files", url=connection_string)
}

db.get_unique_columns <- function(){
  c("location_id","date","duration_hour", "height", "met_type", "format", "hours")
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
                            location_id,
                            met_type,
                            height,
                            duration_hour,
                            hours,
                            date,
                            silent=T){


  # Check format
  ok <- T
  ok <- ok & is.data.frame(trajs)

  fs <- db.get_gridfs()
  tmpdir <- tempdir()
  filepath <- file.path(tmpdir, "trajs.RDS")
  saveRDS(trajs, filepath)
  date <- strftime(as.Date(date),"%Y-%m-%d")

  hours <- if(all(is.null(hours)) || all(is.na(hours))) NULL else {paste0(hours, collapse=',')}
  height <- if(all(is.null(height)) || all(is.na(height))) NULL else {height}

  metadata <- list(location_id=location_id,
                   duration_hour=duration_hour,
                   height=height,
                   met_type=met_type,
                   date=date,
                   hours=hours,
                   format="rds")

  # Remove first if exists
  filter <- metadata[db.get_unique_columns()]
  names(filter) <- paste0("metadata.", names(filter))
  found <- fs$find(jsonlite::toJSON(filter,auto_unbox=T))
  if(nrow(found)>0){
    if(!silent) print("Trajs already exist. Replacing them")
    fs$remove(paste0("id:", found$id))
  }
  # And then upload
  fs$upload(filepath, name=basename(filepath), content_type=NULL,
            metadata=jsonlite::toJSON(metadata, auto_unbox=T))
}


db.find_trajs <- function(location_id, met_type=NULL, height=NULL, duration_hour=NULL, date=NULL, hours=NULL, format="rds"){
  fs <- db.get_gridfs()

  hours <- if(all(is.null(hours)) || all(is.na(hours))) NULL else {paste0(hours, collapse=',')}
  height <- if(all(is.null(height)) || all(is.na(height))) NULL else {height}

  filter <- list(metadata.location_id=location_id,
                   metadata.duration_hour=duration_hour,
                   metadata.hours=hours,
                   metadata.height=height,
                   metadata.met_type=met_type,
                   metadata.date=date,
                   metadata.format=format)

  filter <- filter[!unlist(lapply(filter, is.null))]
  fs$find(jsonlite::toJSON(filter,auto_unbox=T))
}


db.remove_trajs <- function(location_id, met_type=NULL, height=NULL, duration_hour=NULL, date=NULL, hours=NULL, format="rds"){
  fs <- db.get_gridfs()
  found <- db.find_trajs(location_id=location_id, met_type=met_type, height=height,
                         duration_hour=duration_hour, date=date, hours=hours, format=format)

  if(nrow(found)>0) fs$remove(paste0("id:", found$id))
  print(sprintf("%d row(s) removed", nrow(found)))
}


db.available_dates <- function(location_id, met_type, height, duration_hour, hours=NULL, date=NULL, format="rds", min_size=500){

  found <- db.find_trajs(location_id=location_id, met_type=met_type, height=height,
                         duration_hour=duration_hour, hours=hours, format=format, date=date)

  # Filter throws error because of date
  found <- found[found$size > min_size,]

  dates <- unlist(lapply(found$metadata, function(x) jsonlite::fromJSON(x)$date)) %>%
    as.Date()
  return(dates)
}


db.download_trajs <- function(location_id=NULL, met_type=NULL, height=NULL, duration_hour=NULL,
                              hours=NULL, date=NULL, format="rds", min_size=500){
  fs <- db.get_gridfs()
  found <- db.find_trajs(location_id=location_id, met_type=met_type, height=height,
                         duration_hour=duration_hour, hours=hours, date=date, format=format)

  # Filter throws error because of date
  found <- found[found$size > min_size,]

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

#' Remove trajs whose parameters differ from given parameters
#'
#' @param location_id
#' @param hours
#' @param format
#'
#' @return
#' @export
#'
#' @examples
db.remove_trajs <- function(location_id=NULL, hours=NULL, duration_hour=NULL, format="rds"){
  fs <- db.get_gridfs()
  found <- db.find_trajs(location_id=location_id)
  to_remove <- c()

  if(!is.null(hours)){
    hours <- lapply(found$metadata, function(x) tryCatch({jsonlite::fromJSON(x)$hours}, error=function(e){return(NA)}))
    hours <- lapply(hours, function(x){if(is.null(x)) NA else x})
    found$hours <- unlist(hours)
    hours_str <- if(!is.character(hours)) paste0(hours, collapse=",") else hours
    to_remove_hours <- found %>%
      filter(hours != !!hours_str) %>%
      pull(id)

    to_remove <- c(to_remove, to_remove_hours)
  }

  if(!is.null(duration_hour)){
    duration_hour <- lapply(found$metadata, function(x) tryCatch({jsonlite::fromJSON(x)$duration_hour}, error=function(e){return(NA)}))
    duration_hour <- lapply(duration_hour, function(x){if(is.null(x)) NA else x})
    found$duration_hour <- unlist(duration_hours)

    to_remove_duration_hour <- found %>%
      filter(duration_hour != !!duration_hour) %>%
      pull(id)

    to_remove <- c(to_remove, to_remove_duration_hour)
  }

  if(length(to_remove) >0) pbapply::pblapply(to_remove, function(x) fs$remove(paste0("id:", x)))
  print(sprintf("%d row(s) removed", to_remove))
}

