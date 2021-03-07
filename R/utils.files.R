#' Data folder
#'
#' @return Local path to data folder
#'
#' @examples
utils.get_dir_data <- function(){
  suppressWarnings(try(readRenviron(".Renviron")))
  suppressWarnings(try(dotenv::load_dot_env()))

  dir_data <- Sys.getenv("DIR_DATA")
  if(dir_data==""){
    warning("DIR_DATA environment variable undefined. Using working directory.")
    dir_data = getwd()
  }
  return(dir_data)
}


#' Cache folder
#'
#' @param subfolder Subfolder (to create if not existing)
#' @return Local path to cache folder
#'
#' @export
utils.get_cache_folder <- function(subfolder=NULL){
  folder <- file.path(utils.get_dir_data(), "cache")

  if(!is.null(subfolder)){
    folder <- file.path(folder, subfolder)
  }

  if(!dir.exists(folder)) dir.create(folder, recursive = T)
  return(folder)
}


utils.get_firms_folder <- function(){
  suppressWarnings(try(readRenviron(".Renviron")))
  suppressWarnings(try(dotenv::load_dot_env()))

  d <- Sys.getenv("DIR_FIRMS")
  if(d==""){
    stop("DIR_FIRMS environment variable not defined")
  }
  return(d)
}

utils.get_firms_subfolder <- function(region="South_Asia"){
  d <- utils.fires.get_firms_folder()
  return(file.path(d, "suomi-npp-viirs-c2", region))
}
