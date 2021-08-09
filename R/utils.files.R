#' Data folder
#'
#' @return Local path to data folder
#'
#' @examples
#' @export
utils.get_dir_data <- function(){
  suppressWarnings(try(readRenviron(".Renviron"), silent = T))
  suppressWarnings(try(dotenv::load_dot_env(), silent = T))

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


#' Firms folder
#'
#' @return
#' @export
#'
#' @examples
utils.get_firms_folder <- function(){
  suppressWarnings(try(readRenviron(".Renviron"), silent = T))
  suppressWarnings(try(dotenv::load_dot_env(), silent = T))

  d <- Sys.getenv("DIR_FIRMS")
  if(d==""){
    stop("DIR_FIRMS environment variable not defined")
  }
  return(d)
}


#' Region subfolder of Firms folder
#'
#' @param region
#'
#' @return
#' @export
#'
#' @examples
utils.get_firms_subfolder <- function(region="Global"){
  d <- utils.get_firms_folder()
  return(file.path(d, "suomi-npp-viirs-c2", region))
}


#' GFAS folder
#'
#' @param region
#'
#' @return
#' @export
#'
#' @examples
utils.get_gfas_folder <- function(){
  suppressWarnings(try(readRenviron(".Renviron"), silent = T))
  suppressWarnings(try(dotenv::load_dot_env(), silent = T))

  d <- Sys.getenv("DIR_DATA")
  if(d==""){
    stop("DIR_DATA environment variable not defined")
  }

  return(file.path(d, "gfas"))
}

