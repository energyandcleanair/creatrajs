check_configuration <- function(){
  check_libgfortran()
}

#' Check that libgfortran 3 is installed
#' (Only works on linux)
#'
#' @return
#' @export
#'
#' @examples
check_libgfortran <- function() {
  sysname <- Sys.info()["sysname"]
  if(sysname == "Linux"){
    result <- suppressWarnings(system("ldconfig -p | grep libgfortran.so.3", intern = TRUE))

    if (length(result) > 0) {
      # message("libgfortran.so.3 is installed.")
      return(TRUE)
    } else {
      stop("libgfortran.so.3 is NOT installed. HYSPLIT is not likely to run.")
      return(FALSE)
    }
  }
}


