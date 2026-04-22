check_configuration <- function(){
  check_libgfortran()
  check_splitr_hyts_binary()
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

check_splitr_hyts_binary <- function() {
  sysname <- Sys.info()[["sysname"]]
  if (!identical(sysname, "Darwin")) {
    return(invisible(TRUE))
  }

  binary_path <- splitr:::set_binary_path(binary_path = NULL, binary_name = "hyts_std")
  if (!file.exists(binary_path)) {
    stop(sprintf("splitr hyts_std binary not found at '%s'.", binary_path))
  }

  file_info <- tryCatch(
    system2("file", c("-b", binary_path), stdout = TRUE, stderr = TRUE),
    error = function(e) character(0)
  )
  machine <- Sys.info()[["machine"]]
  is_arm_machine <- grepl("arm64|aarch64", machine, ignore.case = TRUE)
  binary_is_x86_only <- length(file_info) > 0 &&
    any(grepl("x86_64", file_info, ignore.case = TRUE)) &&
    !any(grepl("arm64|aarch64", file_info, ignore.case = TRUE))

  if (is_arm_machine && binary_is_x86_only) {
    stop(
      paste(
        "splitr hyts_std binary is x86_64-only, but this R session is running on Apple Silicon.",
        "This prevents trajectory execution and can look like '0 trajectories'.",
        "Run under Rosetta x86_64 R or provide an arm64-compatible HYSPLIT binary via splitr's binary_path."
      )
    )
  }

  invisible(TRUE)
}


