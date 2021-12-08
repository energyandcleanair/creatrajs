#' Pseudo-function to import \strong{dplyr}'s common functions.
#'
#' @importFrom dplyr select rename mutate mutate_at filter filter_at arrange distinct
#'     summarise summarise_at summarize do group_by group_by_at ungroup do left_join inner_join everything bind_rows
#'     pull tibble as_tibble rowwise any_vars all_vars vars collect full_join n intersect starts_with select_at %>%
NULL

#' Pseudo-function to import \strong{tibble}'s common functions.
#'
#' @importFrom tibble tibble
NULL


# ' Pseudo-function to import sf functions
#'
#' @importFrom sf st_as_sf st_coordinates st_as_sfc st_buffer st_union st_centroid st_bbox st_intersection
NULL

# ' Pseudo-function to import sp functions
#'
#' @importFrom sp proj4string proj4string<- CRS SpatialPointsDataFrame SpatialPolygonsDataFrame
NULL
