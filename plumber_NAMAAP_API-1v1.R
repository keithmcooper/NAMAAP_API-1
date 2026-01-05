# plumber.R

# =============================================================================
# Dependencies
# =============================================================================
# Load packages
library(plumber)
library(pool)
library(DBI)
library(RPostgres)
library(glue)
library(jsonlite)
library(config)


# =============================================================================
# API Metadata
# =============================================================================
#* @apiTitle OneBenthic Dredging API
#* @apiDescription Endpoints to query dredged polygons by year and country.
#* @apiVersion 1.0.0

# =============================================================================
# Configuration & DB Pool
# =============================================================================
Sys.setenv(R_CONFIG_ACTIVE = "ices_wgext")
dw <- config::get()

pg_pool <- dbPool(
  drv      = RPostgres::Postgres(),
  dbname   = dw$database,
  host     = dw$server,
  port     = dw$port,
  user     = dw$uid,
  password = dw$pwd,
  minSize  = 1,
  maxSize  = 10
)

# =============================================================================
# Helpers
# =============================================================================

validate_year <- function(year_vec) {
  y <- suppressWarnings(as.integer(year_vec))
  if (any(is.na(y))) {
    stop(plumber::error_rs("Invalid 'year' parameter. Provide integer(s) (e.g., 2014,2015)."), call. = FALSE)
  }
  y
}

# QGIS-safe FeatureCollection
make_feature_collection <- function(df) {
  if (nrow(df) == 0) {
    return(list(type = "FeatureCollection", features = list()))
  }
  
  flatten_coords <- function(coords) {
    if (is.list(coords) && all(sapply(coords, length) == 1)) {
      return(unlist(coords))
    } else if (is.list(coords)) {
      return(lapply(coords, flatten_coords))
    } else {
      return(coords)
    }
  }
  
  features <- lapply(seq_len(nrow(df)), function(i) {
    geom_json <- df$geometry[[i]]
    if (is.null(geom_json) || geom_json == "") return(NULL)
    
    geom <- jsonlite::fromJSON(geom_json, simplifyVector = TRUE)
    geom$coordinates <- flatten_coords(geom$coordinates)
    geom$type <- as.character(geom$type)
    
    list(
      type = "Feature",
      properties = list(
        id = as.integer(df$id[[i]]),
        year = as.integer(df$year[[i]]),
        country_countryname = as.character(df$country_countryname[[i]])
      ),
      geometry = geom
    )
  })
  
  features <- features[!sapply(features, is.null)]
  list(type = "FeatureCollection", features = features)
}

# =============================================================================
# Routes
# =============================================================================

#* List available year–country combinations
#* @get /availability
#* @serializer json
function(res) {
  
  sql <- "
    SELECT DISTINCT
      year,
      country_countryname
    FROM areas.dredged_polygon_footprint_mv
    WHERE geom IS NOT NULL
    ORDER BY year, country_countryname;
  "
  
  dat <- DBI::dbGetQuery(pg_pool, sql)
  
  res$setHeader("Cache-Control", "max-age=300, public")
  dat
}

#* Get dredging polygons as QGIS-ready GeoJSON
#* @param year:string Comma-separated years (required)
#* @param country:string Optional comma-separated countries
#* @get /dredging
function(year, country, res) {
  
  if (missing(year) || is.null(year) || !nzchar(year)) {
    stop(plumber::error_rs("Parameter 'year' is required"), call. = FALSE)
  }
  
  # Split comma-separated years
  years <- validate_year(unlist(strsplit(year, ",")))
  
  # Split comma-separated countries if provided
  countries <- NULL
  if (!missing(country) && nzchar(country)) {
    country <- URLdecode(country)              # decode spaces (%20)
    countries <- trimws(unlist(strsplit(country, ",")))  # trim whitespace
  }
  
  # Build SQL with IN clauses
  if (!is.null(countries) && length(countries) > 0) {
    sql <- glue::glue_sql("
      SELECT gid, year, country_countryname,
             ST_AsGeoJSON(ST_Transform(geom, 4326), 6) AS geometry
      FROM areas.dredged_polygon_footprint_mv
      WHERE year IN ({years*}) AND country_countryname IN ({countries*}) AND geom IS NOT NULL;
    ", years = years, countries = countries, .con = pg_pool)
  } else {
    sql <- glue::glue_sql("
      SELECT gid, year, country_countryname,
             ST_AsGeoJSON(ST_Transform(geom, 4326), 6) AS geometry
      FROM areas.dredged_polygon_footprint_mv
      WHERE year IN ({years*}) AND geom IS NOT NULL;
    ", years = years, .con = pg_pool)
  }
  
  dat <- DBI::dbGetQuery(pg_pool, sql)
  dat <- dat[!is.na(dat$geometry) & dat$geometry != "", ]
  
  fc <- make_feature_collection(dat)
  
  res$setHeader("Content-Type", "application/geo+json; charset=utf-8")
  res$setHeader("Cache-Control", "max-age=120, public")
  res$body <- jsonlite::toJSON(fc, auto_unbox = TRUE, pretty = TRUE)
  res
}

# =============================================================================
# Plumber hook: close pool on shutdown
# =============================================================================
#* @plumber
function(pr) {
  pr$registerHooks(list(
    exit = function() {
      if (!is.null(pg_pool)) {
        try(pool::poolClose(pg_pool), silent = TRUE)
      }
    }
  ))
  pr
}

