####################################################
## Functions for data download and database handling
##
openDB <- function(db){
  # Opens a connection to the app sqlite database
  #
  # Arg:
  #   dbFile: The filename path of the .sqlite file
  #
  # Returns:
  #   A list with:
  #    db$con: SQLiteConnection
  #    ...
  #db <- list()
  #db$con <- DBI::dbConnect(RSQLite::SQLite(), dbFile)
  db$con <-DBI::dbConnect(db$pgdrv, dbname=db$name, host=db$host, port=db$port, user=db$user,password=db$password)

  db$metadata <- list()
  if (length(dbListTables(db$con)) == 0) {
    db$metadata$CreationDate <- date()
    ######dbWriteTable(db$con,"metadata",paste(toJSON(db$metadata)))
  } else {
    ###
  }
  ### To do: additional database checking here ###
  return(db)
}

closeDB <- function(db) {
  # Closes a connection to the app sqlite database
  #
  # Arg:
  #   db:   A list object created by openDB, including an SQLiteConnection in db$con
  #
  # Returns: none
  dbDisconnect(db$con)
}

getGaugeSites <- function(db) {
  # Downloads raingauge site metadata from ECan and saves as a database table, "gauges"
  #
  # Arg:
  #   db:   A list object created by openDB, including an SQLiteConnection in db$con
  #
  # Returns: none
  #
  # ECan rainfall monitoring sites are available from:
  # http://opendata.canterburymaps.govt.nz/datasets/482291bb562540888b1aec7b85919827_5

  # GeoJSON site location data:
  gaugeJSON <- fromJSON("https://opendata.arcgis.com/datasets/482291bb562540888b1aec7b85919827_5.geojson", flatten = TRUE)

  # Extract gauge data as a data frame, rename columns to simplify and remove "."
  gauges <- data.frame(gaugeJSON$features) %>%
    rename_at(vars(starts_with("properties.")), funs(str_replace(., "properties.", ""))) %>%
    rename_at(vars(starts_with("geometry.")), funs(str_replace(., "geometry.", "geo_")))

  # [Shouldn't be needed with Postgres version] Convert point coordiates to real numbers and drop geo column
  geo <- pull(gauges,geo_coordinates) %>% unlist
  gauges$geo_lon <- geo[seq(1, length(geo), 2)]
  gauges$geo_lat <- geo[seq(2, length(geo), 2)]
  # gauges <- gauges %>% select(-geo_coordinates)

  # Remove rows with no samples. Convert LAST_SAMPLE to Date-Time.
  gauges <- gauges %>% filter(!is.na(LAST_SAMPLE))
  gauges$LAST_SAMPLE <- gauges$LAST_SAMPLE %>%
    ymd_hms() %>% force_tz(., tzone = "Pacific/Auckland")

  # Write to database table
  dbWriteTable(db$con, "gauges", gauges, overwrite = TRUE)
}

getSiteData <- function(siteID,timePeriod = "All") {
  # Get an invidual site's data for a requested period
  #
  # Arg:
  #   siteID:     An integer identifier of the site
  #   timePeriod: The timeperiod to download (string). Default: "All"
  #
  # Returns: status code (0 = fail, 1 = success)

  print(paste0("Downloading: ", siteID))

  siteURL <- paste0("http://data.ecan.govt.nz/data/78/Rainfall/Rainfall%20for%20individual%20site/CSV?SiteNo=",siteID,"&Period=",timePeriod)

  # Download site data
  r <- httr::GET(siteURL)

  # Parse CSV string into table
  siteData <- read.table(text = content(r,"text"), sep =",", header = TRUE, stringsAsFactors = FALSE)

  # Handle empty sites: skip to next
  if (nrow(siteData) == 0) {
    print(paste0("  -> no data"))
    status <- 0
  } else {

    # Process date strings into R date-times
    siteData$DateTime <- gsub("[.]", "\\1", siteData$DateTime) %>%
      strptime(., format = "%d/%m/%Y %I:%M:%S %p", tz = 'Pacific/Auckland') %>%
      as.POSIXct(.)

    # Replace spuriously high values (>1000) with NA (the NZ 24-hour record is 758)
    siteData <- siteData %>%
      mutate(RainfallTotal = replace(RainfallTotal, RainfallTotal > 1000, NA))

    # If All data downloaded, overwrite table
    dbWriteTable(db$con, as.character(siteID), siteData, overwrite = TRUE)

    # Otherwise, append, then remove duplicates

    # Handle hourly data - write to separate tables


    status <- 1
  }
  return(status)
}

updateData <- function(db) {
  # Updates all data, downloading using getSiteData()
  #
  # Arg:
  #   db:   A list object created by openDB, including an SQLiteConnection in db$con
  #
  # Returns: none

  gauges <- dbReadTable(db$con,"gauges")
  tblList <- dbListTables(db$con)

  # Process for all sites
  for (siteID in gauges$SITENUMBER) {
    getSiteData(siteID, timePeriod = "All")

    # if (siteID %in% tblList) {
    #   # Download latest data
    #
    # } else {
    #   # Download all data
    #   getSiteData(siteID, timePeriod = "All")
    # }
  }

}





