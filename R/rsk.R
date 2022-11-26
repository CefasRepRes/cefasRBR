#' Read RBR Profiler data
#'
#' @param filename path to the .rsk file
#'
#' @import data.table
#' @return cefasRSK object
#' @export
#'
read.rsk <- function(filename){
  options(digits.secs = 3) # RBR instruments have sub-second time resolution
  if(!file.exists(filename)){stop(paste(filename, "does not exist"))}
  con <- DBI::dbConnect(RSQLite::SQLite(), dbname = filename)

  dbInfo <- RSQLite::dbReadTable(con, "dbInfo")
  if(dbInfo$type[1] != "EPdesktop"){warning("only tested with EPdesktop RSK files")}

  suppressWarnings({
    instrument = data.table::setDT(DBI::dbReadTable(con, "instruments"))
    sensors = data.table::setDT(DBI::dbReadTable(con, "instrumentSensors"))
  })

  events = data.table::setDT(DBI::dbReadTable(con, "events"))
  events = merge(events, rbr_event_codes, by.x = "type", by.y = "Event")[order(tstamp)]

  errors = setDT(DBI::dbReadTable(con, "errors"))
  errors = merge(errors, rbr_error_codes, by.x = "type", by.y = "Error")
  errors[, channelName := paste0("channel", formatC(channelOrder, 1, format = "d", flag = "0"))]

  channels = setDT(DBI::dbReadTable(con, "channels"))
  channels = merge.data.table(channels, rbr_channels[,.(Type, Description)], by.x = "shortName", by.y = "Type", all.x = T)
  channels[, channelName := paste0("channel", formatC(channelID, 1, format = "d", flag = "0"))]

  region_query = DBI::dbSendQuery(con, "
                                  SELECT
                                  region.regionID as regionID,
                                  type, tstamp1, tstamp2, label, latitude, longitude, refValue, refunit
                                  from region
                                  LEFT JOIN regionGeoData ON region.regionID = regionGeoData.regionID
                                  LEFT JOIN regionPlateau ON region.regionID = regionPlateau.regionID
                                  ")
  regions = data.table::setDT(DBI::dbFetch(region_query))
  DBI::dbClearResult(region_query)

  sample_period = RSQLite::dbReadTable(con, "continuous")$samplingPeriod / 1000

  fields = DBI::dbListFields(con, "data") # data names
  fields = fields[!grepl("tstamp", fields)]
  sql_fields = paste(c("tstamp", fields), collapse = ",")
  sql_fields = paste("SELECT", sql_fields, "FROM data")
  data_query = DBI::dbSendQuery(con, paste(sql_fields,  ";"))
  data = data.table::setDT(DBI::dbFetch(data_query))
  DBI::dbClearResult(data_query)
  data[, dateTime := as.POSIXct(tstamp/1000, origin = "1970-01-01", tz = "UTC")]
  data.table::setnames(data, channels$channelName, channels$shortName, skip_absent = T)

  ret = list() # Initialise return list containing data and metadata
  ret[["dbInfo"]] = dbInfo
  ret[["instrument"]] = instrument
  ret[["channels"]] = channels[order(channelID), -c("feModuleType", "feModuleVersion", "longName", "channelName")]
  ret[["regions"]] = regions
  ret[["events"]] = events
  ret[["data"]] = data
  class(ret) = "cefasRSK"

  DBI::dbDisconnect(con)
  return(ret)
}

#' Process the activation times from a RBR instrument
#'
#' @param rsk an object of class cefasRSK
#' @param min_length_activation how short a activation can be in seconds
#'
#' @return cefasRSK object
#' @import data.table
#' @export
#'
rsk.activations <- function(rsk, min_length_activation = 120){
  if(rsk$dbInfo$type != "EPdesktop"){stop("only tested with EPdesktop RSK files")}
  events = rsk[["events"]]
  data = rsk[["data"]]
  # ---- mark activations
  # 24 is start, 25 is pause
  data = merge(data, events[type %in% c(24),.(tstamp, run = (type - 23))], by = "tstamp", all = T) # run is 1 for each activation
  data[is.na(run), run := 0]
  data[, run := cumsum(run)]
  data[, n := .N, by = run] # how many samples per run
  data = data[n > min_length_activation/sample_period] # get rid of short activations
  data = na.omit(data)
  data[, "run" := .GRP, by = run] # renumber runs
  # data[, n := NULL]
  ret[["activations"]] = data[,.(startTime = min(dateTime), duration = max(dateTime) - min(dateTime)), by = run]
  return(rsk)
}

#' Add profile label and GPS metadata to data
#'
#' @param rsk an object of class cefasRSK
#'
#' @import data.table
#' @return cefasRSK object
#' @export
#'
rsk.regions <- function(rsk){
  if(rsk$dbInfo$type != "EPdesktop"){stop("only tested with EPdesktop RSK files")}
  # Apply Profiles
  for(ID in rsk$regions[type == "PROFILE"]$regionID){
    region = rsk$regions[regionID == ID]
    rsk$data[tstamp %between% c(region$tstamp1, region$tstamp2),
             profile := region$label]
  }
  # Apply GPS regions
  for(ID in rsk$regions[type == "GPS"]$regionID){
    region = rsk$regions[regionID == ID]
    rsk$data[tstamp %between% c(region$tstamp1, region$tstamp2),
             c("latitude", "longitude") := list(region$latitude, region$longitude)]
  }
  # Apply Calibration regions
  for(ID in rsk$regions[type == "CALIBRATION_PLATEAU"]$regionID){
    region = rsk$regions[regionID == ID]
    rsk$data[tstamp %between% c(region$tstamp1, region$tstamp2),
             c("label", "refValue", "refUnit") := list(region$label, region$refValue, region$refUnit)]
  }
  return(rsk)
}

write.rsk_csv <- function(rsk, filename){
  fwrite(rsk$instrument, filename)
  fwrite(data.table(NA), filename, append = T)
  fwrite(rsk$channels, filename, append = T, col.names = T)
  fwrite(data.table(NA), filename, append = T)
  fwrite(rsk$data, filename, append = T, col.names = T)
}
