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
  if(dbInfo$type[1] == "EasyParse"){easyparse = T}else{easyparse = F}

  instrument = setDT(DBI::dbReadTable(con, "instruments"))

  if(easyparse){
    deployments = setDT(DBI::dbGetQuery(con, "SELECT comment, loggerTimeDrift, timeOfDownload, name, sampleSize FROM deployments"))
    deployments = cbind(deployments,DBI::dbGetQuery(con, "SELECT mode, samplingPeriod FROM schedules"))
    tzoffset = 0 # actually need to check this
  } else {
    deployments = setDT(DBI::dbGetQuery(con, "SELECT
                                      comment, loggerTimeDrift, timeOfDownload, name, sampleSize, mode, samplingPeriod
                                      FROM deployments
                                      JOIN schedules ON deployments.instrumentID = schedules.instrumentID
                                      JOIN continuous ON schedules.scheduleID = continuous.continuousID"))
    tzoffset = RSQLite::dbGetQuery(con, "SELECT value FROM parameterKeys WHERE key = 'OFFSET_FROM_UTC'")
    tzoffset = as.numeric(tzoffset$value) * 3600
    }

  events = setDT(DBI::dbReadTable(con, "events"))
  events = merge(events, rbr_event_codes, by.x = "type", by.y = "Event")[order(tstamp)]

  # channels = setDT(DBI::dbReadTable(con, "channels"))
  if(easyparse){
    channels = setDT(DBI::dbGetQuery(con, "SELECT
                                     instrumentChannels.channelID, shortName, units, 'unknown' AS serialID
                                     FROM channels
                                     LEFT JOIN instrumentChannels ON channels.channelID = instrumentChannels.channelID"))
  } else {
    channels = setDT(DBI::dbGetQuery(con, "SELECT
                                     instrumentChannels.channelID, shortName, units, CAST(serialID AS char) AS serialID
                                     FROM channels
                                     LEFT JOIN instrumentChannels ON channels.channelID = instrumentChannels.channelID
                                     LEFT JOIN instrumentSensors ON instrumentChannels.channelOrder = instrumentSensors.channelOrder"))
  }
  channels = merge.data.table(channels, rbr_channels[,.(Type, Description)], by.x = "shortName", by.y = "Type", all.x = T)
  channels[, channelName := paste0("channel", formatC(channelID, 1, format = "d", flag = "0"))]

  if("doxy23" %in% channels$shortName){
    odo_serial = channels[shortName == "doxy23"]$serialID
    channels[shortName == "temp16", serialID := odo_serial]
  }
  channels[shortName %in% c("cond10", "temp14"), serialID := instrument$serialID]

  errors = setDT(DBI::dbReadTable(con, "errors"))
  errors = merge(errors, rbr_error_codes, by.x = "type", by.y = "Error")
  errors[, channelName := paste0("channel", formatC(channelOrder, 1, format = "d", flag = "0"))]
  errors = merge(errors, channels[,.(channelName, shortName)], by = "channelName")

  region_query = DBI::dbSendQuery(con, "
                                  SELECT
                                  region.regionID as regionID,
                                  type, tstamp1, tstamp2, label, latitude, longitude, refValue, refunit
                                  from region
                                  LEFT JOIN regionGeoData ON region.regionID = regionGeoData.regionID
                                  LEFT JOIN regionPlateau ON region.regionID = regionPlateau.regionID
                                  ")
  regions = setDT(DBI::dbFetch(region_query))
  regions[, startTime := as.POSIXct((tstamp1/1000), origin = "1970-01-01", tz = "UTC") - tzoffset]
  DBI::dbClearResult(region_query)

  sample_period = RSQLite::dbReadTable(con, "continuous")$samplingPeriod / 1000

  fields = DBI::dbListFields(con, "data") # data names
  fields = fields[!grepl("tstamp", fields)]
  sql_fields = paste(c("tstamp", fields), collapse = ",")
  sql_fields = paste("SELECT", sql_fields, "FROM data")
  data_query = DBI::dbSendQuery(con, paste(sql_fields,  ";"))
  data = setDT(DBI::dbFetch(data_query))
  DBI::dbClearResult(data_query)
  data[, dateTime := as.POSIXct((tstamp/1000), origin = "1970-01-01", tz = "UTC") - tzoffset]
  data.table::setnames(data, channels$channelName, channels$shortName, skip_absent = T)

  ret = list() # Initialise return list containing data and metadata
  ret[["dbInfo"]] = dbInfo
  ret[["deployments"]] = deployments
  ret[["instrument"]] = instrument
  ret[["channels"]] = channels[order(channelID), -c("channelName")]
  ret[["errors"]] = errors
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
#'
rsk.activations <- function(rsk, min_length_activation = 120){
  if(dbInfo$type[1] != "EPdesktop"){warning("only tested with EPdesktop RSK files")}
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
  data[, n := NULL]
  # rsk[["data"]] = data
  # rsk[["activations"]] = data[,.(startTime = min(dateTime), duration = max(dateTime) - min(dateTime)), by = run]
  # return(rsk)
}

#' Add profile label and GPS metadata to extracted data
#'
#' @param rsk an object of class cefasRSK
#'
#' @import data.table
#' @return cefasRSK object
#' @export
#'
rsk.regions <- function(rsk){
  # Apply Profiles
  for(ID in rsk$regions[type == "PROFILE"]$regionID){
    region = rsk$regions[regionID == ID]
    region[label == "", label := ID]
    rsk$data[tstamp %between% c(region$tstamp1, region$tstamp2),
             c("profile", "startTime") := list(region$label, min(region$startTime))]
  }
  for(ID in rsk$regions[type == "CAST"]$regionID){
    region = rsk$regions[regionID == ID]
    region[label == "", label := ID]
    rsk$data[tstamp %between% c(region$tstamp1, region$tstamp2),
             c("cast", "startTime") := list(region$label, min(region$startTime))]
  }
  # Apply GPS regions
  for(ID in rsk$regions[type == "GPS"]$regionID){
    region = rsk$regions[regionID == ID]
    rsk$data[tstamp %between% c(region$tstamp1, region$tstamp2),
             c("site", "latitude", "longitude") := list(region$label, region$latitude, region$longitude)]
  }
  # Apply Calibration regions
  for(ID in rsk$regions[type == "CALIBRATION_PLATEAU"]$regionID){
    region = rsk$regions[regionID == ID]
    region[label == "", label := ID]
    rsk$data[tstamp %between% c(region$tstamp1, region$tstamp2),
             c("label", "refValue", "refUnit") := list(region$label, region$refValue, region$refUnit)]
  }
  return(rsk)
}

rsk.getCalibration.RBRCoda <- function(rsk){
  if(rsk$dbInfo$type != "EPdesktop"){warning("only tested with EPdesktop RSK files")}

  if(nrow(rsk$regions[type == "CALIBRATION_PLATEAU"]) < 2){stop("Calibration requires at least two calibration plateaus")}

  # Collect calibrations
  for(ID in rsk$regions[type == "CALIBRATION_PLATEAU"]$regionID){
    region = rsk$regions[regionID == ID]
    rsk$data[tstamp %between% c(region$tstamp1, region$tstamp2),
             c("refValue", "refUnit") := list(region$refValue, region$refUnit)]
  }
  cal_data = rsk$data[!is.na(refValue)]
  if(all(cal_data$refUnit == "%")){

  }
  if(all(cal_data$refUnit == "x")){

  }else{
    stop("calibration reference units are not all the same type, please amend .rsk")
  }

}

rsk.write_csv <- function(rsk, filename){
  fwrite(rsk$instrument, filename)
  fwrite(data.table(NA), filename, append = T)
  fwrite(rsk$channels, filename, append = T, col.names = T)
  fwrite(data.table(NA), filename, append = T)
  fwrite(rsk$data, filename, append = T, col.names = T)
}

#' Insert location metadata into .RSK file from table
#'
#' @param filename .rsk file path
#' @param tbl a data.frame containing the following columns
#' dateTime - POSIXct timestamp
#' latitude - in decimal degrees
#' longitude - in decimal degrees
#' label - a station code, can be numeric or text
#' description - text, can be empty
#' @param duration how long to set the length of the annotation, in seconds
#'
#' @return nothing
#'
#' @examples
#' arb_data = data.frame(
#' label = c(10, 55),
#' description = c("arbitary position data"),
#' dateTime = as.POSIXct(c("2024-04-21 08:19", "2024-04-21 09:30"), tz = "UTC"),
#' latitude = c(52.1, 52.12),
#' longitude = c(2, 2.1)
#' )
#' ## Not run:
#' rsk.addgeoregion("myrsk.rsk", arb_data)
#' ## End(Not run)
#' @export
rsk.addgeoregion <- function(filename, tbl, duration = 120){
  # validation of tbl
  expected_columns = c("dateTime", "latitude", "longitude", "label", "description")
  if(!all(expected_columns %in% names(tbl))) {
    missing = expected_columns[!expected_columns %in% names(tbl)]
    stop(paste(missing, "not found in tbl, unable to write geo regions"))
  }

  setDT(tbl)

  if(!file.exists(filename)){stop(paste(filename, "does not exist"))}
  con <- DBI::dbConnect(RSQLite::SQLite(), dbname = filename)

  dbInfo <- RSQLite::dbReadTable(con, "dbInfo")
  if(dbInfo$type[1] == "EasyParse"){easyparse = T}else{easyparse = F}

  if(easyparse){
    tzoffset = 0 # TODO actually need to check this
  } else {
    tzoffset = RSQLite::dbGetQuery(con, "SELECT value FROM parameterKeys WHERE key = 'OFFSET_FROM_UTC'")
    tzoffset = as.numeric(tzoffset$value) * 3600
  }

  region  = setDT(DBI::dbReadTable(con, "region"))
  geo  = setDT(DBI::dbReadTable(con, "regionGeoData"))

  tduration = duration*1000 # rsk timestamps are in 1000ths of a second since 1970

  tbl[, regionID := max(region$regionID) + 1:.N]
  tbl[, tstamp1 := bit64::as.integer64(as.numeric(dateTime + tzoffset)*1000)]
  tbl[, tstamp2 := tstamp1 + tduration]

  new_regions = tbl[,.(
    datasetID = max(region$regionID),
    regionID, type = "GPS",
    tstamp1, tstamp2,
    label, description = "position from cefasRBR",
    collapsed = 0)]
  new_geo = tbl[,.(regionID, latitude, longitude)]
  DBI::dbAppendTable(con, "region", new_regions)
  DBI::dbAppendTable(con, "regionGeoData", new_geo)
  print(paste("wrote", nrow(new_regions),"to rsk"))
  DBI::dbDisconnect(con)
}

#' Recalculate profiles for a .RSK file
#'
#' This tool gives you a little more control on the automated profile detection provided by ruskin
#'
#'  Note: this tool will overwrite any annotations already present in the .RSK, use with caution
#'  Note: this tool does not attempt to identify casts, TODO
#'
#' @param filename the .RSK file
#' @param salinity_tol the minimum salinity for the start of a profile, default = 0.1
#' @param prs_tol the threshold increase in pressure over the minimum, default is 0.1 (i.e. minimum recorded pressure + 0.1 hPa)
#'
#' @returns writes directly to the .RSK
#' @export
rsk.recalculate_profiles <- function(filename, salinity_tol = 0.1, prs_tol = 0.1){
  con <- DBI::dbConnect(RSQLite::SQLite(), dbname = filename)
  dbInfo <- RSQLite::dbReadTable(con, "dbInfo")
  if(dbInfo$type[1] == "EasyParse"){easyparse = T}else{easyparse = F}
  region  = setDT(DBI::dbReadTable(con, "region"))
  events  = setDT(DBI::dbReadTable(con, "events"))
  channels  = setDT(DBI::dbReadTable(con, "channels"))
  channels[, `:=`(channelName, paste0("channel", formatC(channelID,  1, format = "d", flag = "0")))]
  starts = events[type == 24] # twists
  stops = events[type == 25] # twists
  stops = stops[tstamp > min(starts$tstamp)]
  if(nrow(starts) == nrow(stops)){
    starts = cbind(starts, stops[,.(tstamp_end = tstamp)])
  }else{stop()}
  starts[, duration := (tstamp_end/1000) - (tstamp/1000)]
  starts = starts[duration > 60]
  DATA  = setDT(DBI::dbReadTable(con, "data"))[order(tstamp)]
  setnames(DATA, channels$channelName, channels$shortName, skip_absent = T)
  m = rbind(starts[,.(tstamp, id = 1)], DATA[,.(tstamp, pres24, sal_00, id = 0)], fill=T)[order(tstamp)]
  m[, id := cumsum(id)]
  m = m[!is.na(pres24) & id > 0]
  apres = min(m$pres24)
  m[, maxprs := max(pres24), by = id]
  m = m[m[pres24 == maxprs, .(id, maxprs_tstamp = tstamp)], on = "id"]
  m = m[sal_00 > 0.1 & pres24 > apres+0.10]
  m[, cast := "down"]
  m[tstamp > maxprs_tstamp, cast := "up"]

  profiles = m[,.(datasetID = 1, type = "PROFILE",
                  tstamp1 = min(tstamp+3000), tstamp2 = max(tstamp-3000),
                  label = .GRP, description = "cefasRBR generated profile",
                  collapsed = 0), by = list(regionID = id)]
  # down = m[cast == "down",.(datasetID = 1, type = "CAST",
  #                 tstamp1 = min(tstamp+10000), tstamp2 = max(tstamp),
  #                 label = "", description = "cefasRBR generated cast",
  #                 collapsed = 0), by = list(regionID = id)]
  # up = m[cast == "up",.(datasetID = 1, type = "CAST",
  #                 tstamp1 = min(tstamp), tstamp2 = max(tstamp-10000),
  #                 label = "", description = "cefasRBR generated cast",
  #                 collapsed = 0), by = list(regionID = id)]
  # regions = rbind(profiles, down, up)[order(tstamp1)]
  regions[, regionID := 1:.N]
  DBI::dbWriteTable(con, "region", profiles, overwrite = T)
  DBI::dbDisconnect(con)
}
