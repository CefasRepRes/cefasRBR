# rbr_event_codes = data.table::fread("data/rbr_event_codes.csv", encoding = "UTF-8")[,.(Event, Description)] # TODO embed in package
# rbr_error_codes = data.table::fread("data/rbr_error_codes.csv", encoding = "UTF-8")[,.(Error, Description)]
# rbr_channels = data.table::fread("data/rbr_channels.csv", encoding = "UTF-8")
# usethis::use_data(rbr_event_codes, overwrite = T)
# usethis::use_data(rbr_error_codes, overwrite = T)
# usethis::use_data(rbr_channels, overwrite = T)

#' RBR logger3 command set - channel types
#'
#' A list of the supported channel types by the RBR logger3 firmware, and a description of what those channels are.
#' Used by `read.rsk`
#'
#' @source <https://docs.rbr-global.com/display/L3commandreference/Supported-Channel-Types>
"rbr_channels"

#' RBR logger3 command set - error codes
#'
#' A list of the supported error codes by the RBR logger3 firmware, and a description of what those errors are.
#' Used by `read.rsk`
#'
#' @source <https://docs.rbr-global.com/L3commandreference/format-of-stored-data/easyparse-calbin00-format/sample-data-easyparse-format>
"rbr_error_codes"

#' RBR logger3 command set - event codes
#'
#' A list of the supported event codes by the RBR logger3 firmware, and a description of what those events are.
#' Used by `read.rsk`
#'
#' @source <https://docs.rbr-global.com/L3commandreference/format-of-stored-data/easyparse-calbin00-format/easyparse-format-events-markers>
"rbr_event_codes"
