# Set up environment #
require("data.table")
require("dplyr")
require("lubridate")
require("openxlsx")
require("reshape2")
require("odbc")
require("DBI")

options(scipen = 999)

Sys.setenv(TZ = "GMT")

Sys.setenv("R_ZIPCMD" = "D:/Rtools/bin/zip.exe")

# Connect to FRED #
zzCon <- dbConnect(odbc(), 
                   dsn = "", 
                   uid = "", 
                   pwd = "")

## Application dates ##
# 2016: 2016-01-21 - 2016-02-21
# 2017: 2017-01-17 - 2017-02-17
# 2018: 2018-01-15 - 2018-02-15
# 2019: 2019-01-14 - 2019-02-14
# 2020
  # Flash: 2019-11-03 - 2019-11-05
  # Regular: 2020-01-30 - 2020-02-13

# Clear out data #
rm(list = setdiff(ls(), "zzCon"))
gc()

# Post drawing file name #
postDrawingFile <- "marathonPostDrawingSelectedVars2019.csv"

# Application open and close dates #
appOpen <- as.Date("2019-01-14")
appClose <- as.Date("2019-02-14")

# Event IDs and years #
currYear <- 2019
prevYear <- currYear - 1
nextYear <- currYear + 1

currID <- 13543
prevID <- 3385
nextID <- 13674

# Set dormancy level #
y <- 1 # Years
m <- 0 # Months

# Run algo #
source("2 source.R")
