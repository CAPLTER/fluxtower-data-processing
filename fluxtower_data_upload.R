
# README -----------------------------------------------------------------------

# General workflow for adding new EC and CR23x data

# Here, I am using the ON CONFLICT clause in the insert statement to avoid
# inserting duplicate values. Basically, if a record exists (where a record is
# a unique date_time, measurement_id combination), that record is skipped in
# the insert.

# Data are imported, manipulated as needed (stacked, formatted, analyses
# converted to their database reference id, etc.), and inserted from a
# temporary table. There are checks to query for duplicates and to be sure all
# measurments received the proper database reference code. Note that we are
# checking here only for duplicates in the file to be uploaded, not duplicates
# between what is in the file and what may already exist in the database.


# libraries --------------------------------------------------------------------

library(tidyverse)
library(RPostgreSQL)


# database connections ---------------------------------------------------------

source("~/Documents/localSettings/pg_prod.R")
source("~/Documents/localSettings/pg_local.R")


# EC ---------------------------------------------------------------------------

# process data -----------------------------------------------------------------

# get analyses data
analyses_metadata <- dbGetQuery(pg, "SELECT * FROM fluxtower.measurements")
# analyses_metadata <- read_csv("~/Desktop/measurements.csv")

# single file
# newdata <- read_csv("~/Desktop/fluxTower/TOA5_102_TOB1_february_27_2018_5297.flux.dat", skip = 1) %>%
#   slice(-c(1:2))

dats <- list.files("~/Desktop/fluxTower", pattern = "*.dat", full.names = T, recursive = FALSE)

harvest_flux <- function(dat) {

  file <- read_csv(
    file = dat,
    skip = 1,
    na = c("NAN"),
    col_names = T) %>%
  slice(-c(1:2)) %>%
  mutate(filename = basename(dat))

return(file)

}

newdata <- map_df(.x = dats, .f = harvest_flux)


# need to add a check to see if there are any duplicates
nrow(newdata %>% group_by(TIMESTAMP) %>% filter(n() > 1))

# if so, what to do, purge them?
# newdata %>% distinct(TIMESTAMP, .keep_all = T) # purge duplicates

newDataUpload <- newdata %>%
  mutate_at(.vars = c(3:length(newdata) - 1), .funs = as.numeric) %>%
  mutate(
    TIMESTAMP = as.POSIXct(TIMESTAMP, format = "%Y-%m-%d %H:%M:%S"),
    RECORD = as.integer(RECORD)
    ) %>%
  gather(key = observation, value = value, Hs:batt_volt_Avg) %>% # stack
  # need to blitz case to match observations
  mutate(obsToMatch = tolower(observation)) %>%
    inner_join(analyses_metadata[, c("id", "obsToMatch")], by = "obsToMatch") %>%
    select(
      date_time = TIMESTAMP,
      record = RECORD,
      measurement_id = id,
      value,
      filename
    )

# need to add a check to make sure measurement were matches
nrow(newDataUpload %>% filter(is.na(measurement_id)))

# inspect data params, though not much I can do if there are concerns (optional)
newDataUpload %>%
  group_by(measurement_id) %>%
  summarise(
    sd = sd(value, na.rm = T),
    min = min(value, na.rm = T),
    max = max(value, na.rm = T)
    ) %>%
  inner_join(analyses_metadata[, c(1, 2, 4)], by = c("measurement_id" = "id")) %>%
  print(n = Inf)


# write.csv(newDataUpload, "~/Desktop/newDataUpload.csv")


# write to database ------------------------------------------------------------

if (dbExistsTable(pg, c("fluxtower", "temptable"))) dbRemoveTable(pg, c("fluxtower", "temptable")) # make sure tbl does not exist
dbWriteTable(pg, c("fluxtower", "temptable"), value = newDataUpload, row.names = F)

# remove time zone created by dbWriteTable
dbExecute(pg,'
  ALTER TABLE fluxtower.temptable
  ALTER COLUMN date_time TYPE TIMESTAMP WITHOUT TIME ZONE;')

# insert from temp to cr1000ec
dbExecute(pg,'
INSERT INTO fluxtower.cr1000ec
(
  date_time,
  record,
  measurement_id,
  value,
  filename
)
(
  SELECT
    date_time,
    record,
    measurement_id,
    value,
    filename
  FROM fluxtower.temptable
)
ON CONFLICT ON CONSTRAINT cr1000ec_unique_observations DO NOTHING;')

# make sure tbl does not exist
if (dbExistsTable(pg, c("fluxtower", "temptable"))) dbRemoveTable(pg, c("fluxtower", "temptable"))


# CR23x ------------------------------------------------------------------------

# process data -----------------------------------------------------------------

# get the data, cut out loggernet metadata, and covert to dataframe
CR23X_data <- list.files("~/Desktop/cr23data", pattern = "*.dat", full.names = T, recursive = FALSE) %>%
  map(~read_csv(.x, skip = 1, na = c("NAN"), col_names = T)) %>%
  map(~mutate(.x, Rain_mm_TOT = as.numeric(Rain_mm_TOT))) %>% # need an extra step for this one where a rain as a char is preventing the rbind
  map(~slice(.x, -c(1:2))) %>%
  map_df(~ as.data.frame(.))

# need to add a check to see if there are any duplicates
nrow(CR23X_data %>% group_by(TIMESTAMP) %>% filter(n() > 1))

CR23X_data <- CR23X_data %>%
  # mutate_at(.cols = c(3:length(CR23X_data)), .funs = as.numeric) %>% # not needed here
  mutate(
    TIMESTAMP = as.POSIXct(TIMESTAMP, format = "%Y-%m-%d %H:%M:%S"),
    RECORD = as.integer(RECORD)
    ) %>%
#   distinct(TIMESTAMP, .keep_all = T) %>% # purge duplicates - update: check first
gather(key = observation, value = value, SR01_up_1_AVG:Rain_mm_TOT) %>% # stack
# need to blitz case to match observations
mutate(obsToMatch = tolower(observation)) %>%
  inner_join(analyses_metadata[, c("id", "obsToMatch")], by = "obsToMatch") %>%
  select(
    date_time = TIMESTAMP,
    record = RECORD,
    measurement_id = id,
    value)

# need to add a check to make sure measurement were matches
nrow(CR23X_data %>% filter(is.na(measurement_id)))

# write to database ------------------------------------------------------------

# data to temp table
if (dbExistsTable(pg, c("fluxtower", "temptable"))) dbRemoveTable(pg, c("fluxtower", "temptable")) # make sure tbl does not exist
dbWriteTable(pg, c("fluxtower", "temptable"), value = CR23X_data, row.names = F)

# remove time zone created by dbWriteTable
dbExecute(pg,'
  ALTER TABLE fluxtower.temptable
  ALTER COLUMN date_time TYPE TIMESTAMP WITHOUT TIME ZONE;')

# insert temp table to cr23x
dbExecute(pg,'
INSERT INTO fluxtower.cr23x
(
  date_time,
  record,
  measurement_id,
  value
)
(
  SELECT
    date_time,
    record,
    measurement_id,
    value
  FROM fluxtower.temptable
)
ON CONFLICT ON CONSTRAINT cr23x_unique_observations DO NOTHING;')

# clean up
if (dbExistsTable(pg, c("fluxtower", "temptable"))) dbRemoveTable(pg, c("fluxtower", "temptable"))
