# README

# There are a lot of pieces at play in this script. First is some testing with
# paramaterized queries. Several approaches are documented, and the
# sqlInterpolate approach is incorporated into a for-loop for inserting new
# tower data into the database table. There are a couple of problems with this.
# One, looping through insert statements is really, really slow. Also, though
# possible to implement, we have not explored or documented here how to handle
# errors, notably handling insert errors stemming from unique date_time,
# measurement constraints. It seems my traditional approach of writing to a
# temporary table to postgres then inserting from that location may still be the
# best, certainly the most efficient. Here, I am using the ON CONFLICT clause in
# the insert statement to avoid inserting duplicate values. Basically, if a
# record exists (where a record is a unique date_time, measurement_id
# combination), that record is skipped in the insert.
# 
# Finally, there are a series of scripts for uploading new data, though note so
# far only for CR1000_EC data. Data are imported, manipulated as needed
# (stacked, formatted, analyses converted to their database reference id, etc.),
# and inserted from a temporary table. There are checks to query for duplicates
# and to be sure all measurments received the proper database reference code.
# Note that we are checking here only for duplicates in the file to be uploaded,
# not duplicates between what is in the file and what may already exist in the
# database.

# R DBI paramaterized queries ----

# helpful resources: 
# https://stackoverflow.com/questions/37131569/proper-way-to-pass-parameters-to-query-in-r-dbi

source('~/Documents/localSettings/pg_prod.R')
source('~/Documents/localSettings/pg_local.R')

# simple SQL statement
# SELECT id, date_time, record, measurement_id, value
# FROM fluxtower.cr1000ec
# WHERE record = 5 AND measurement_id = 1;

# 1. DBI sqlInterpolate
# note that you can use the db connection (e.g., pg_prod, pg_local) in lieu of
# ANSI - ANSI is just a placeholder
rs <- "SELECT * FROM fluxtower.cr1000ec WHERE record = ?rec AND measurement_id = ?meas;"
alpha <- sqlInterpolate(ANSI(), rs, rec = 5, meas = 1)
beta <- dbGetQuery(pg_local, alpha)
# dbClearResult(rs)

# 2a. DBI $1 (positional matching by index) in RPostgres - dbGetQuery
# note details of params (e.g., for postgres ($) versus MySQL (?) in
# ?DBI::dbBind
charlie <- dbGetQuery(pg_local, "SELECT * FROM fluxtower.cr1000ec WHERE record = $1 AND measurement_id = $2;", c(5, 1))

# 2b. DBI $1 (positional matching by index) in RPostgres - dbSendQuery
# note details of params (e.g., for postgres ($) versus MySQL (?) in
# ?DBI::dbBind
rs <- dbSendStatement(pg_local, "SELECT * FROM fluxtower.cr1000ec WHERE record = $1 AND measurement_id = $2;", c(5, 1))
charlie <- dbFetch(rs)
dbClearResult(rs)

# could not get this to work with dbBind directly (sensu
# https://cran.r-project.org/web/packages/RSQLite/vignettes/RSQLite.html)!


# new tower data ----

# libraries
library(tidyr)
library(dplyr)
library(purrr)
library(RPostgreSQL)

# get analyses data
analyses_metadata <- dbGetQuery(pg_prod, "SELECT * FROM fluxtower.measurements")

# newdata <- read_csv('~/Desktop/newCR1000ec/TOA5_90_TOB1_february_28_2017_5297.flux.dat', skip = 1) %>% 
#   slice(-c(1:2))
# newdata <- read_csv('~/Desktop/newCR1000ec/TOA5_91_TOB1_march_31_2017_5297.flux.dat', skip = 1) %>% 
#   slice(-c(1:2))
# newdata <- read_csv('~/Desktop/newCR1000ec/TOA5_92_TOB1_may_2_2017_5297.flux.dat', skip = 1) %>% 
#   slice(-c(1:2))
newdata <- read_csv('~/Desktop/newCR1000ec/TOA5_93_TOB1_may_30_2017_5297.flux.dat', skip = 1) %>% 
  slice(-c(1:2))

# for multiple files
list.files(".", pattern="*.dat", full.names=T, recursive=FALSE) %>% 
  map(~read_csv(.x, skip = 1, col_names = T)) %>% 
  map(~slice(.x, -c(1:2))) %>% 
  map_df(~ as.data.frame(.)) -> newdata 

# need to add a check to see if there are any duplicates
nrow(newdata %>% group_by(TIMESTAMP) %>% filter(n() > 1))

# if so, what to do, purge them?
# newdata %>% distinct(TIMESTAMP, .keep_all = T) # purge duplicates

newDataUpload <- newdata %>%   
  mutate_at(.cols = c(3:length(newdata)), .funs = as.numeric) %>% 
  mutate(TIMESTAMP = as.POSIXct(TIMESTAMP, format = '%Y-%m-%d %H:%M:%S'),
         RECORD = as.integer(RECORD)) %>% 
  gather(key = observation, value = value, Hs:batt_volt_Avg) %>% # stack
  mutate(obsToMatch = tolower(observation)) %>% # need to blitz case to match observations
  inner_join(analyses_metadata[,c('id', 'obsToMatch')], by = 'obsToMatch') %>% 
  select(date_time = TIMESTAMP,
         record = RECORD,
         measurement_id = id,
         value)

# need to add a check to make sure measurement were matches
nrow(newDataUpload %>% filter(is.na(measurement_id)))

# inspect data params, though not much I can do if there are concerns (optional)
newDataUpload %>% 
  group_by(measurement_id) %>% 
  summarise(sd = sd(value),
            min = min(value),
            max = max(value)) %>% 
  inner_join(analyses_metadata[,c(1,2,4)], by = c('measurement_id' = 'id')) -> dataAssessment

# approach one: insert from R ----

# upload tower data ----
for (i in 1:nrow(newDataUpload)){
  
  insertQuery <- "INSERT INTO fluxtower.cr1000ec(date_time, record, measurement_id, value) VALUES (?datetime, ?record, ?measurement, ?value);"
  insertStatement <- sqlInterpolate(ANSI(),
                                    insertQuery, 
                                    datetime = paste0(newDataUpload[i,]$date_time),
                                    record = newDataUpload[i,]$record, 
                                    measurement = newDataUpload[i,]$measurement_id, 
                                    value = newDataUpload[i,]$value)
  dbExecute(pg_local, insertStatement) 
  
}

# approach two: insert from temp table -----
# this is infinitely faster than looping through insert statements

if (dbExistsTable(pg_prod, c('fluxtower', 'temptable'))) dbRemoveTable(pg_prod, c('fluxtower', 'temptable')) # make sure tbl does not exist
dbWriteTable(pg_prod, c('fluxtower', 'temptable'), value = newDataUpload, row.names = F)

dbExecute(pg_prod,'
INSERT INTO fluxtower.cr1000ec
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
ON CONFLICT ON CONSTRAINT cr1000ec_unique_observations DO NOTHING;')

# clean up
if (dbExistsTable(pg_prod, c('fluxtower', 'temptable'))) dbRemoveTable(pg_prod, c('fluxtower', 'temptable')) # make sure tbl does not exist



# database testing ----

# testing constraints
newDataUpload <- tail(newDataUpload)
newDataUpload[1,]$value <- 585