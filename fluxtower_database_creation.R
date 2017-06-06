# README ----

# 2017-05-24 creating a database of the 30-minute data from the flux tower for
# storage (this in addition to files stored on S3), and, mostly, as an
# intermediate step to publishing these data.

# DB connections ----
# source database connection details

# libraries ----
library("EML")
library('RPostgreSQL')
library('RMySQL')
library('tidyverse')
library("tools")
library("readr")
library("readxl")
library("magrittr")

# db_connection ----
source('~/Documents/localSettings/pg_prod.R')

# metadata ----

# get the metadata from Tom Volo's document 
library("docxtractr")
towerMetadata <- read_docx('DataDocumentation.docx')

metadataEC30 <- docx_extract_tbl(towerMetadata, 5) %>% 
  filter(!`Data Table Label` %in% c('TIMESTAMP', 'RECORD')) %>% 
  mutate(base_analyses = "CR1000_EC")

metadataCR23X <- docx_extract_tbl(towerMetadata, 2) %>% 
  filter(!`Data Table Label` %in% c('TIMESTAMP', 'RECORD')) %>% 
  mutate(base_analyses = "CR23X") %>% 
  mutate(`Data Table Label` = replace(`Data Table Label`, `Data Table Label` == 'IR-1dnCor_AVG', 'IR01dnCor_AVG'))
  # note that last mutate is required as there is a slight discrepency between the metadata and datafiles
  
metadataSOIL <- docx_extract_tbl(towerMetadata, 3) %>% 
  filter(!`Data Table Label` %in% c('TIMESTAMP', 'RECORD')) %>% 
  mutate(base_analyses = "CR1000_Soil")

analyses_metadata <- bind_rows(metadataEC30, metadataCR23X, metadataSOIL) %>% 
  mutate(id = seq_along(Units)) %>% 
  select(id, observation = `Data Table Label`, units = Units, decscription = Description) %>% 
  mutate(units = replace(units, units == 'samples', 'number')) %>% 
  mutate(units = replace(units, units == '-', NA)) %>% 
  mutate(obsToMatch = tolower(observation)) %>% # need to blitz case to match observations
  distinct(obsToMatch, .keep_all = T) # purge duplicates (basically just battery voltage)


# CR1000_EC ----

list.files(".", pattern="*.dat", full.names=T, recursive=FALSE) %>% 
  map(~read_csv(.x, skip = 1, col_names = T)) %>% 
  map(~slice(.x, -c(1:2))) %>% 
  map_df(~ as.data.frame(.)) -> EC_data

EC_data %<>% 
  mutate_at(.cols = c(3:length(EC_data)), .funs = as.numeric) %>% 
  mutate(TIMESTAMP = as.POSIXct(TIMESTAMP, format = '%Y-%m-%d %H:%M:%S'),
         RECORD = as.integer(RECORD)) %>% 
  distinct(TIMESTAMP, .keep_all = T) %>% # purge duplicates
  gather(key = observation, value = value, Hs:batt_volt_Avg) %>% # stack
  mutate(obsToMatch = tolower(observation)) %>% # need to blitz case to match observations
  inner_join(analyses_metadata[,c('id', 'obsToMatch')], by = 'obsToMatch') %>% 
  select(date_time = TIMESTAMP,
         record = RECORD,
         measurement_id = id,
         value)

# EC_data[EC_data == ''] <- NA


# CR1000_Soil ----

# get the data, cut out loggernet metadata, and covert to dataframe
list.files(".", pattern="*.dat", full.names=T, recursive=FALSE) %>% 
  map(~read_csv(.x, skip = 1, col_names = T)) %>% 
  map(~slice(.x, -c(1:2))) %>% 
  map_df(~ as.data.frame(.)) -> soil_data
  
soil_data %<>% 
  mutate_at(.cols = c(3:length(soil_data)), .funs = as.numeric) %>% 
  mutate(TIMESTAMP = as.POSIXct(TIMESTAMP, format = '%Y-%m-%d %H:%M:%S'),
         RECORD = as.integer(RECORD)) %>% 
  distinct(TIMESTAMP, .keep_all = T) %>% # purge duplicates
  gather(key = observation, value = value, VW_Avg:PA_uS_3_Avg) %>% # stack
  mutate(obsToMatch = tolower(observation)) %>% # need to blitz case to match observations
  inner_join(analyses_metadata[,c('id', 'obsToMatch')], by = 'obsToMatch') %>% 
  select(date_time = TIMESTAMP,
         record = RECORD,
         measurement_id = id,
         value)

# soil_data[soil_data == ''] <- NA


# CR23X ----

# get the data, cut out loggernet metadata, and covert to dataframe
list.files(".", pattern="*.dat", full.names=T, recursive=FALSE) %>% 
  map(~read_csv(.x, skip = 1, col_names = T)) %>% 
  map(~mutate(.x, Rain_mm_TOT = as.numeric(Rain_mm_TOT))) %>% # need an extra step for this one where a rain as a char is preventing the rbind
  map(~slice(.x, -c(1:2))) %>% 
  map_df(~ as.data.frame(.)) -> CR23X_data

CR23X_data %<>% 
  # mutate_at(.cols = c(3:length(CR23X_data)), .funs = as.numeric) %>% # not needed here
  mutate(TIMESTAMP = as.POSIXct(TIMESTAMP, format = '%Y-%m-%d %H:%M:%S'),
         RECORD = as.integer(RECORD)) %>% 
  distinct(TIMESTAMP, .keep_all = T) %>% # purge duplicates
  gather(key = observation, value = value, SR01_up_1_AVG:Rain_mm_TOT) %>% # stack
  mutate(obsToMatch = tolower(observation)) %>% # need to blitz case to match observations
  inner_join(analyses_metadata[,c('id', 'obsToMatch')], by = 'obsToMatch') %>% 
  select(date_time = TIMESTAMP,
         record = RECORD,
         measurement_id = id,
         value)

# CR23X_data[CR23X_data == ''] <- NA


# analyses_metadata to PG ----
if (dbExistsTable(pg, c('fluxtower', 'measurements'))) dbRemoveTable(pg, c('fluxtower', 'measurements')) # make sure tbl does not exist
dbWriteTable(pg, c('fluxtower', 'measurements'), value = analyses_metadata, row.names = F)

dbExecute(pg,
            '
            ALTER TABLE fluxtower.measurements OWNER TO fluxtower;

            CREATE SEQUENCE fluxtower.measurements_id_seq;

            ALTER TABLE fluxtower.measurements 
            ALTER COLUMN id SET DEFAULT nextval(\'fluxtower.measurements_id_seq\'),
            ALTER COLUMN id SET NOT NULL,
            ADD PRIMARY KEY(id);

            ALTER SEQUENCE fluxtower.measurements_id_seq OWNER TO fluxtower;
            ALTER SEQUENCE fluxtower.measurements_id_seq OWNED BY fluxtower.measurements.id;

            SELECT setval(\'fluxtower.measurements_id_seq\', mx.mx)
            FROM (SELECT MAX(id) AS mx FROM fluxtower.measurements) mx;'
            )

# CR1000_EC to PG ----

if (dbExistsTable(pg, c('fluxtower', 'EC_data'))) dbRemoveTable(pg, c('fluxtower', 'EC_data')) # make sure tbl does not exist
dbWriteTable(pg, c('fluxtower', 'EC_data'), value = EC_data, row.names = F)

dbExecute(pg,'
CREATE TABLE fluxtower.cr1000ec
(
  id serial primary key,
  date_time timestamp without time zone NOT NULL,
  record integer,
  measurement_id integer,
  value double precision
)
WITH (
  OIDS=FALSE
);
ALTER TABLE fluxtower.cr1000ec
ADD CONSTRAINT cr1000ec_fk_measurements
FOREIGN KEY (measurement_id)
REFERENCES fluxtower.measurements(id),
OWNER TO fluxtower;')

dbExecute(pg,'
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
  FROM fluxtower."EC_data"
);')

# clean up
if (dbExistsTable(pg, c('fluxtower', 'EC_data'))) dbRemoveTable(pg, c('fluxtower', 'EC_data')) # make sure tbl does not exist

# unique constraint added at a later date (2017-06-05). Note, however, that this
# creates a unique index, which is functionally similar to a unique constraint 
# but is not actually a constraint. In hindsight, this should have been added as
# a constraint (but that is best done at time of create and is more complicated
# on an existing table).
dbExecute(pg_prod,
          'CREATE UNIQUE INDEX cr1000ec_unique_observations ON fluxtower.cr1000ec (date_time, measurement_id);')

# changing the index to a contraint will allow us to use the constraint to skip
# inserting a record (date_time, measurement_id) if that record exists
dbExecute(pg_prod, '
          DROP INDEX fluxtower.cr1000ec_unique_observations;')
dbExecute(pg_prod,
          'CREATE UNIQUE INDEX cr1000ec_date_time_measurement_id ON fluxtower.cr1000ec (date_time, measurement_id);')
dbExecute(pg_prod, '
          ALTER TABLE fluxtower.cr1000ec ADD CONSTRAINT cr1000ec_unique_observations
          UNIQUE USING INDEX cr1000ec_date_time_measurement_id;')

# CR1000_Soil to PG ----

if (dbExistsTable(pg, c('fluxtower', 'soil_data'))) dbRemoveTable(pg, c('fluxtower', 'soil_data')) # make sure tbl does not exist
dbWriteTable(pg, c('fluxtower', 'soil_data'), value = soil_data, row.names = F)

dbExecute(pg,'
CREATE TABLE fluxtower.cr1000soil
(
  id serial primary key,
  date_time timestamp without time zone NOT NULL,
  record integer,
  measurement_id integer,
  value double precision
)
WITH (
  OIDS=FALSE
);
ALTER TABLE fluxtower.cr1000soil
ADD CONSTRAINT cr1000soil_fk_measurements
FOREIGN KEY (measurement_id)
REFERENCES fluxtower.measurements(id),
OWNER TO fluxtower;')

dbExecute(pg,'
INSERT INTO fluxtower.cr1000soil
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
  FROM fluxtower."soil_data"
);')

# clean up
if (dbExistsTable(pg, c('fluxtower', 'soil_data'))) dbRemoveTable(pg, c('fluxtower', 'soil_data')) # make sure tbl does not exist

# unique constraint added at a later date (2017-06-05). Note, however, that this
# creates a unique index, which is functionally similar to a unique constraint 
# but is not actually a constraint. In hindsight, this should have been added as
# a constraint (but that is best done at time of create and is more complicated
# on an existing table).
dbExecute(pg_prod,
          'CREATE UNIQUE INDEX cr1000soil_unique_observations ON fluxtower.cr1000soil (date_time, measurement_id);')

# changing the index to a contraint will allow us to use the constraint to skip
# inserting a record (date_time, measurement_id) if that record exists
dbExecute(pg_prod, '
          DROP INDEX fluxtower.cr1000soil_unique_observations;')
dbExecute(pg_prod,
          'CREATE UNIQUE INDEX cr1000soil_date_time_measurement_id ON fluxtower.cr1000soil (date_time, measurement_id);')
dbExecute(pg_prod, '
          ALTER TABLE fluxtower.cr1000soil ADD CONSTRAINT cr1000soil_unique_observations
          UNIQUE USING INDEX cr1000soil_date_time_measurement_id;')

# CR23X to PG ----

if (dbExistsTable(pg, c('fluxtower', 'CR23X'))) dbRemoveTable(pg, c('fluxtower', 'CR23X')) # make sure tbl does not exist
dbWriteTable(pg, c('fluxtower', 'CR23X'), value = CR23X_data, row.names = F)

dbExecute(pg,'
CREATE TABLE fluxtower.cr23x
(
  id serial primary key,
  date_time timestamp without time zone NOT NULL,
  record integer,
  measurement_id integer,
  value double precision
)
WITH (
  OIDS=FALSE
);
ALTER TABLE fluxtower.cr23x
ADD CONSTRAINT cr23x_fk_measurements
FOREIGN KEY (measurement_id)
REFERENCES fluxtower.measurements(id),
OWNER TO fluxtower;')

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
  FROM fluxtower."CR23X"
);')

# clean up
if (dbExistsTable(pg, c('fluxtower', 'CR23X'))) dbRemoveTable(pg, c('fluxtower', 'CR23X')) # make sure tbl does not exist

# unique constraint added at a later date (2017-06-05). Note, however, that this
# creates a unique index, which is functionally similar to a unique constraint 
# but is not actually a constraint. In hindsight, this should have been added as
# a constraint (but that is best done at time of create and is more complicated
# on an existing table).
dbExecute(pg_prod,
          'CREATE UNIQUE INDEX cr23x_unique_observations ON fluxtower.cr23x (date_time, measurement_id);')

# changing the index to a contraint will allow us to use the constraint to skip
# inserting a record (date_time, measurement_id) if that record exists
dbExecute(pg_prod, '
          DROP INDEX fluxtower.cr23x_unique_observations;')
dbExecute(pg_prod,
          'CREATE UNIQUE INDEX cr23x_date_time_measurement_id ON fluxtower.cr23x (date_time, measurement_id);')
dbExecute(pg_prod, '
          ALTER TABLE fluxtower.cr23x ADD CONSTRAINT cr23x_unique_observations
          UNIQUE USING INDEX cr23x_date_time_measurement_id;')
