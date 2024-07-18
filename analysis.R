library(DBI)
library(odbc)
library(dplyr)
library(stringr)
library(sf)
library(tidyr)
library(lubridate)
library(ggplot2)
library(scales)

conn <- dbConnect(odbc::odbc(),
                  dsn = 'impala-prod')

query =
"
SELECT 
    app_no, app_score, requested_project_start_date, funding_oppurtunity_title,
    app_status, fed_approved_amount, requested_federal_amount, grant_program_id, 
    grant_number, `424_federal`, recommended_federal_amount, applicant_name, 
    applicant_ein, app_org_city, app_org_state, app_org_zip, cfda_name, 
    title_of_applicants_project, grantee_id, app_recvd_date
    
FROM mrp_finance.hhs_application_ams_tbl
    
WHERE funding_oppurtunity_title IN 
    ('Farmers Market Promotion Program',
    'Farmers Market Promotion Program Fiscal Year 2024', 
    'Local Food Promotion Program',
    'Local Food Promotion Program Fiscal Year 2024',
    'Regional Food System Partnerships');
"

from_db <- dbGetQuery(conn, query)

dbDisconnect(conn)

#################
# Add spatial data
#################
# clean up location data to make it possible to join with geo data
app_data <- from_db
app_data$city <- str_to_title(str_trim(app_data$app_org_city))
app_data$state <- str_to_upper(str_trim(app_data$app_org_state))
app_data <- app_data |>
  mutate(
    funded = 
      ifelse(is.na(recommended_federal_amount) | recommended_federal_amount == 0, 0, 1),
    application_year = lubridate::year(app_recvd_date),
    )

# Load US Cities dataset from github
url <- 'https://raw.githubusercontent.com/kelvins/US-Cities-Database/main/csv/us_cities.csv'
city_loc <- read.csv(url) |>
  select(-c(ID, COUNTY)) |>
  rename(
    city = CITY,
    state = STATE_CODE
  ) |>
  mutate(city = str_squish(str_trim(str_to_title(city)))) |>
  group_by(city, state) |>
  filter(row_number()==1) |> 
  ungroup()

# do the join
app_data <- merge(app_data, city_loc, by = c('city', 'state'), all.x = TRUE)

# convert to sf object
app_data_sf <- app_data |> 
  drop_na(LATITUDE, LONGITUDE) |>
  st_as_sf(coords = c("LONGITUDE", "LATITUDE")) |>
  st_set_crs(4326) |>
  st_transform(4326)

# Load counties data (downloaded somewhere on the interwebs...)
counties <- st_read('data/cb_2021_us_county_20m/cb_2021_us_county_20m.shp') |>
  filter(!STATE_NAME %in% c('Alaska', 'Hawaii', 'Puerto Rico')) |>
  rename(
    county_name = NAMELSAD,
    fips = GEOID
  ) |>
  select(fips, county_name, geometry) |>
  #st_set_crs(4326) |>
  st_transform(4326)

# spatial join points within counties
app_data_by_county <- counties |>
  st_join(app_data_sf, join = st_contains, left = TRUE) |>
  group_by(fips) |>
  mutate(app_no = ifelse(is.na(app_no), "none", app_no)) |>
  summarize(
    total_app_count = n(),
    total_app_funded = as.integer(sum(funded, na.rm = TRUE)),
    success_rate = round(total_app_funded/total_app_count, 2),
    total_funding = as.integer(sum(recommended_federal_amount, na.rm = TRUE))
    )

############################
# make a plot of funding
############################

g <- ggplot(app_data, aes(x = application_year, y = recommended_federal_amount)) +
  geom_point(shape = 95, size = 10, alpha = .4, color = '#005440') +
  theme(legend.position="none")  +
  #scale_x_date(date_breaks = '1 year', date_labels = "'%y") + 
  scale_y_continuous(labels = label_dollar()) +
  ylab("Funded amount") +
  xlab("")

g
