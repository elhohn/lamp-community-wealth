library(DBI)
library(odbc)
library(dplyr)
library(stringr)
library(sf)
library(tidyr)
library(lubridate)
library(ggplot2)
library(scales)
library(gt)
library(tidyUSDA)
library(units)

#census_api_key <- Sys.getenv("CENSUS_API_KEY")
ag_census_api_key <- Sys.getenv("AG_CENSUS_API_KEY")

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

############################
# Community Wealth data
############################
comm_wealth_url <- 'https://raw.githubusercontent.com/CSU-Local-and-Regional-Food-Systems/USDA-AMS-Data-and-Metrics/main/Indicators%20of%20Community%20Wealth/community_wealth.csv'
comm_wealth_all <- read.csv(comm_wealth_url) %>%
  filter(fips > 100) %>%
  filter(!county_name %in% c('Menominee County', 'Kidder County'))# remove national level rows 

# Function to add leading 0 to 4-digit fips values in comm_wealth to facilitate later joins
add_leading_zero <- function(x) {
  # Check if the number has 4 digits
  ifelse(nchar(x) == 4, paste0("0", x), x)
}

# apply funcion to fips column
comm_wealth_all$fips <- sapply(comm_wealth_all$fips, add_leading_zero)

comm_wealth_all <- comm_wealth_all %>%
  filter(variable_name != 'broad_11')


vars <- unique(comm_wealth_all$variable_name)

# cats <- list()
# for (var in vars) {
#   my_cat <- comm_wealth_all$category[match(var, comm_wealth_all$variable_name)]
#   cats <- append(cats, my_cat)
# }
# cats <- unlist(cats)

descriptions_csv <- read.csv('data/metadata_all_files.csv')

descriptions <- list()
for (var in vars) {
  my_desc <- descriptions_csv$variable_definition[match(var, descriptions_csv$variable_name)]
  descriptions <- append(descriptions, my_desc)
}
descriptions <- unlist(descriptions)

cats <- list()
for (var in vars) {
  my_cat <- descriptions_csv$category[match(var, descriptions_csv$variable_name)]
  cats <- append(cats, my_cat)
}
cats <- unlist(cats)

sources <- list()
for (var in vars) {
  my_source <- descriptions_csv$source[match(var, descriptions_csv$variable_name)]
  sources <- append(sources, my_source)
}
sources <- unlist(sources)

d <- data.frame(
  list(
    'variable' = vars,
    'category' = cats,
    'description' = descriptions,
    'source' = sources
  )
)

gt_tbl <- d |>
  mutate(category = replace_na(category, "Uncategorized")) |>
  group_by(category) |>
  gt(rowname_col = "variable") %>%
  cols_hide('category') %>%
  # tab_header(
  #   title = "Indicators of community wealth variables",
  #   subtitle = "Descriptions and sources of data used in analysis"
  # ) |>
  tab_row_group(
    label = html("<strong>Community Characteristics</strong>"),
    rows = category == 'Community Characteristics'
  ) |>
  tab_row_group(
    label = html("<strong>Processing & Distribution</strong>"),
    rows = category == 'Processing & Distribution'
  ) |>
  tab_row_group(
    label = html("<strong>Food Access</strong>"),
    rows = category == 'Food Access'
  ) |>
  tab_row_group(
    label = html("<strong>Institutions</strong>"),
    rows = category == 'Institutions'
  ) |>
  tab_row_group(
    label = html("<strong>Labor</strong>"),
    rows = category == 'Labor'
  ) |>
  tab_row_group(
    label = html("<strong>Demographics</strong>"),
    rows = category == 'Demographics'
  ) |>
  cols_label(
    description = "Description",
    source = "Data Source"
  ) |>
  tab_style(
    style = list(
      cell_fill("grey90"),
      cell_text(color = "black", weight = "bold")
    ),
    locations = cells_row_groups()
  ) |>
  tab_options(row.striping.include_table_body = FALSE) |>
  opt_row_striping(row_striping = FALSE) |>
  opt_table_font(
    font = google_font("Roboto")) |>
  tab_options(
    table.font.size = 12
  ) |>
  tab_options(quarto.disable_processing = TRUE)


# Show the gt table
gt_tbl


#################################
# Poverty data
#################################
pov_url <- 'https://www2.census.gov/programs-surveys/saipe/datasets/2022/2022-state-and-county/est22all.xls'
pov <- rio::import(file = pov_url)
pov <- tail(pov, -2) %>%
  janitor::row_to_names(row_number = 1)
pov$FIPS <- paste0(pov$`State FIPS Code`, pov$`County FIPS Code`)
pov <- data.frame(pov) %>%
  filter(County.FIPS.Code != '000') %>%
  mutate(poverty_rate = as.numeric(Poverty.Percent..All.Ages)) %>%
  select(FIPS, poverty_rate) %>%
  rename(fips = FIPS)

pov_sf <- counties %>%
  merge(pov, by = 'fips') %>%
  st_transform(4326)


##################################
# Farmland proportion stuff
##################################
cropland_acres_df <- getQuickstat(
  key = ag_census_api_key,
  program = "CENSUS",
  data_item = "AG LAND, CROPLAND - ACRES",
  geographic_level = "COUNTY",
  domain = "TOTAL",
  year = "2022",
  geometry = TRUE,
  lower48 = TRUE) %>%
  select(GEOID, NAMELSAD, Value) %>%
  rename(cropland_acres = Value)

pasture_acres_df <- getQuickstat(
  key = ag_census_api_key,
  program = "CENSUS",
  data_item = "AG LAND, PASTURELAND - ACRES",
  geographic_level = "COUNTY",
  domain = "TOTAL",
  year = "2022",
  geometry = TRUE,
  lower48 = TRUE) %>%
  st_drop_geometry() %>%
  select(GEOID, Value) %>%
  rename(pasture_acres = Value)

ag_acres_df <- cropland_acres_df %>%
  mutate(area_meters = st_area(cropland_acres_df),
         county_acres = area_meters * 0.000247105) %>%
  merge(pasture_acres_df, by = 'GEOID') %>%
  mutate(
    ag_acres = cropland_acres + pasture_acres) %>%
  select(GEOID, NAMELSAD, county_acres, ag_acres)

ag_acres_df$county_acres <- units::set_units(st_area(ag_acres_df), "acre")

ag_land <- ag_acres_df %>%
  select(GEOID, NAMELSAD, ag_acres, county_acres) %>%
  drop_na() %>%
  rename(fips = GEOID) %>%
  mutate(ag_proportion = round(ag_acres / county_acres * 100), 1) %>%
  select(fips, ag_proportion) %>%
  drop_units() %>%
  st_drop_geometry() # geoms are wonky from the original soure

ag_land_sf <- counties %>%
  merge(ag_land, by = 'fips') %>%
  st_transform(4326)
