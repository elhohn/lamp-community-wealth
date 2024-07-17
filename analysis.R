library(DBI)
library(odbc)

conn <- dbConnect(odbc::odbc(),
                  dsn = 'impala-prod')

query =
"
SELECT 
    app_no, app_score, requested_project_start_date, funding_oppurtunity_title,
    app_status, fed_approved_amount, requested_federal_amount, grant_program_id, 
    grant_number, `424_federal`, recommended_federal_amount, applicant_name, 
    applicant_ein, app_org_state, app_org_zip, cfda_name, title_of_applicants_project, 
    grantee_id, grant_program_id
    
    FROM mrp_finance.hhs_application_ams_tbl
    
    WHERE funding_oppurtunity_title IN 
        ('Farmers Market Promotion Program',
        'Local Food Promotion Program',
        'Regional Food System Partnerships');
"

app_data <- dbGetQuery(conn, query)

dbDisconnect(conn)


