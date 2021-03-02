library(tidyverse)
library(sparklyr)

source('/home/cdsw/q_covid_valid/_functions.R')

config <- spark_config() 
config$spark.dynamicAllocation.maxExecutors <- 30
config$spark.executor.cores <- 5
config$spark.executor.memory <- "20g"
config$spark.driver.maxResultSize <- "10g"
sc <- spark_connect(master = "yarn-client",
                    app_name = "q_cov_val_pp",
                    config = config,
                    version = "2.3.0")


# function to rename column 'score' to 'baseline_risk' in sparklyr
rename_col <- function(df){
  df <- select(df, t, death_covid, age, a, linked_gpes, ethnicity_9,
               town_q, region, tpp, census_person_id, baseline_score = score)
  return(df)
}

# function to select columns after dfs have been joined
select_cols <- function(df){
  df <- select(df, t, death_covid, age, age, linked_gpes, ethnicity_9, 
               town_q, region, score, baseline_score, tpp)
}

# For every age:
# BMI to 25
# Ethnicity to white
# Townsend to 1 (most affluent)
# All comorbidities to 0

set_values <- function(df){

  df <- df %>%
    mutate(bmi = 25,
           ethnicity = 0, # white
           town_q = 1, # most affluent
           p_solidtransplant = 0,
           b_congenheart = 0,
           b_cerebralpalsy = 0,
           b_respcancer = 0,
           b_vte = 0,
           p_marrow6 = 0,
           p_radio6 = 0,
           chemocat = 0,
           b_asthma = 0,
           b_AF = 0,
           b_chd = 0,
           b_bloodcancer = 0,
           b_copd = 0,
           b_pulmrare = 0,
           b_dementia = 0,
           diabetes_cat = 0,
           b_epilepsy = 0,
           b_CCF = 0,
           learncat = 0,
           b_cirrhosis = 0,
           b_neurorare = 0,
           b_parkinsons = 0,
           b_pvd = 0,
           b_pulmhyper = 0,
           b_ra_sle = 0,
           b_semi = 0,
           b_sicklecelldisease = 0,
           b_stroke = 0,
           b_fracture4 = 0,
           b2_82 = 0,
           b2_leukolaba = 0,
           b2_prednisolone = 0,
           renalcat = 0,
           diabetes0 = 1,
           diabetes1= 0,
           diabetes2 = 0,
           chemocat0 = 1,
           chemocat1 = 0,
           chemocat2 = 0,
           chemocat3 = 0,
           ethnicity0 = 1, # white
           ethnicity1 = 0,
           ethnicity2 = 0,
           ethnicity3 = 0,
           ethnicity4 = 0,
           ethnicity5 = 0,
           ethnicity6 = 0,
           ethnicity7 = 0,
           ethnicity8 = 0,
           ethnicity9 = 0,
           ethnicity10 = 0,
           ethnicity11 = 0,
           ethnicity12 = 0,
           ethnicity13 = 0,
           ethnicity14 = 0,
           ethnicity15 = 0,
           ethnicity16 = 0,
           homecat0 = 1, # neither homeless nor carehome
           homecat1 = 0,
           homecat2 = 0,
           learncat0 = 1, # no learning disability
           learncat1 = 0,
           learncat2 = 0,
           renalcat0 = 1, # no chronic kidney disease
           renalcat1 = 0,
           renalcat2 = 0,
           renalcat3 = 0,
           renalcat4 = 0,
           renalcat5 = 0,
           renalcat6 = 0)
  
  return(df)
}






# read in data 
df <- sdf_sql(sc, "SELECT * FROM  q_cov_valid.linked_census_qcovid_bmi_imp_pp")

## get unique ages for min and max
#ages <- df %>% distinct(age) %>% collect() %>% .$age
#min_age <- min(ages)
#max_age <- max(ages)

# Period 1 : 24/01/2020 to 30/04/2020 #
start_date_1 =  "2020-01-24"
end_date_1 = "2020-04-30"

df_p1 <- df %>%
      mutate(death_covid = ifelse(death_date <= end_date_1 & death_covid == 1,1,0),
             t = ifelse(!is.na(death_date) & death_date < end_date_1,
                    datediff(death_date, start_date_1),
                    datediff(end_date_1, start_date_1)))

# This definitely works
df_p1 <- set_values(df_p1)

# scores for females 
base_female_risk_p1 <- rename_col(get_female_death_scores(df_p1))

# scores for males 
base_male_risk_p1 <- rename_col(get_male_death_scores(df_p1))



# Period 2 : 01/05/2020 to 28/07//2020 # 
start_date_2 =  "2020-05-01"
end_date_2 = "2020-07-28"

df_p2 <- df %>%
      mutate(death_covid = ifelse(death_date <= end_date_2 & death_covid == 1,1,0),
             t = ifelse(!is.na(death_date) & death_date < end_date_2,
                    datediff(death_date, start_date_2),
                    datediff(end_date_2, start_date_2)))

df_p2 <- set_values(df_p2)

# scores for females 
base_female_risk_p2 <- rename_col(get_female_death_scores(df_p2))

# scores for males 
base_male_risk_p2 <- rename_col(get_male_death_scores(df_p2))


# read in original risk scores
male_risk_p1 <- sdf_sql(sc, 'SELECT * FROM q_cov_valid.male_death_scores_p1')
female_risk_p1 <- sdf_sql(sc, 'SELECT * FROM q_cov_valid.female_death_scores_p1')
male_risk_p2 <- sdf_sql(sc, 'SELECT * FROM q_cov_valid.male_death_scores_p2')
female_risk_p2 <- sdf_sql(sc, 'SELECT * FROM q_cov_valid.female_death_scores_p2')


# join baseline risk to original risk
male_p1 <- left_join(base_male_risk_p1, male_risk_p1, by='census_person_id',
                     suffix = c('_base', '')) %>%
           select_cols() %>%
           mutate(relative_risk = score / baseline_score)

female_p1 <- left_join(base_female_risk_p1, female_risk_p1, by='census_person_id',
                       suffix = c('_base', '')) %>%
             select_cols() %>%
             mutate(relative_risk = score / baseline_score)

male_p2 <- left_join(base_male_risk_p2, male_risk_p2, by='census_person_id',
                     suffix = c('_base', '')) %>%
           select_cols() %>%
           mutate(relative_risk = score / baseline_score)

female_p2 <- left_join(base_female_risk_p2, female_risk_p2, by='census_person_id',
                       suffix = c('_base', '')) %>%
             select_cols() %>%
             mutate(relative_risk = score / baseline_score)


# Save the data

# Delete tables if they exist
sql <- paste0('DROP TABLE IF EXISTS q_cov_valid.relative_male_death_scores_p1')
DBI::dbExecute(sc, sql)

sql <- paste0('DROP TABLE IF EXISTS q_cov_valid.relative_female_death_scores_p1')
DBI::dbExecute(sc, sql)

sql <- paste0('DROP TABLE IF EXISTS q_cov_valid.relative_male_death_scores_p2')
DBI::dbExecute(sc, sql)

sql <- paste0('DROP TABLE IF EXISTS q_cov_valid.relative_female_death_scores_p2')
DBI::dbExecute(sc, sql)


# Create tables
sdf_register(male_p1, 'male_p1')
sql <- paste0('CREATE TABLE q_cov_valid.relative_male_death_scores_p1 AS SELECT * FROM male_p1')
DBI::dbExecute(sc, sql)

sdf_register(female_p1, 'female_p1')
sql <- paste0('CREATE TABLE q_cov_valid.relative_female_death_scores_p1 AS SELECT * FROM female_p1')
DBI::dbExecute(sc, sql)

sdf_register(male_p2, 'male_p2')
sql <- paste0('CREATE TABLE q_cov_valid.relative_male_death_scores_p2 AS SELECT * FROM male_p2')
DBI::dbExecute(sc, sql)

sdf_register(female_p2, 'female_p2')
sql <- paste0('CREATE TABLE q_cov_valid.relative_female_death_scores_p2 AS SELECT * FROM female_p2')
DBI::dbExecute(sc, sql)










