# Load packages
library(dplyr)
library(sparklyr)
library(tidyverse)
library(survival)
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
# Check config
spark_config(sc)



#source("score_test.R")
#### period 1 : 24/01/2020 to 30/04/2020 ####
start_date_1 =  "2020-01-24"
eos_date_1 = "2020-04-30"
# read in data #
df <- sdf_sql(sc, "SELECT * FROM  q_cov_valid.linked_census_qcovid_bmi_imp_pp")



df <- df %>%
     # left_join(df2, by = "census_person_id")%>%
      mutate(death_covid = ifelse(death_date <= eos_date_1 & death_covid == 1 ,1, 0  ),
             t = ifelse(!is.na(death_date) & death_date < eos_date_1 ,
                    datediff(death_date, start_date_1),
                    datediff(eos_date_1, start_date_1)))



# scores for females #
female_death_scores <- get_female_death_scores(df)

# scores for males #
male_death_scores <- get_male_death_scores(df)


# Save the data

# Delete table
sql <- paste0('DROP TABLE IF EXISTS q_cov_valid.female_death_scores_p1')
DBI::dbExecute(sc, sql)


# Delete table
sql <- paste0('DROP TABLE IF EXISTS q_cov_valid.male_death_scores_p1')
DBI::dbExecute(sc, sql)



sdf_register(female_death_scores, 'female_death_scores')
sql <- paste0('CREATE TABLE q_cov_valid.female_death_scores_p1 AS SELECT * FROM female_death_scores')
DBI::dbExecute(sc, sql)

sdf_register(male_death_scores, 'male_death_scores')
sql <- paste0('CREATE TABLE q_cov_valid.male_death_scores_p1 AS SELECT * FROM male_death_scores')
DBI::dbExecute(sc, sql)



#### period 2 : 01/05/2020 to 28/07/2020 ####
start_date =  "2020-05-01"
eos_date = "2020-07-28"
# read in data #
df <- sdf_sql(sc, "SELECT * FROM  q_cov_valid.linked_census_qcovid_bmi_imp_pp")

df <- df %>%
     # left_join(df2, by = "census_person_id")%>%
      filter(is.na(death_date)|death_date >= start_date)%>%
      mutate(death_covid = ifelse(death_date <= eos_date & death_covid == 1 ,1, 0  ),
             t = ifelse(!is.na(death_date) & death_date < eos_date ,
                    datediff(death_date, start_date),
                    datediff(eos_date, start_date)))

# scores for females #
female_death_scores <- get_female_death_scores(df)



# scores for males #
male_death_scores <- get_male_death_scores(df)


# Save the data

# Delete table
sql <- paste0('DROP TABLE IF EXISTS q_cov_valid.female_death_scores_p2')
DBI::dbExecute(sc, sql)


# Delete table
sql <- paste0('DROP TABLE IF EXISTS q_cov_valid.male_death_scores_p2')
DBI::dbExecute(sc, sql)



sdf_register(female_death_scores, 'female_death_scores')
sql <- paste0('CREATE TABLE q_cov_valid.female_death_scores_p2 AS SELECT * FROM female_death_scores')
DBI::dbExecute(sc, sql)

sdf_register(male_death_scores, 'male_death_scores')
sql <- paste0('CREATE TABLE q_cov_valid.male_death_scores_p2 AS SELECT * FROM male_death_scores')
DBI::dbExecute(sc, sql)
