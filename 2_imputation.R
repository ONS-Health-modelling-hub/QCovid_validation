# Load packages
library(dplyr)
library(sparklyr)
library(tidyverse)

########################
#Set up the spark connection
#########################

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

# Check connection is open
spark_connection_is_open(sc)

###############
#Get data frames
###############

# To find what tables are in the database
tbl_change_db(sc, "q_cov_valid")
#src_tbls(sc)

# Create connection to main analytical dataset
df <- sdf_sql(sc, "SELECT * FROM  linked_census_qcovid")


# set to 0 conditions that are unrealistically high

df <- df %>%
mutate(b_sicklecelldisease=0,
      p_marrow6 =0)

#desc <- df %>%
#        summarise(n=n(), sum(linked_gpes), mean(linked_gpes))%>%
#        collect()

# Have a look at the data
#df %>%
#head(10) %>%
#collect() %>%
#print()

# Dimensions
#n_row <- df %>% sdf_nrow()
#n_row
#
#n_col <- df %>% sdf_ncol()
#n_col

# Summary
#summary <- df %>% sdf_describe()
#print(summary)
#
#summary1 <- df %>%
#summarise_all(~max(.)) %>%
#collect()
#print(summary1)
#
## Missingness
#na_per_col <- df %>%
#summarise_all(~sum(as.integer(is.na(.)))) %>%
#collect()
#print(na_per_col)
#
## Variable classes
#str(df %>% head(1) %>% collect())

# Change some variable classes and create dummy vars
df <- df %>%
mutate(diabetes0 = ifelse(diabetes_cat == 0, 1, 0),
       diabetes1 = ifelse(diabetes_cat == 1, 1, 0),
       diabetes2 = ifelse(diabetes_cat == 2, 1, 0),
       chemocat0 = ifelse(chemocat == 0, 1, 0),
       chemocat1 = ifelse(chemocat == 1, 1, 0),
       chemocat2 = ifelse(chemocat == 2, 1, 0),
       chemocat3 = ifelse(chemocat == 3, 1, 0),
       ethnicity0 = ifelse(ethnicity == 0, 1, 0),
       ethnicity1 = ifelse(ethnicity == 1, 1, 0),
       ethnicity2 = ifelse(ethnicity == 2, 1, 0),
       ethnicity3 = ifelse(ethnicity == 3, 1, 0),
       ethnicity4 = ifelse(ethnicity == 4, 1, 0),
       ethnicity5 = ifelse(ethnicity == 5, 1, 0),
       ethnicity6 = ifelse(ethnicity == 6, 1, 0),
       ethnicity7 = ifelse(ethnicity == 7, 1, 0),
       ethnicity8 = ifelse(ethnicity == 8, 1, 0),
       ethnicity9 = ifelse(ethnicity == 9, 1, 0),
       ethnicity10 = ifelse(ethnicity == 10, 1, 0),
       ethnicity11 = ifelse(ethnicity == 11, 1, 0),
       ethnicity12 = ifelse(ethnicity == 9, 1, 0),
       ethnicity13 = ifelse(ethnicity == 13, 1, 0),
       ethnicity14 = ifelse(ethnicity == 14, 1, 0),
       ethnicity15 = ifelse(ethnicity == 15, 1, 0),
       ethnicity16 = ifelse(ethnicity == 16, 1, 0),
       homecat0 = ifelse(homecat == 0, 1, 0),
       homecat1 = ifelse(homecat == 1, 1, 0),
       homecat2 = ifelse(homecat == 2, 1, 0),
       learncat0 = ifelse(learncat == 0, 1, 0),
       learncat1 = ifelse(learncat == 1, 1, 0),
       learncat2 = ifelse(learncat == 2, 1, 0),
       renalcat0 = ifelse(renalcat == 0, 1, 0),
       renalcat1 = ifelse(renalcat == 1, 1, 0),
       renalcat2 = ifelse(renalcat == 2, 1, 0),
       renalcat3 = ifelse(renalcat == 3, 1, 0),
       renalcat4 = ifelse(renalcat == 4, 1, 0),
       renalcat5 = ifelse(renalcat == 5, 1, 0),
       renalcat6 = ifelse(renalcat == 6, 1, 0),
      bmi = ifelse(bmi == 0, NA, bmi))

# Variable classes again
#str(df %>% head(1) %>% collect())

## Missingness again
na_per_col <- df %>%
summarise_all(~sum(as.integer(is.na(.)))) %>%
collect()
print(na_per_col)

## Compare imputation method for cases where bmi label could be used vs lm
#df_bmi_label <- df %>%
#  filter(!is.na(bmi) & !is.na(bmi_cat)) %>%
#  count() %>%
#  collect()


#df_bmi_label %>%
#head(10) %>%
#collect() %>%
#print()

#partitions <- sdf_partition(df_bmi_label, training = 0.7, test = 0.3)
#df_bmi_label_train <- partitions$training
#df_bmi_label_test <- partitions$test
#
#fit_male <- df_bmi_label_train %>%
#filter(sex == 1) %>%
#mutate(age2 = age^2,
#      age3 = age^3) %>%
#select(-chemocat,
#      -ethnicity,
#      -homecat,
#      -learncat,
#      -renalcat,
#      -bmi_cat,
#      -nhs_number,
#      -census_person_id,
#      -linked_gpes,
#      -p_marrow6_gpes) %>%
#ml_linear_regression(., bmi ~ . + sex:age + age:.)
#
#fit_female <- df_bmi_label_train %>%
#filter(sex == 0) %>%
#mutate(age2 = age^2,
#      age3 = age^3) %>%
#select(-chemocat,
#      -ethnicity,
#      -homecat,
#      -learncat,
#      -renalcat,
#      -bmi_cat,
#      -nhs_number,
#      -census_person_id,
#      -linked_gpes,
#      -p_marrow6_gpes) %>%
#ml_linear_regression(., bmi ~ . + sex:age + age:.)
#
#summary(fit_male)
#summary(fit_female)
#
#pred_male <- df_bmi_label_test %>%
#filter(sex == 1) %>%
#mutate(age2 = age^2,
#      age3 = age^3) %>%
#ml_predict(fit_male, .) %>%
#rename(prediction_lm_male = prediction)
#
#pred_female <- df_bmi_label_test %>%
#filter(sex == 0) %>%
#mutate(age2 = age^2,
#      age3 = age^3) %>%
#ml_predict(fit_female, .) %>%
#rename(prediction_lm_female = prediction)
#
## pred %>% head(10) %>% collect() %>% print()
#
#df_bmi_label_test <- df_bmi_label_test %>%
#left_join(., pred_male) %>%
#left_join(., pred_female)
#
#df_bmi_label_test <- df_bmi_label_test %>%
#mutate(prediction_lm = prediction_lm_male) %>%
#mutate(prediction_lm = ifelse(is.na(prediction_lm),
#                              prediction_lm_female,
#                              prediction_lm))
#
#mean_bmi_by_label <- df_bmi_label_train %>%
#group_by(bmi_cat, sex) %>%
#summarise(prediction_mm = mean(bmi))
#
#df_bmi_label_test <- df_bmi_label_test %>%
#left_join(., mean_bmi_by_label, by = c("bmi_cat", "sex"))
#
## df_bmi_label_test %>% head(10) %>% collect() %>% print()
#
#mse <- df_bmi_label_test %>%
#mutate(se_lm = (bmi - prediction_lm)^2,
#      se_mm = (bmi - prediction_mm)^2) %>%
#summarise(mse_lm = mean(se_lm), mse_mm = mean(se_mm))
#
#mse %>% collect() %>% print()
#
# As MSE of means of bmi_cat is lower, firstly impute
# missing bmi where bmi_cat is present

mean_bmi_by_label <- df %>%
filter(!is.na(bmi) & !is.na(bmi_cat)) %>%
group_by(bmi_cat, sex) %>%
summarise(prediction_mm = mean(bmi))

df <- df %>%
left_join( mean_bmi_by_label, by = c("bmi_cat", "sex"))

df <- df %>%
mutate(bmi = ifelse(is.na(bmi) & !is.na(bmi_cat), prediction_mm, bmi)) %>%
select(-prediction_mm)

#
#### Missingness again
#na_per_col <- df %>%
#summarise_all(~sum(as.integer(is.na(.)))) %>%
#collect()
#print(na_per_col)

# Linear regression of BMI on all vars
bmi_fit_male <- df %>%
filter(!is.na(bmi) & sex == 1) %>%
mutate(age2 = age^2,
      age3 = age^3) %>%
select(-chemocat,
      -ethnicity,
      -homecat,
      -learncat,
      -renalcat,
      -bmi_cat,
      -nhs_number,
      -census_person_id,
        -death_date,
      -linked_gpes,
       -tpp,
       -town_q, -ethnicity_9, -region,-pr_link,
      -sex,- death_covid, -t) %>%
ml_linear_regression(., bmi ~ . + age:.)

bmi_fit_female <- df %>%
filter(!is.na(bmi) & sex == 0) %>%
mutate(age2 = age^2,
      age3 = age^3) %>%
select(-chemocat,
      -ethnicity,
      -homecat,
      -learncat,
      -renalcat,
      -bmi_cat,
      -nhs_number,
      -census_person_id,
       -death_date,
      -linked_gpes,
        -tpp,
              -town_q, -ethnicity_9, -region,-pr_link,
      -sex,- death_covid, -t) %>%
ml_linear_regression(., bmi ~ . + age:.)

#summary(bmi_fit_male)
#summary(bmi_fit_female)

# Predict missing values
predicted_bmi_male <- df %>%
filter(is.na(bmi) & sex == 1) %>%
mutate(age2 = age^2,
      age3 = age^3) %>%
select(-bmi) %>%
ml_predict(bmi_fit_male, .) %>%
rename(bmi = prediction) %>%
select(-age2,
      -age3)

predicted_bmi_female <- df %>%
filter(is.na(bmi) & sex == 0) %>%
mutate(age2 = age^2,
      age3 = age^3) %>%
select(-bmi) %>%
ml_predict(bmi_fit_female, .) %>%
rename(bmi = prediction) %>%
select(-age2,
      -age3)

# Compare summary stats of actual vs predicted bmi
#df %>%
#filter(!is.na(bmi) & sex == 1) %>%
#select(bmi) %>%
#sdf_describe() %>%
#print()
#
#predicted_bmi_male %>%
#select(bmi) %>%
#sdf_describe() %>%
#print()
#
#df %>%
#filter(!is.na(bmi) & sex == 0) %>%
#select(bmi) %>%
#sdf_describe() %>%
#print()
#
#predicted_bmi_female %>%
#select(bmi) %>%
#sdf_describe() %>%
#print()
#
## Combine the data
df_complete_cases <- df %>%
filter(!is.na(bmi))

df <- sdf_bind_rows(df_complete_cases, predicted_bmi_male) %>%
sdf_bind_rows(., predicted_bmi_female)
#
## Look at the data
#df %>%
#head(10) %>%
#collect() %>%
#print()
#
## Dimensions again
#n_row_new <- df %>% sdf_nrow()
#n_row_new
#n_row
#
#n_col_new <- df %>% sdf_ncol()
#n_col_new
#
## Missingness again
na_per_col_new <- df %>%
summarise_all(~sum(as.integer(is.na(.)))) %>%
collect()
print(na_per_col_new)

#df %>%
#mutate(na_bmi = ifelse(is.na(bmi),1,0))%>%
#summarise(sum(na_bmi))%>%
#collect()


# Delete table
sql <- paste0('DROP TABLE IF EXISTS q_cov_valid.linked_census_qcovid_bmi_imp_pp')
DBI::dbExecute(sc, sql)

sdf_register(df, 'df')
sql <- paste0('CREATE TABLE q_cov_valid.linked_census_qcovid_bmi_imp_pp AS SELECT * FROM df')
DBI::dbExecute(sc, sql) 

