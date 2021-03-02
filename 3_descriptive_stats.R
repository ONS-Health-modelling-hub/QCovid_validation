library(tidyverse)
library(sparklyr)


########################
#Set up the spark connection
#########################

config <- spark_config() 
config$spark.dynamicAllocation.maxExecutors <- 30
config$spark.executor.cores <- 5
config$spark.executor.memory <- "20g"
config$spark.driver.maxResultSize <- "10g"
sc <- spark_connect(master = "yarn-client",
                    app_name = "R_Example",
                    config = config,
                    version = "2.3.0")



# if using ft_bucketizer: buckets are defined by [x,y)

linked_data <- sdf_sql(sc, "SELECT * FROM  q_cov_valid.linked_census_qcovid") %>%
  filter(linked_gpes == 1) %>%
  mutate(bmi_category = case_when(
               bmi >= 18.5 & bmi < 25 | bmi_cat == "IDEAL" ~ "18.5 to 25",
               bmi < 18.5 | bmi_cat == "UNDERWEIGHT" ~ "< 18.5",
               bmi >= 25 & bmi < 30 | bmi_cat == "OVERWEIGHT" ~ "25 to 30",
               bmi >= 30  | bmi_cat == "OBESE" ~ ">= 30",
               is.na(bmi) & (is.na(bmi_cat)|bmi_cat == "UNDER 20 NULLS") ~ "Missing")) %>%
  mutate(age_group = case_when(
               age >= 19 & age < 30 ~ '19 - 29 years',
               age >= 30 & age < 40 ~ '30 - 39 years',
               age >= 40 & age < 50 ~ '40 - 49 years',
               age >= 50 & age < 60 ~ '50 - 59 years',
               age >= 60 & age < 70 ~ '60 - 69 years',
               age >= 70 & age < 80 ~ '70 - 79 years',
               age >= 80 & age < 90 ~ '80 - 89 years',
               age >= 90 ~ '90+ years'))


# set to 0 variables with too high counts
linked_data  <-linked_data  %>%
mutate(b_sicklecelldisease=0,
      p_marrow6 =0)
#linked_data  %>%
#group_by(diabetes_cat)%>%
#summarise(n=n())%>%
#collect()




linked_data_p1 <- linked_data %>%
  filter(!is.na(death_date)) %>%
  filter(death_date >= '2020-01-24', death_date <= '2020-04-30')


linked_data_p2 <- linked_data %>%
  filter(!is.na(death_date)) %>%
  filter(death_date >= '2020-05-01', death_date <= '2020-07-28')



#count_na <- function(x){sum(is.na(x))}
#d <- collect(test)
#na_summary <- d %>%
#  summarise_all(count_na) %>%
#  t()


get_tables_per_var <- function(df, vars){
  
  res <- list()
  
  for (v in vars){
    print(v)
    
    v <- as.symbol(v)
    v_str <- as.character(v)
    
    df_var <- df %>%
      select(v)
    
    continuous_vars <- c('bmi', 'age', 't')
    
    if (!(v_str %in% continuous_vars)){
      df_summary <- df_var %>%
        select(v) %>%
        group_by(v) %>%
        tally() %>%
        collect() %>%
        as.data.frame()
      
      df_summary$percentage <- df_summary$n / sum(df_summary$n) * 100
      df_summary$percentage <- round(df_summary$percentage, 2)
      df_summary$n <- format(df_summary$n, big.mark=",", scientific=FALSE)
      df_summary[[v_str]] <- as.character(df_summary[[v_str]])
      
      new_col <- rep('temp', nrow(df_summary))
      n <- df_summary$n
      p <- df_summary$p
      
      for (i in 1:length(new_col)){
        new_col[i] <- paste0(n[i], ' (', format(p[i], nsmall = 2), ')')
      }
      
      df_summary[['Count (n)']] <- new_col
      df_summary <- select(df_summary, -n, -percentage)
      
    }
    
    else if (v_str %in% continuous_vars){
      df_summary <- df_var %>%
      sdf_describe(cols=v_str) %>%
      filter(summary=='stddev' | summary=='mean') %>%
      collect() %>%
      t() %>%
      as.data.frame()
      
      colnames(df_summary) <- c('mean', 'sd')
      df_summary <- df_summary[-1,]
      df_summary$sd <- as.numeric(as.character(df_summary$sd))
      df_summary$sd <- round(df_summary$sd, 2)
      df_summary$sd <- format(df_summary$sd, big.mark=",", scientific=FALSE)
      df_summary$mean <- as.numeric(as.character(df_summary$mean))
      df_summary$mean <- round(df_summary$mean, 2)
      df_summary$mean <- format(df_summary$mean, big.mark=",", scientific=FALSE)
      
      
      new_col <- rep('temp', nrow(df_summary))
      mean <- df_summary$mean
      sd <- df_summary$sd
      
      for (i in 1:length(new_col)){
        new_col[i] <- paste0(mean[i], ' (', sd[i], ')')
      }
      
      df_summary[['Mean (SD)']] <- new_col
      df_summary <- select(df_summary, -mean, -sd)
      
    }
    
    df_summary$variable <- rep(v_str, nrow(df_summary))
    
    res <- append(res, list(df_summary))
    
    rm(df_var, df_summary)
    gc()
    
  }
  
  return(res)
}


ignore <- c('death_covid', 
            'census_person_id',
            'nhs_number', 
            'town',
            'linked_gpes',
            'nhs_number',
            'death_date')

columns <- colnames(linked_data)[!(colnames(linked_data) %in% ignore)][8]

sum_stats <- get_tables_per_var(linked_data, columns)

sum_stats_covid_p1 <- get_tables_per_var(filter(linked_data_p1, death_covid==1), 
                                         columns)

sum_stats_covid_p2 <- get_tables_per_var(filter(linked_data_p2, death_covid==1), 
                                         columns)


path <- '/home/cdsw/Results'

if (!file.exists(path)){
  dir.create(path)
}

saveRDS(sum_stats, paste0(path, '/summary_stats_list.rds'))
saveRDS(sum_stats_covid_p1, paste0(path, '/summary_stats_covid_p1_list.rds'))
saveRDS(sum_stats_covid_p2, paste0(path, '/summary_stats_covid_p2_list.rds'))



sum_stats <- readRDS(paste0(path,'/summary_stats_list.rds'))
sum_stats_covid_p1 <- readRDS(paste0(path,'/summary_stats_covid_p1_list.rds'))
sum_stats_covid_p2 <- readRDS(paste0(path,'/summary_stats_covid_p2_list.rds'))



continuous_vars <- c('age', 't', 'bmi')

ind <- c(4,6,42) # This needs to be updated as the data changes

cat <- sum_stats[-ind]
con <- sum_stats[ind]

cat_covid_p1 <- sum_stats_covid_p1[-ind]
con_covid_p1 <- sum_stats_covid_p1[ind]

cat_covid_p2 <- sum_stats_covid_p2[-ind]
con_covid_p2 <- sum_stats_covid_p2[ind]

for (i in 1:length(cat)){
  colnames(cat[[i]]) <- c('value', 'Count (n)', 'variable')
}

for (i in 1:length(cat_covid_p1)){
  colnames(cat_covid_p1[[i]]) <- c('value', 'Count (n)', 'variable')
}

for (i in 1:length(cat_covid_p2)){
  colnames(cat_covid_p2[[i]]) <- c('value', 'Count (n)', 'variable')
}

cat <- do.call('rbind', cat)
con <- do.call('rbind', con)

cat_covid_p1 <- do.call('rbind', cat_covid_p1)
con_covid_p1 <- do.call('rbind', con_covid_p1)

cat_covid_p2 <- do.call('rbind', cat_covid_p2)
con_covid_p2 <- do.call('rbind', con_covid_p2)

write_csv(cat, paste0(path, '/cat.csv'))
write_csv(con, paste0(path, '/cont.csv'))

write_csv(cat_covid_p1, paste0(path, '/cat_covid_p1.csv'))
write_csv(con_covid_p1, paste0(path, '/cont_covid_p1.csv'))

write_csv(cat_covid_p2, paste0(path, '/cat_covid_p2.csv'))
write_csv(con_covid_p2, paste0(path, '/cont_covid_p2.csv'))
