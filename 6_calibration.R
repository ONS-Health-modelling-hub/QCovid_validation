library(tidyverse)
library(sparklyr)
library(survival)

########################
# Functions
source('/home/cdsw/q_covid_valid/_functions.R')
path <- '/home/cdsw/Results'


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

#### Period 1 ####
### males ###
df <- sdf_sql(sc, 'SELECT * FROM q_cov_valid.male_death_scores_p1') %>%
            filter(linked_gpes == 1) %>%
            select(t, death_covid, score)%>%
            collect()

q_thres = df %>%
          mutate(q  = ntile(score, 20))%>%
          group_by(q)%>%
          summarise(min = min(score),
                   deaths = sum(death_covid),
                   total = n())%>%
          arrange(desc(q))%>%
          ungroup() %>%
          mutate(cum_deaths = cumsum(deaths),
                 cum_deaths_p = cum_deaths/sum(deaths))

write_csv(q_thres , paste0(path, "/q_thres_m_p1.csv"))

### females ###
df <- sdf_sql(sc, 'SELECT * FROM q_cov_valid.female_death_scores_p1') %>%
            filter(linked_gpes == 1) %>%
            select(t, death_covid, score)%>%
            collect()

q_thres = df %>%
          mutate(q  = ntile(score, 20))%>%
          group_by(q)%>%
          summarise(min = min(score),
                   deaths = sum(death_covid),
                   total = n())%>%
          arrange(desc(q))%>%
          ungroup() %>%
          mutate(cum_deaths = cumsum(deaths),
                 cum_deaths_p = cum_deaths/sum(deaths))

write_csv(q_thres , paste0(path, "/q_thres_f_p1.csv"))


#### Period 2 ####
### males ###
df <- sdf_sql(sc, 'SELECT * FROM q_cov_valid.male_death_scores_p2') %>%
            filter(linked_gpes == 1) %>%
            select(t, death_covid, score)%>%
            collect()

q_thres = df %>%
          mutate(q  = ntile(score, 20))%>%
          group_by(q)%>%
          summarise(min = min(score),
                   deaths = sum(death_covid),
                   total = n())%>%
          arrange(desc(q))%>%
          ungroup() %>%
          mutate(cum_deaths = cumsum(deaths),
                 cum_deaths_p = cum_deaths/sum(deaths))
write_csv(q_thres , paste0(path, "/q_thres_m_p2.csv"))

### females ###
df <- sdf_sql(sc, 'SELECT * FROM q_cov_valid.female_death_scores_p2') %>%
            filter(linked_gpes == 1) %>%
            select(t, death_covid, score)%>%
            collect()

q_thres = df %>%
          mutate(q  = ntile(score, 20))%>%
          group_by(q)%>%
          summarise(min = min(score),
                   deaths = sum(death_covid),
                   total = n())%>%
          arrange(desc(q))%>%
          ungroup() %>%
          mutate(cum_deaths = cumsum(deaths),
                 cum_deaths_p = cum_deaths/sum(deaths))

write_csv(q_thres , paste0(path, "/q_thres_f_p2.csv"))
