library(tidyverse)
library(sparklyr)
library(survival)

########################
# Functions


########################
#Set up the spark connection
#########################

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
