
  
library(tidyverse)
library(sparklyr)
library(survival)

########################
# Functions
source('/home/cdsw/q_covid_valid/_functions.R')
path <- '/home/cdsw/Results'

## function to get the metrics
get_metrics <- function(df, samp=0.02){
  set.seed(123)
  df <- df %>%
  mutate(risk_score = 100 - score)
  df_samp <- sample_data_df(df, frac_alive=0.02)
  
  # Concordance  
  cat("Concordance \n")
  conc <- concordance(Surv(t, death_covid) ~ risk_score, data = df, weight=df$weight)
  gc()
  # D-Stat
  cat(" D-Stat \n")
  d_stat <- D.index(x = df_samp$score,
                       surv.time = df_samp$t,
                       surv.event = df_samp$death_covid,
                       weights = df_samp$weight,
                       strat = rep(1, nrow(df_samp)))
  d_lci = d_stat$coef - 1.96 * d_stat$se
  d_uci = d_stat$coef + 1.96 * d_stat$se
  gc()
  # Brier score
  cat("Brier score \n")
  brier <- brier_score(pred=df$score / 100, true=df$death_covid)
  gc()
  
  # pseudo-R2 
  cat("pseudo-R2 \n")

   k = sqrt(8/pi)
  rsq <- (d_stat$coef^2/k^2)/(pi^2/6 + d_stat$coef^2/k^2)
  rsq_lci = (d_lci^2/k^2)/(pi^2/6 + d_lci^2/k^2)
  rsq_uci = (d_uci^2/k^2)/(pi^2/6 + d_uci^2/k^2)
  

  gc()
  
  # get results together
  res <- data.frame(
                    conc = conc$concordance,
                    conc_lci = conc$concordance - 1.96 * sqrt(conc$var),
                    conc_uci = conc$concordance + 1.96 * sqrt(conc$var),
                   d_stat = d_stat$coef,
                   d_lci = d_stat$coef - 1.96 * d_stat$se ,
                   d_uci = d_stat$coef + 1.96 * d_stat$se ,
                   rsq = rsq,
                   rsq_lci =  rsq_lci,
                   rsq_uci =  rsq_uci,
                   brier = brier)
  
  res$conc_ci  <- paste0(format(round(res$conc, 3), nsmall = 2), " (", 
                         format(round(res$conc_lci, 3), nsmall = 2), " to ",
                         format(round(res$conc_uci, 3), nsmall = 2), ")")
  res$d_ci  <- paste0(format(round(res$d_stat, 3), nsmall = 2), " (", 
                         format(round(res$d_lci, 3), nsmall = 2), " to ",
                         format(round(res$d_uci, 3), nsmall = 2), ")")
  res$rsq_ci  <- paste0(format(round(res$rsq, 3), nsmall = 2), " (", 
                         format(round(res$rsq_lci, 3), nsmall = 2), " to ",
                         format(round(res$rsq_uci, 3), nsmall = 2), ")")
  
  return(res)
}

## wrapper function
get_metrics_table <- function(df){
  metrics_all <- get_metrics(df) %>%
                  mutate(var="all", x="all")

metrics_eth <- map_dfr(unique(df$ethnicity_9), function(x){
#  if(x != "White British"){
#      samp=0.2
#  }else{
#   samp=0.02 
#  }
  print(x)
  res <- c_data %>%
           filter(ethnicity_9 == x) %>%
           get_metrics(samp=samp) %>%
           mutate(var = as.character(x))
  return(res)
})%>%
  mutate(x="ethnicity")%>%
  arrange(var)
    gc()                 
metrics_town_q <- map_dfr(unique(df$town_q), function(x){
  print(x)
  res <- c_data %>%
           filter(town_q == x) %>%
           get_metrics() %>%
           mutate(var = as.character(x))
  return(res)
})%>%
  mutate(x="Townsend")%>%
  arrange(var)
gc()
metrics_age_group<- map_dfr(unique(df$age_group), function(x){
  print(x)
  res <- c_data %>%
           filter(age_group == x) %>%
           get_metrics() %>%
           mutate(var = as.character(x))
  return(res)
})%>%
  mutate(x="age")%>%
  arrange(var)
  gc()
  res <- rbind(metrics_all,metrics_age_group,metrics_eth,metrics_town_q  )
  
  res$conc_ci  <- paste0(format(round(res$conc, 3), nsmall = 2), " (", 
                         format(round(res$conc_lci, 3), nsmall = 2), " to ",
                         format(round(res$conc_uci, 3), nsmall = 2), ")")
  res$d_ci  <- paste0(format(round(res$d_stat, 3), nsmall = 2), " (", 
                         format(round(res$d_lci, 3), nsmall = 2), " to ",
                         format(round(res$d_uci, 3), nsmall = 2), ")")
  res$rsq_ci  <- paste0(format(round(res$rsq, 3), nsmall = 2), " (", 
                         format(round(res$rsq_lci, 3), nsmall = 2), " to ",
                         format(round(res$rsq_uci, 3), nsmall = 2), ")")
  res
}


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

alive <- 0.50

######################
### Study period 1 ###
######################

#### only people who linked to GPES ####

### males ###
c_data <- sdf_sql(sc, 'SELECT * FROM q_cov_valid.male_death_scores_p1') %>%
            filter(linked_gpes == 1) %>%
            select(-linked_gpes, -region, -census_person_id, -tpp)
c_data <- sample_data(c_data, frac_alive = alive)
c_data <- create_age_groups(c_data)


#metrics <- get_metrics(c_data) 
metrics <- get_metrics_table(c_data) 
write_csv(metrics, paste0(path, "/metrics_m_p1.csv"))
rm(c_data)
gc()

### females ###
c_data <- sdf_sql(sc, 'SELECT * FROM q_cov_valid.female_death_scores_p1') %>%
            filter(linked_gpes == 1) %>%
            select(-linked_gpes, -region, -census_person_id, -tpp)
c_data <- sample_data(c_data, frac_alive = alive)
c_data <- create_age_groups(c_data)


metrics <- get_metrics_table(c_data) 
#metrics <- get_metrics(c_data) 


write_csv(metrics, paste0(path, "/metrics_f_p1.csv"))
rm(c_data)
gc()


######################
### Study period 2 ###
######################

#### only people who linked to GPES ####

### males ###
c_data <- sdf_sql(sc, 'SELECT * FROM q_cov_valid.male_death_scores_p2') %>%
            filter(linked_gpes == 1) %>%
            select(-linked_gpes, -region, -census_person_id, -tpp)
c_data <- sample_data(c_data, frac_alive = alive)
c_data <- create_age_groups(c_data)

metrics <- get_metrics_table(c_data) 
write_csv(metrics, paste0(path, "/metrics_m_p2.csv"))
rm(c_data)
gc()

### females ###
c_data <- sdf_sql(sc, 'SELECT * FROM q_cov_valid.female_death_scores_p2') %>%
            filter(linked_gpes == 1) %>%
            select(-linked_gpes, -region, -census_person_id, -tpp)
c_data <- sample_data(c_data, frac_alive = alive)
c_data <- create_age_groups(c_data)


#metrics <- get_metrics(c_data) 

metrics <- get_metrics_table(c_data) 
write_csv(metrics, paste0(path, "/metrics_f_p2.csv"))
rm(c_data)
gc()


## combine all metrics ##


m_m_p1 <- read.csv(paste0(path, "/metrics_m_p1.csv"))%>%
  filter(x=="all") %>%
  mutate(x="Males, P1")
m_f_p1 <- read.csv(paste0(path, "/metrics_f_p1.csv"))%>%
  filter(x=="all") %>%
  mutate(x="Females, P1")

m_m_p2 <- read.csv(paste0(path, "/metrics_m_p2.csv"))%>%
  filter(x=="all") %>%
  mutate(x="Males, P2")
m_f_p2 <- read.csv(paste0(path, "/metrics_f_p2.csv"))%>%
  filter(x=="all") %>%
  mutate(x="Females, P2")

metrics <- rbind(m_m_p1, m_f_p1,m_m_p2, m_f_p2)
print(metrics)
write_csv(metrics, paste0(path, "/metrics_summary.csv"))





####--------------------------------####
#### only people  registered in TPP ####
####--------------------------------####



### Study period 1 ###

### males ###
c_data <- sdf_sql(sc, 'SELECT * FROM q_cov_valid.male_death_scores_p1') %>%
            filter(linked_gpes == 1, tpp == 1) %>%
            select(-linked_gpes, -region, -census_person_id, -tpp)
c_data <- sample_data(c_data, frac_alive=0.5)
c_data <- create_age_groups(c_data)


metrics <- get_metrics(c_data) 

write_csv(metrics, paste0(path, "/metrics_tpp_m_p1.csv"))
rm(c_data)
gc()

### females ###
c_data <- sdf_sql(sc, 'SELECT * FROM q_cov_valid.female_death_scores_p1') %>%
            filter(linked_gpes == 1, tpp == 1) %>%
            select(-linked_gpes, -region, -census_person_id, -tpp)
c_data <- sample_data(c_data, frac_alive=0.5)
c_data <- create_age_groups(c_data)


metrics <- get_metrics(c_data) 

write_csv(metrics, paste0(path, "/metrics_tpp_f_p1.csv"))
rm(c_data)
gc()



### Study period 2 ###

### males ###
c_data <- sdf_sql(sc, 'SELECT * FROM q_cov_valid.male_death_scores_p2') %>%
            filter(linked_gpes == 1, tpp == 1) %>%
            select(-linked_gpes, -region, -census_person_id, -tpp)
c_data <- sample_data(c_data, frac_alive=0.5)
c_data <- create_age_groups(c_data)


metrics <- get_metrics(c_data) 

write_csv(metrics, paste0(path, "/metrics_tpp_m_p2.csv"))
rm(c_data)
gc()

### females ###
c_data <- sdf_sql(sc, 'SELECT * FROM q_cov_valid.female_death_scores_p2') %>%
            filter(linked_gpes == 1, tpp == 1) %>%
            select(-linked_gpes, -region, -census_person_id, -tpp)
c_data <- sample_data(c_data, frac_alive=0.5)
c_data <- create_age_groups(c_data)


metrics <- get_metrics(c_data) 

write_csv(metrics, paste0(path, "/metrics_tpp_f_p2.csv"))
rm(c_data)
gc()




#### summary of results ####

m_m_p1 <- read.csv(paste0(path, "/metrics_tpp_m_p1.csv"))%>%
  mutate(x="Males, P1")

m_f_p1 <- read.csv(paste0(path, "/metrics_tpp_f_p1.csv"))%>%
  mutate(x="Females, P1")

m_m_p2 <- read.csv(paste0(path, "/metrics_tpp_m_p2.csv"))%>%
  mutate(x="Males, P2")

m_f_p2 <- read.csv(paste0(path, "/metrics_tpp_f_p2.csv"))%>%
  mutate(x="Females, P2")

metrics <- rbind(m_m_p1, m_f_p1, m_m_p2, m_f_p2)
print(metrics)
write_csv(metrics, paste0(path, "/metrics_tpp_summary.csv"))



