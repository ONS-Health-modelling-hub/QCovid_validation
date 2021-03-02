library(tidyverse)
library(sparklyr)
library(survival)
library(extrafont)
loadfonts()

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
df <- sdf_sql(sc, 'SELECT * FROM q_cov_valid.relative_male_death_scores_p1') %>%
            filter(linked_gpes == 1) %>%
            select(t, death_covid, relative_risk)%>%
            collect()

cal_m = df %>%
          mutate(q  = ntile(relative_risk, 20))%>%
          group_by(q)%>%
          summarise(min = min(relative_risk),
                    predicted = mean(relative_risk),
                   observed = mean(death_covid)*100,
                   total = n()) %>%
          select(q,Predicted=predicted, Observed = observed)%>%
          gather(value="value", key="type",-q)%>%
           mutate(sex="Males", period=1)

plt_m <- ggplot(cal_m , aes(x=q, y=value, colour=type ))+
geom_point()+
labs(x="twentieth of predicted risk at 97 days",
    y="Risk of COVID19 death (%)",
    title="Men")+
theme_bw()+scale_colour_discrete(name="")



### females ###
df <- sdf_sql(sc, 'SELECT * FROM q_cov_valid.relative_female_death_scores_p1') %>%
            filter(linked_gpes == 1) %>%
            select(t, death_covid, relative_risk)%>%
            collect()


cal_f = df %>%
           mutate(q  = ntile(relative_risk, 20))%>%
          group_by(q)%>%
          summarise(min = min(relative_risk),
                    predicted = mean(relative_risk),
                   observed = mean(death_covid)*100,
                   total = n()) %>%
          select(q,Predicted=predicted, Observed = observed)%>%
          gather(value="value", key="type",-q)%>%
           mutate(            sex="Females", period=1)

plt_f <- ggplot(cal_f , aes(x=q, y=value, colour=type ))+
geom_point()+
labs(x="twentieth of predicted risk at 97 days",
    y="Risk of COVID19 death (%)", 
    title="Women")+
theme_bw()+scale_colour_discrete(name="")


write_csv(rbind(cal_m, cal_f) , paste0(path, "/relative_risk_calibration.csv"))


plt_cal <- ggpubr::ggarrange(plt_f, plt_m,  ncol = 2, nrow=1, common.legend = TRUE, legend = "top")+
 theme(text=element_text(family="Times New Roman", size=10))+
    ggsave( paste0(path,"/relative_risk_cal_p1.png"), width= 5*16/9, height= 5, dpi=600 )
plt_cal
