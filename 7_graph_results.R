library(tidyverse)
library(extrafont)
path <- '/home/cdsw/Results'

if (!file.exists(path)){
  dir.create(path)
}


#### Period 1 ####
## Females ##
metrics_f <- read.csv( paste0(path, '/metrics_f_p1.csv'))
conc_f = as.character(metrics_f$conc_ci[[1]])
metrics_f_age <- metrics_f %>%
filter(x == "age")%>% 
   mutate(conc = as.numeric(as.character(conc)),
          age_gr= gsub("[()]", "", var) ,
          age_gr=gsub("]]", "",age_gr),
           age_gr=gsub(",", "-",age_gr),
           age_gr=gsub("[[]", " ",age_gr)
          )

c_f <- ggplot(metrics_f_age, aes(x=age_gr, y=conc))+
          geom_point()+
          geom_errorbar(aes(ymin=conc_lci, ymax=conc_uci),width=0.1)+
          labs(title = paste0("Females - C-Stat:", conc_f),
              x="Age", y="Concordance Index")+
          theme_bw()+ theme(axis.text.x = element_text(angle = 45, hjust = 1)) 


## males ##

metrics_m <- read.csv( paste0(path, '/metrics_m_p1.csv'))
conc_m = as.character(metrics_m$conc_ci[[1]])
metrics_m_age <- metrics_m %>%
filter(x == "age")%>% 
   mutate(conc = as.numeric(as.character(conc)),
          age_gr= gsub("[()]", "", var) ,
          age_gr=gsub("]]", "",age_gr),
           age_gr=gsub(",", "-",age_gr),
           age_gr=gsub("[[]", " ",age_gr)
          )

c_m <- ggplot(metrics_m_age, aes(x=age_gr, y=conc))+
          geom_point()+
          geom_errorbar(aes(ymin=conc_lci, ymax=conc_uci),width=0.1)+
          labs(title = paste0("Males - C-Stat:", conc_m),
              x="Age", y="Concordance Index")+
          theme_bw()+ theme(axis.text.x = element_text(angle = 45, hjust = 1)) 

## Combine ##
c_p1<- ggpubr::ggarrange(c_f, c_m,  ncol = 2, nrow=1, common.legend = TRUE, legend = "top")+
    ggsave( paste0(path,"/c_stat_by_age_p1.png"), width= 5*16/9, height= 5,dpi=600 )

c_p1+theme(text=element_text(family="Times New Roman", size=10))

#### Period 2 ####
## Females ##
metrics_f <- read.csv( paste0(path, '/metrics_f_p2.csv'))
conc_f = as.character(metrics_f$conc_ci[[1]])
metrics_f_age <- metrics_f %>%
filter(x == "age")%>% 
   mutate(conc = as.numeric(as.character(conc)),
          age_gr= gsub("[()]", "", var) ,
          age_gr=gsub("]]", "",age_gr),
           age_gr=gsub(",", "-",age_gr),
           age_gr=gsub("[[]", " ",age_gr)
          )

c_f <- ggplot(metrics_f_age, aes(x=age_gr, y=conc))+
          geom_point()+
          geom_errorbar(aes(ymin=conc_lci, ymax=conc_uci),width=0.1)+
          labs(title = paste0("Females - C-Stat:", conc_f),
              x="Age", y="Concordance Index")+
          theme_bw()+ theme(axis.text.x = element_text(angle = 45, hjust = 1)) 


## males ##

metrics_m <- read.csv( paste0(path, '/metrics_m_p2.csv'))
conc_m = as.character(metrics_m$conc_ci[[1]])
metrics_m_age <- metrics_m %>%
filter(x == "age")%>% 
   mutate(conc = as.numeric(as.character(conc)),
          age_gr= gsub("[()]", "", var) ,
          age_gr=gsub("]]", "",age_gr),
           age_gr=gsub(",", "-",age_gr),
           age_gr=gsub("[[]", " ",age_gr)
          )

c_m <- ggplot(metrics_m_age, aes(x=age_gr, y=conc))+
          geom_point()+
          geom_errorbar(aes(ymin=conc_lci, ymax=conc_uci),width=0.1)+
          labs(title = paste0("Males - C-Stat:", conc_m),
              x="Age", y="Concordance Index")+
          theme_bw()+ theme(axis.text.x = element_text(angle = 45, hjust = 1)) 

## Combine ##
c_p2 <- ggpubr::ggarrange(c_f, c_m,  ncol = 2, nrow=1, common.legend = TRUE, legend = "top")+
    ggsave( paste0(path,"/c_stat_by_age_p2.png"), width= 5*16/9, height= 5, dpi=600 )

c_p2+theme(text=element_text(family="Times New Roman", size=10))
