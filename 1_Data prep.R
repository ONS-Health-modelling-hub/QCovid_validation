library(dplyr)
#library(dbplyr)
library(sparklyr)
library(tidyverse)

eos_date = "2020-07-28"
dir ="cen_dth_hes/QCOVID validation"
keyworker_lookup_file = "KEYWORKER_LOOKUP_TABLE.csv"


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



###############
#Get data frames
###############

tbl_change_db(sc, "cen_dth_hes")


## create connection to main analytical dataset

df <- sdf_sql(sc, "SELECT * FROM  analytical_cen_deaths_hes_dt_26102020_4")



## add more variables from raw census data ##
census <- sdf_sql(sc, "SELECT * FROM census_with_geogs_ce ")
grep("scg", tbl_vars(census), value =TRUE)

add_var_ind <- census %>%
           select(il3_person_id, scgpuk11)

# add more household level variables
census <- sdf_sql(sc, "SELECT * FROM census_2011_cdme_tactical_t2.furd_il3_f_household_std")
grep("acc", tbl_vars(census), value =TRUE)

add_var <- census %>%
           select(household_id, rooms, bedrooms, cenheat, roomreq11, typaccom)

## get postcodes
census_ind <- sdf_sql(sc, "SELECT * FROM census_2011_cdme_tactical_t2.furd_il4_f_person_std ")

pcd <- census_ind %>%
        select(il4_person_id,
               home_pcd=enumpc, wkplpc)

#############
# DATA PREP #
#############

### apply study population inclusion criteria

df_eligible <- df %>%
  filter(as.numeric(uresindpuk11)==1,
         death_date >= "2020-01-24"| is.na(death_date),
         age_2020<=100 & age_2020 >= 19,
       pr_census_region!="W" & !is.na(pr_census_region))%>%
   left_join(add_var_ind, by="il3_person_id")%>%
 left_join(pcd, by="il4_person_id")%>%
   left_join(add_var, by="household_id")

#sample_counts <- df_eligible %>%
#summarise(n_all = n())%>%
#collect()



df_ext <- df_eligible %>%
          mutate(care_home = ifelse(!is.na(care_home_pr19),1,0))%>%
 select(census_person_id,
         household_id,
         fmspuk11,
         residence_type,
         care_home_pr19,
        care_home,
         Multigen,  
         ethnicity7 = ethnicity,    ### TEMPORARY
         ethpuk11,
         hlqpuk11, 
         relpuk11,
         hhchuk11,
         age = age_2020,
         age_range_2020_broad,
         sex,
         health, 
         disability,
         tenhuk11,
         deprived,
         People_per_Sq_Km,
         COVID_death,
         region_code,
         la_code,
         IMD_decile = WIMD_IMD_decile,
         IMD_quintile = WIMD_IMD_quintile,
         #nsshuk11_final,
         uresindpuk11,
         occ,
         indpuk,
         death_date,
         proximity_to_others,
         exposure_to_disease,
         keyworker_type,
         age_range_2020,
         bedrooms,rooms, cenheat,roomreq11,
         pr_census_region,
         ruralurban_name,
         scgpuk11,typaccom, oa_code
        )%>%
        mutate(
          COVID_death = ifelse(COVID_death!="Alive" & death_date>eos_date, "Alive", COVID_death),
          death_covid = ifelse(COVID_death == 'COVID', 1, 0),
          death_other = ifelse(COVID_death == 'Not COVID', 1, 0),
          t = ifelse(COVID_death!="Alive" ,
                    datediff(death_date, "2020-01-24"),
                    datediff(eos_date, "2020-01-24")),
          ethpuk11=as.numeric(ethpuk11),
          ethnicity = case_when(
           ethpuk11 <=2 ~ ethpuk11,
           ethpuk11 ==  3| ethpuk11 == 4 ~3,
           ethpuk11 > 4 & ethpuk11 <= 11 ~ ethpuk11 -1,
           ethpuk11 == 12 ~15,
           ethpuk11 == 13 ~ 11,
           ethpuk11 == 14 ~13,
           ethpuk11 == 15 ~12,
           ethpuk11 == 16 ~14,
           ethpuk11 %in% c(17, 18)~16 
          ),
          ethnicity_9 = case_when(
                   ethpuk11 == 1 ~ "White British", 
                   ethpuk11 %in% 2:4 ~ "White other",
                   ethpuk11 %in% 5:8 ~ "Mixed",
                   ethpuk11 == 9 ~ "Indian",
                   ethpuk11 == 10 ~ "Pakistani",
                   ethpuk11 == 11 ~ "Bangladeshi",
                   ethpuk11 == 12 ~ "Chinese",
                   ethpuk11 == 14 ~ "Black African",
                   ethpuk11 == 15 ~ "Black Caribbean",
                   ethpuk11 > 15 | ethpuk11 == 13 ~  "Other"
        ), 
          sex= if_else(as.numeric(sex) == 1, 1, 0),
          homecat = if_else(care_home == 1, 1, 0)

        )

#check <- df_ext %>%
#  group_by(ethpuk11, ethnicity)%>%
#  summarise(n=n(), max_t = max(t))%>%
#  collect() %>%
#arrange(ethnicity)


#### townsend index ####
townsend <-spark_read_csv(sc,name = "town", path = "/dap/landing_zone/ons/townsend_dataset/v1/Scores- 2011 UK Ouput Areas.csv", header = TRUE, delimiter = ",")
townsend <- townsend  %>%
            select(oa_code=geo_code, town=TDS, town_q=quintile )


#### hes data ####
# lookup nhs number census ID
lookup <-  sdf_sql(sc, "SELECT * FROM  cen_dth_hes.cenmortlink_gpes_20201109")%>%
             select(newnhsno_hes= hes_nhsno,census_person_id )
## read in data
#temporary
hes1 <- sdf_sql(sc, "SELECT * FROM cen_dth_hes.cen_hes_qcovid_20201210_v1")%>%
      select(-hes_nhsno, -deaths_nhsno,-newnhsno_hes)
hes2 <- sdf_sql(sc, "SELECT * FROM cen_dth_hes.cen_hes_qcovid_20201210_v2")%>%
      select(-hes_nhsno, -deaths_nhsno,-newnhsno_hes)
hes3 <- sdf_sql(sc, "SELECT * FROM cen_dth_hes.cen_hes_qcovid_20201210_v3")%>%
      select(-hes_nhsno, -deaths_nhsno,-newnhsno_hes)

hes <- hes1 %>%
  left_join(hes2, by="census_person_id" )%>%
  left_join(hes3, by="census_person_id" )

hes <-sdf_sql(sc, "SELECT * FROM cen_dth_hes.cen_hes_qcovid_cerebral_palsy_test_all")
#
### read in data
#hes <- sdf_sql(sc, "SELECT * FROM cen_dth_hes.cen_hes_qcovid_20201210")
#tbl_vars(hes)
#hes <- hes %>%
#left_join(hes1, by="newnhsno_hes")%>%
#      left_join(lookup, by="newnhsno_hes")


## rename columns
hes <- hes %>%
   select(census_person_id,
          p_solidtransplant = SolidOrganTransplant_FLAG,
          b_congenheart = CongenitalHeartProblem_FLAG,
          b_cerebralpalsy= CerebralPalsy_FLAG,
          b_respcancer = RespiratoryCancer_FLAG,
          b_vte =ThrombosisPulmonaryEmbolus_FLAG,
          b_congenheart =CongenitalHeartProblem_FLAG,
          b_pulmhyper_hes =PulmonaryHypertensionOrFibrosis_FLAG,
          b_pulmrare_hes =CysticFibrosisBronchiectasisAlveolitis_FLAG,
          b_stroke_hes=Stroke_FLAG,
          b_fracture4_hes = FracturesHipWristSpineHumerus_FLAG ,
          p_marrow6 = BoneMarrowTransplantInLast6Months_FLAG
         ) %>%
    mutate(p_solidtransplant =as.numeric(p_solidtransplant),
           b_congenheart = as.numeric( b_congenheart ),
           b_cerebralpalsy = as.numeric(  b_cerebralpalsy),
           b_respcancer=as.numeric(b_respcancer), 
           b_vte=as.numeric( b_vte),
           b_pulmrare_hes =as.numeric( b_pulmrare_hes),
           b_pulmhyper_hes =as.numeric( b_pulmhyper_hes),
           b_stroke_hes = as.numeric(b_stroke_hes) ,
           b_fracture4_hes = as.numeric(b_fracture4_hes),
           p_marrow6 = as.numeric(p_marrow6 ))   
tbl_vars(hes)



desc2 <- hes %>%
select(-census_person_id)%>%
    summarise_all(sum)%>%
              collect()

# deal with duplicates
hes <- hes %>%
  group_by(census_person_id)%>%
  summarise_all(~(max(.)))


#desc1 <- hes%>%
#    summarise(p_solidtransplant = sum(p_solidtransplant, na.rm=TRUE),
#              p_marrow6 = sum(p_marrow6, na.rm=TRUE),
#              b_congenheart_hes = sum(b_congenheart_hes, na.rm=TRUE), n=n())%>%
#              collect()


#hes_bm <- sdf_sql(sc, "SELECT * FROM cen_dth_hes.cen_hes_qcovid_20201123_bone_marrow_jm")%>%
#   select(census_person_id,
#          #p_solidtransplant = SolidOrganTransplant_FLAG,
#           p_marrow6_hes = BoneMarrowTransplantInLast6Months_FLAG)%>%
#          #b_congenheart_hes = CongenitalHeartProblem_FLAG) %>%
#    mutate(p_marrow6_hes = as.numeric(p_marrow6_hes ))
#tbl_vars(hes_bm)
#


#### GPES data #####

gpes <- sdf_sql(sc, "SELECT * FROM cen_dth_hes.qcovidoutputs_gpesv2")
tbl_vars(gpes)



# lookup nhs number census ID
lookup <-  sdf_sql(sc, "SELECT * FROM  cen_dth_hes.cenmortlink_gpes_20201109")%>%
             select(nhs_number = gpes_nhsno,census_person_id )
#
#desc <- gpes %>%
#      summarise_all(list(~sum(.))) %>%
#      collect()

gpes <- gpes %>%
        left_join(lookup, by = "nhs_number")%>%
        select(census_person_id,
               b_asthma = Asthma ,
               b_AF=AtrialFibrillation ,
               b_chd=CoronaryHeartDisease,
               b_bloodcancer ,
              # b_congenheart = CongenitalHeartProblem  ,
               b_copd = Copd  ,
               b_pulmrare = CysticFibrosisBronchiectasisAlveolitis  ,
               b_dementia = Dementia   ,
               diabetes_cat = Diabetes  ,
               b_epilepsy = Epilepsy ,
               b_CCF = HeartFailure  ,
               learncat = learn_cat , 
               b_cirrhosis = LiverCirrhosis ,
              # b_respcancer=LungOrOralCancer ,
               b_neurorare = MotorNeuroneDiseaseOrMultipleSclerosisOrMyaestheniaOrHuntingtonsChorea ,
               b_parkinsons = ParkinsonsDisease ,
               b_pvd = PeripheralVascularDisease ,
               b_pulmhyper = PulmonaryHypertensionOrFibrosis ,
               b_ra_sle = RheumatoidArthritisOrSle ,
               b_semi = SevereMentalIllness ,
               b_sicklecelldisease =	SickleCellOrSevereCombinedImmunodeficiencySyndromeViaBool,
               b_stroke = StrokeOrTia ,
              # b_vte = ThrombosisOrPulmonaryEmbolus,
               b_fracture4=PriorFractureOfHipWristSpineHumerus,
               p_marrow6_gpes = p_marrow6, 
               p_solidtransplant_gpes = p_solidtransplant,
               b2_82 = b2_82v1,
               b2_leukolaba=b2_leukolabav1,
               b2_prednisolone=b2_prednisolonev1,
               renalcat=ckd_cat,
               GP_SYSTEM_SUPPLIER
              )%>%
            mutate(b_asthma=as.numeric(b_asthma),
b_AF=as.numeric(b_AF),
b_chd=as.numeric(b_chd),
b_bloodcancer=as.numeric(b_bloodcancer),
b_copd=as.numeric(b_copd),
b_pulmrare=as.numeric(b_pulmrare),
b_dementia=as.numeric(b_dementia),
diabetes_cat=as.numeric(diabetes_cat),
b_epilepsy=as.numeric(b_epilepsy),
b_CCF=as.numeric(b_CCF),
learncat=as.numeric(learncat),
b_cirrhosis=as.numeric(b_cirrhosis),
b_neurorare=as.numeric(b_neurorare),
b_parkinsons=as.numeric(b_parkinsons),
b_pvd=as.numeric(b_pvd),
b_pulmhyper=as.numeric(b_pulmhyper),
b_ra_sle=as.numeric(b_ra_sle),
b_semi=as.numeric(b_semi),
b_sicklecelldisease=as.numeric(b_sicklecelldisease),
b_stroke=as.numeric(b_stroke),
b_fracture4=as.numeric(b_fracture4),
p_marrow6_gpes=as.numeric(p_marrow6_gpes),
p_solidtransplant_gpes=as.numeric(p_solidtransplant_gpes),
b2_82=as.numeric(b2_82),
b2_leukolaba=as.numeric(b2_leukolaba),
b2_prednisolone=as.numeric(b2_prednisolone),
tpp = ifelse(GP_SYSTEM_SUPPLIER=="TPP",1,0))%>%
select(-GP_SYSTEM_SUPPLIER)


# deal with duplicates
gpes <- gpes %>%
  group_by(census_person_id)%>%
  summarise_all(~(max(.)))

### bmi data ###
bmi <- sdf_sql(sc, "SELECT * FROM cen_dth_hes.gpes_gdppr_bmi_recent") %>%
        select(nhs_number = NHS_NUMBER, bmi=BMI, bmi_cat = BMI_CATEGORY)%>%
        left_join(lookup, by = "nhs_number")

gpes <- gpes %>%
left_join(bmi , by="census_person_id")



#### sact data  ####

sact <- sdf_sql(sc, "SELECT * FROM q_cov_valid.composite_rtds_sact_cenid_tat")
tbl_vars(sact)
sact <- sact %>%
      select(census_person_id, p_radio6, sact12)%>%
      group_by(census_person_id) %>%
      mutate(chemocat = case_when(sact12 == "A"~1,
                                  sact12 == "B"~2,
                                  sact12 == "C"~3))%>%
      summarise(p_radio6 = max(p_radio6, na.rm=TRUE), 
                chemocat=max(chemocat,na.rm=TRUE))


## add flags for non-linkage
linkage_info <-  sdf_sql(sc, "SELECT * FROM  cen_dth_hes.cenmortlink_gpes_20201109")%>%
           select(census_person_id, gpes_nhsno) %>%
           mutate(linked_gpes = as.numeric(!is.na(gpes_nhsno)))%>%
           select(-gpes_nhsno)
                 #unlinked_pr =ifelse(pr_link == 1, 1, 0))

linkage_pr <-  sdf_sql(sc, "SELECT * FROM  cen_dth_hes.cenmortlink_20201116") %>%
                 select(census_person_id,pr_link)


#### ------------------####
#### Link all the data ####
#### ------------------####

## link to townsend ##
linked_data <- df_ext %>%
            select(sex, ethnicity, ethnicity_9, age, homecat,census_person_id, death_covid,t,death_date, oa_code,region = pr_census_region )%>%
              left_join(townsend, by="oa_code")


#sample_counts <- linked_data  %>%
#summarise(n_all = n())%>%
#collect()
#sample_counts
## link to hes  ##
linked_data <- linked_data %>%
              left_join(hes, by="census_person_id") %>%
              mutate(p_solidtransplant =  ifelse(is.na(p_solidtransplant),0,p_solidtransplant),
                     p_marrow6 = ifelse(is.na( p_marrow6),0, p_marrow6),
                     b_congenheart = ifelse(is.na(b_congenheart),0,b_congenheart),
                     b_cerebralpalsy = ifelse(is.na(  b_cerebralpalsy),0,  b_cerebralpalsy))
#n_hes <- linked_data  %>%
#count()%>%
#collect()

## link to sact ##
linked_data <- linked_data %>%
              left_join(sact, by="census_person_id") %>%
              mutate(p_radio6 = if_else(is.na(p_radio6), 0, p_radio6),
                     chemocat = if_else(is.na(chemocat), 0, chemocat))
#n_sact <- linked_data  %>%
#count()%>%
#collect()
## link to GPES
linked_data <- linked_data %>%
              left_join(gpes, by="census_person_id") %>%
            mutate(not_in_gpes = if_else(!is.na( b_asthma ), 1, 0 )) %>%
              mutate(
                     b_chd = if_else(is.na( b_chd ), 0, b_chd ),
                    b_copd = if_else(is.na( b_copd ), 0, b_copd ),
                    diabetes_cat = if_else(is.na( diabetes_cat ), 0, diabetes_cat ),
                    learncat = if_else(is.na( learncat ), 0, learncat ),
                    b_parkinsons = if_else(is.na( b_parkinsons ), 0, b_parkinsons ),
                    b_pulmhyper = if_else(is.na( b_pulmhyper ), 0, b_pulmhyper ),
                    b_pulmhyper_hes = if_else(is.na( b_pulmhyper_hes ), 0, b_pulmhyper_hes),
                    b_sicklecelldisease = if_else(is.na( b_sicklecelldisease ), 0, b_sicklecelldisease ),
                    p_solidtransplant_gpes = if_else(is.na( p_solidtransplant_gpes ), 0, p_solidtransplant_gpes ),
                    b2_prednisolone = if_else(is.na( b2_prednisolone ), 0, b2_prednisolone ),
                    b_asthma = if_else(is.na( b_asthma ), 0, b_asthma ),
                    b_bloodcancer = if_else(is.na( b_bloodcancer ), 0, b_bloodcancer ),
                    b_pulmrare = if_else(is.na( b_pulmrare ), 0, b_pulmrare ),
                     b_pulmrare_hes = if_else(is.na( b_pulmrare_hes ), 0, b_pulmrare_hes ),
                    b_epilepsy = if_else(is.na( b_epilepsy ), 0, b_epilepsy ),
                    b_respcancer = if_else(is.na( b_respcancer ), 0, b_respcancer ),
                    b_pvd = if_else(is.na( b_pvd ), 0, b_pvd ),
                    b_ra_sle = if_else(is.na( b_ra_sle ), 0, b_ra_sle ),
                    b_vte = if_else(is.na( b_vte ), 0, b_vte ),
                    b2_82 = if_else(is.na( b2_82 ), 0, b2_82 ),
                    renalcat = if_else(is.na( renalcat ), 0, renalcat ),
                    b_AF = if_else(is.na( b_AF ), 0, b_AF ),
                    b_congenheart = if_else(is.na( b_congenheart ), 0, b_congenheart ),
                    b_dementia = if_else(is.na( b_dementia ), 0, b_dementia ),
                    b_CCF = if_else(is.na( b_CCF ), 0, b_CCF ),
                    b_neurorare = if_else(is.na( b_neurorare ), 0, b_neurorare ),
                    b_fracture4 = if_else(is.na( b_fracture4 ), 0, b_fracture4 ),
                     b_fracture4_hes = if_else(is.na( b_fracture4_hes ), 0, b_fracture4_hes ),
                    b_semi = if_else(is.na( b_semi ), 0, b_semi ),
                    p_marrow6_gpes = if_else(is.na( p_marrow6_gpes ), 0, p_marrow6_gpes ),
                    b2_leukolaba = if_else(is.na( b2_leukolaba ), 0, b2_leukolaba ),
                    b_stroke = ifelse(is.na( b_stroke ), 0, b_stroke ),
                     b_stroke_hes = ifelse(is.na( b_stroke_hes ), 0, b_stroke_hes ),
                     b_cirrhosis = if_else(is.na(  b_cirrhosis ), 0, b_cirrhosis) )%>%
 #combine vars from hes and gpes
             mutate(b_stroke = ifelse(b_stroke_hes ==1, 1,as.numeric(b_stroke)),
                   b_pulmrare = ifelse(b_pulmrare_hes ==1, 1,as.numeric(b_pulmrare)),
                   b_pulmhyper = ifelse(b_pulmhyper_hes ==1, 1,as.numeric(b_pulmhyper)),
                    b_fracture4 = ifelse(b_fracture4_hes ==1, 1,as.numeric(b_fracture4)))%>%
            mutate( b_stroke = if_else(is.na( b_stroke ), 0, b_stroke ),
                   b_pulmhyper = if_else(is.na( b_pulmhyper ), 0, b_pulmhyper ),
                    b_pulmrare = if_else(is.na( b_pulmrare ), 0, b_pulmrare ),
                    b_fracture4 = if_else(is.na( b_fracture4 ), 0, b_fracture4 )
                  )%>%
# can't distinguish between T1 and T2 diabetes, so let's assume they all have T2
            mutate(diabetes_cat =  ifelse(diabetes_cat == 1,2,diabetes_cat))

#n_gpes <- linked_data  %>%
#count()%>%
#collect()

# linkage info
linked_data <- linked_data %>%
          left_join(linkage_info, by="census_person_id")
        
n_final <- linked_data  %>%
count()%>%
collect()

desc <- linked_data %>%
select(b_stroke, b_stroke_hes, not_in_gpes, linked_gpes, pr_link)%>%
      summarise_all(list(~sum(.))) %>%
      collect()


desc




## removing unnecessary variables

linked_data <- linked_data %>%
            select(-oa_code,-not_in_gpes, -p_marrow6_gpes, -p_solidtransplant_gpes,
                   -b_stroke_hes, -b_pulmrare_hes, -b_pulmhyper_hes, -b_fracture4_hes
                 )



# Delete table
sql <- paste0('DROP TABLE IF EXISTS q_cov_valid.linked_census_qcovid')
DBI::dbExecute(sc, sql)

# save
sdf_register(linked_data, 'linked_data')
sql <- paste0('CREATE TABLE q_cov_valid.linked_census_qcovid AS SELECT * FROM linked_data')
DBI::dbExecute(sc, sql)

