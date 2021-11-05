library(DBI)
library(RPostgres)
library(data.table)
library(tidyverse)
library(magrittr)
library(lubridate)
library(clipr)

### read in sample data
coco_claims <- readxl::read_excel("/Users/valeriaduran/Downloads/Noam_Sample_PULL.xlsx")


#### create relabled pos column
coco_claims %<>% mutate(new_pos = clm_pos_cln(pos_cd, pos_nm))

### select columns of interest and drop denied claims
names(coco_claims)
claims_sample <- coco_claims %>% select(person_id,clm_nat_key,clm_line_item_activity_nat_key,prs_nat_key,servicing_healthcare_prov_org_nat_key,billing_healthcare_prov_org_nat_key,clm_id,
                                        serv_from_dt,serv_to_dt,clm_status_nm,pos_cd,pos_nm,diags,diag_1,procs,proc_1,revcode,revcode_1,billed_amt,net_paid_amt,ded_amt,copay_amt,coinsurance_amt, cov_amt,new_pos) %>%
  filter(clm_status_nm != 'DENIED') 

### turn dates into ymd type
claims_sample %<>% 
  mutate(serv_to_dt = ymd(serv_to_dt),
         serv_from_dt = ymd(serv_from_dt))

### order columns
claims_sample  %<>% arrange(person_id,serv_from_dt, serv_to_dt) %>% setDT()

### create column that has difference between service from date and service end date by prs_nat_key
claims_sample[,days_diff := time_length(serv_to_dt-serv_from_dt, unit = 'days'), prs_nat_key]

### grab all unique service from and service to dates that have a date difference > 0 and have a new_pos code of IP
date_period <- claims_sample[new_pos == 'IP', .(unique_start =serv_from_dt[days_diff>0],
                                                unique_end = serv_to_dt[days_diff>0]), prs_nat_key][order(prs_nat_key,unique_start,-unique_end)]
date_period %<>% distinct(prs_nat_key, unique_start, .keep_all = TRUE) %>% drop_na() %>% setDT() ##drop dupes

### The IRanges package found in the Bioconductor package finds overlapping dates 
library(IRanges)


### create a new DT that only has the unique min_start and max_end date ranges by prs_nat_key and remove rows that have a gap
date_new <- date_period[, as.data.table(reduce(IRanges(as.numeric(unique_start), as.numeric(unique_end), min.gapwidth = 0L))), prs_nat_key]### only handles numeric values so convert dates to numeric

date_new[,c(1:3)] <- date_new[, lapply(.SD, as.IDate), by=prs_nat_key, .SDcols = c("start","end")] #convert numerics back to dates
date_new[,min_start:= start][,start := NULL][,max_end := end][,end := NULL][, day_diff := width][,width := NULL] #relabel columns

claim_IP <- claims_sample[order(prs_nat_key,serv_from_dt,-serv_to_dt)][,identifier:=NULL][,max_start_date:=NULL][,max_end_date:=NULL]

##### join the date_new table onto the claims table with conditions
claim_IP[date_new,`:=`(min_start=i.min_start,max_end=i.max_end, dt_range = i.day_diff), on = .(serv_from_dt >= min_start,
                                                                                               serv_from_dt <= max_end,
                                                                                               serv_to_dt >= min_start,
                                                                                               serv_to_dt <= max_end,
                                                                                               prs_nat_key == prs_nat_key)]



### relabel NA min/max to correspond to the svc_from/svc_to dates 
claim_IP <- claim_IP[is.na(min_start), min_start := serv_from_dt][is.na(max_end), max_end := fifelse(!is.na(serv_to_dt), serv_to_dt,serv_from_dt)][order(prs_nat_key,min_start,max_end)]
setDT(claim_IP) 

claim_IP[,c("min_start","max_end")] <- claim_IP[, lapply(.SD, as.Date), .SDcols = c("min_start","max_end")]

## if anything is within min&max date & max+1 day, count as part of visit, but exclude Home and IP - Post Acute
require(data.table)
claim_IP[order(prs_nat_key,min_start,max_end, new_pos),VisitID:=cumsum(!((min_start == data.table::shift(max_end,1, fill = FALSE) |min_start == data.table::shift(min_start,1, fill = FALSE) | min_start == data.table::shift(max_end+1,1, fill = FALSE))  & prs_nat_key == data.table::shift(prs_nat_key,1,fill = FALSE) & (new_pos != 'IP - Post Acute'  & new_pos != 'Home' & data.table::shift(new_pos,1,fill = FALSE) != 'IP - Post Acute' & data.table::shift(new_pos,1,fill = FALSE) != 'Home')
                                                                         & (!(servicing_healthcare_prov_org_nat_key %like% "HOME ") & !(data.table::shift(servicing_healthcare_prov_org_nat_key,1,fill = FALSE) %like% 'HOME '))))]

require(dplyr)
## if an IP code is found within a visit, relabel entire visit to IP (exclude lines where POS code is 21 but service provider has HOME in it)
claim_IP[,update_pos := new_pos[new_pos=='IP' & !(servicing_healthcare_prov_org_nat_key %like% "HOME ")][1], VisitID][,final_pos := fifelse(is.na(update_pos)& !(servicing_healthcare_prov_org_nat_key %like% "HOME "), new_pos, fifelse(!(servicing_healthcare_prov_org_nat_key %like% "HOME "),update_pos,'Home')), VisitID][,update_pos:=NULL]

### get start and end date and length of stays
Visits_IP <- claim_IP[final_pos == 'IP'][order(min_start),admit_dt := dplyr::first(min_start), by =VisitID][order(max_end),dist_dt := dplyr::last(max_end), by =VisitID][,los := dist_dt-admit_dt, by = VisitID][,days_diff := NULL][,dt_range := NULL][,new_pos:=NULL][,min_start:=NULL][,max_end:=NULL]

### Get count of unique IDs
Visits_IP[, .(distinct_cnt = uniqueN(VisitID)), final_pos]

### label room and board
Visits_IP[,`:=`(room_board_ind= +(any(grepl("^1|^2[0-1]",revcode_1))),
                room_board_type= fifelse(grepl("^10",revcode_1), 'All Inclusive',
                                         fifelse(grepl("^11",revcode_1), 'Private',
                                                 fifelse(grepl("^12",revcode_1), 'Semi-Private Two Bed',
                                                         fifelse(grepl("^13",revcode_1), 'Semi-Private - Three and Four Beds',
                                                                 fifelse(grepl("^14",revcode_1), 'Private (Deluxe)',
                                                                         fifelse(grepl("^15",revcode_1), 'Ward',
                                                                                 fifelse(grepl("^16",revcode_1), 'Other',
                                                                                         fifelse(grepl("^17",revcode_1), 'Nursery',
                                                                                                 fifelse(grepl("^18",revcode_1), 'LoA',
                                                                                                         fifelse(grepl("^19",revcode_1), 'Subacute Care',
                                                                                                                 fifelse(grepl("^200",revcode_1), 'ICU',
                                                                                                                         fifelse(grepl("^201",revcode_1), 'Surgical',
                                                                                                                                 fifelse(grepl("^202",revcode_1), 'Medical',
                                                                                                                                         fifelse(grepl("^203",revcode_1), 'Pediatric',
                                                                                                                                                 fifelse(grepl("^204",revcode_1), 'Psychiatric',
                                                                                                                                                         fifelse(grepl("^206",revcode_1), 'Post ICU',
                                                                                                                                                                 fifelse(grepl("^207",revcode_1), 'Burn Care',
                                                                                                                                                                         fifelse(grepl("^208",revcode_1), 'Trauma',
                                                                                                                                                                                 fifelse(grepl("^209",revcode_1), 'Other intensive care',
                                                                                                                                                    fifelse(grepl("^210",revcode_1), 'CCU, General',
                                                                                                                                                            fifelse(grepl("^211",revcode_1), 'CCU, Myocardial Infarction',
                                                                                                                                                                    fifelse(grepl("^212",revcode_1), 'CCU, Pulmonary Care',
                                                                                                                                                                            fifelse(grepl("^214",revcode_1), 'Post CCU',
                                                                                                                                                                                    fifelse(grepl("^213",revcode_1), 'CCU, Heart Transplant',
                                                                                                                                                                                    fifelse(grepl("^219",revcode_1), 'CCU, Other',NA_character_)))))))))))))))))))))))))), VisitID]

### calculate room&board cost
Visits_IP %<>% mutate(cost_room_board = ifelse(!is.na(room_board_type),rowSums(across(c(net_paid_amt,copay_amt,coinsurance_amt,ded_amt))), 0)) %>% setDT()

### get the visit summary with total_allowed and total_net_paid and total_room&board
IP_visit_summary <- Visits_IP[,lapply(.SD,sum),.SDcols=c('net_paid_amt','copay_amt','coinsurance_amt','ded_amt'), .(person_id,final_pos,VisitID,admit_dt,dist_dt,los,cost_room_board,room_board_ind)][,.(total_allowed_amt = sum(.SD), total_net_paid = sum(net_paid_amt), total_room_board = sum(cost_room_board)),.SDcols=c('net_paid_amt','copay_amt','coinsurance_amt','ded_amt'),.(person_id,final_pos,VisitID,admit_dt,dist_dt,los,room_board_ind)][order(person_id,admit_dt,dist_dt)]

write_clip(IP_visit_summary)
write_clip(Visits_IP)



