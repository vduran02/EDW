# EDW
### Setting up R environment
Must select claim lines of interest.

Sample code:
```edw <- dbConnect(drv = RPostgres::Postgres(), 
                 dbname = 'acp_edw', 
                 host='redshift-prod.accint.io', 
                 port=5439, 
                 user='', 
                 password='')  



claim_line <- setDT(dbGetQuery(con, "select *
from (
SELECT * FROM acp_edw.edw.clm_line_item_activity
WHERE serv_from_dt >= '1-1-2020' AND serv_from_dt <= '1-31-2020'
AND activity_paid_dt >= '1-1-2020' AND activity_paid_dt <= '3-31-2021'
AND prs_nat_key in (SELECT prs_nat_key FROM (
SELECT prs_nat_key FROM
(SELECT beneficiary_id FROM edw.mbrshp_covrg WHERE acp_mbr_flg_val = 1 AND ((covrg_eff_dt <= '1-1-2020' AND covrg_end_dt >= '1-31-2020') OR (covrg_eff_dt <= '1-1-2020' AND covrg_end_dt IS NULL))) elig
LEFT JOIN (SELECT * FROM rpt.work_client_view WHERE group_nm = 'UAL In-Scope') mbr on elig.beneficiary_id = mbr.person_id
LEFT JOIN (SELECT sdt_id, mstr_prs_key FROM acp_edw.edw.xwlk_sdt_edt_key) xw on mbr.person_id = xw.sdt_id
LEFT JOIN (SELECT mstr_prs_key as key_2, prs_nat_key FROM acp_edw.edw.prs_xref WHERE data_source_system_cd LIKE '%UAL%') prs_key on xw.mstr_prs_key = prs_key.key_2
))
order by prs_nat_key,serv_from_dt);"))
```

R packages that are needed:
```
library(DBI)
library(RPostgres)
library(data.table)
library(tidyverse)
library(magrittr)
library(lubridate)
library(clipr)
```
### IP Visit Logic
Find claim lines that have a svc_to_dt - svc_from_dt > 0. Create a new table containing these dates along with prs_nat_key. Following this, drop rows containing same svc_frm_dt based on prs_nat_key. Afterwards, create two new columns min_start and max_end, based on the earliest svc_frm_dt and latest svc_to_dt that are in a continuous date range. Once this has been created, join this table onto the overall claims table. Any claim line that has a matching prs_nat_key and has svc_fm and svc_to dates landing within the min_start/max_end will have the join. Otherwise, min/max will equal svc_frm/svc_to. From then on, count any claims that are adjacent by date as a visit, excluding Home and SNFs. Create indicators for room&board, as well as type of room&board. Summarize Visits by total_allowed_amt, net_paid, room_board_cost, admit date, discharge date, LOS, and room&board ind.


### final output should give 118 IP visits, compared to 100 samples Coco pulled.
