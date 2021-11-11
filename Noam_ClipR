library(clipr)
library(stringr)
library(readr)


person_id_sample <- read_tsv(clipboard())

prs_nat_key <- as.vector(person_id_sample$person_id)
write_clip(str_c("(", str_c("'", sdt_id, "'", sep = "", collapse = ", " ), ")" ), breaks = ',\n')
write_clip(str_c("'",sdt_id,"'"), breaks = ',\n')
