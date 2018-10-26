library(rvest)
library(RSQLite)
library(DBI)
path_to_files = '/Users/pedroveronezi/BIA656_PaperProbability/'
path_to_links_file <- 'links_ids.txt'
con = dbConnect(RSQLite::SQLite(), dbname=paste0(path_to_files,"PaperProbability.db"))
min_max_sleep_time <- c(60, 120)
links_list <- read.table(paste0(path_to_files,path_to_links_file))
total_to_scrape <- length(links_list[[1]])

create_str_from_list <- function(list){
  d <- 0
  list <- clean_list_of_strings(list)
  if (length(list) != 0){
    for (item in list){
      if(d == 0){list_insert <- paste0(item); d<-d+1}else{
        list_insert <- paste0(list_insert, '|', item)
        d<-d+1
      }
    }
  }else{
    list_insert <- 'not found'
  }
  return(list_insert)
}

clean_list_of_strings <- function(list){
  if (length(list) > 1){
    c <- 0
    for(item in list){
      if(c == 0){
        list_insert <- gsub("'", "", item)
        c <- c + 1
      }else{
        list_insert <- c(list_insert, gsub("'", "", item))
        c <- c + 1
      }
    }
  }else{
    list_insert <- gsub("'", "", list)
  }
  return(list_insert)
}


base_string <- "http://pubsonline.informs.org/doi/ref/10.1287/"

create_table_if_necessary_query = "CREATE TABLE IF NOT EXISTS refs (
link_id TEXT PRIMARY KEY,
title TEXT,
author TEXT,
journals TEXT)"

dbSendQuery(con, create_table_if_necessary_query)
iteration <- 1
for (link in links_list[[1]]){
  
  # Adjusts the link to the references page
  link <- as.character(link)
  link_adj <- gsub("http://pubsonline.informs.org/doi/abs/10.1287/", base_string, link)
  
  art <- read_html(link_adj)

  art %>%
    html_nodes('.references .NLM_article-title') %>%
    html_text() -> title
  
  art %>%
    html_nodes('.references .i') %>%
    html_text() -> journals
  
  art %>%
    html_nodes('.NLM_year') %>%
    html_text() -> year
  
  art %>%
    html_nodes('.references td') %>%
    html_text() -> complete_refs
  
  complete_refs = complete_refs[complete_refs != "  "]
  complete_refs = complete_refs[complete_refs != " "]
  complete_refs = complete_refs[complete_refs != ""]
  authors <- c()
  for(i in 1:length(complete_refs)){
    idx <- regexpr(paste0('(',year[i],')'), complete_refs[i])
    authors <- c(authors, substr(complete_refs[i], 1, (idx-2)))
  }
  author_insert <- create_str_from_list(authors)
  
  links_id_insert <- gsub(base_string, "", link_adj)
  
  title_insert <- create_str_from_list(title)
  journals_insert <- "not found"
  tryCatch({journals_insert <- create_str_from_list(journals)},error=function(e){
             print(e)
             print('Error on Journals Getting')
           })
  
  insert_query <- paste0("INSERT INTO refs VALUES ('",links_id_insert, "', '", title_insert, "', '", author_insert, "', '",
                         journals_insert, "')")
  
  tryCatch({
    dbListTables(con)
  }, error=function(e){
    con = dbConnect(RSQLite::SQLite(), dbname=paste0(path_to_files,"PaperProbability.db"))
  })
  
  tryCatch({
    dbSendQuery(con, insert_query)
  }, error=function(e){
    print(e)
    print(paste0('Error on iteration: ', iteration, ' | for link: ', links_id_insert, ' !!!'))
  })
  print(paste0(links_id_insert, ' Finished!'))
  print(paste('Iteration: ', iteration, ' | out of: ', total_to_scrape))
  iteration <- iteration + 1
  Sys.sleep(runif(1, min_max_sleep_time[1], min_max_sleep_time[2]))
}


