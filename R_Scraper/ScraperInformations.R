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
  c <- 0
  list <- clean_list_of_strings(list)
  for (item in list){
    if(c == 0){list_insert <- paste0(item); c<-c+1}else{
      list_insert <- paste0(list_insert, '|', item)
      c<-c+1
    }
  }
  return(list_insert)
}

clean_list_of_strings <- function(list){
  
  if (length(list) > 1){
    c <- 0
    for(item in list){
      if(c == 0){
        list_insert <- gsub("'", "", item)
      }else{
        list_insert <- c(list_insert, gsub("'", "", item))
      }
    }
  }else{
    list_insert <- gsub("'", "", list)
  }
  return(list_insert)
}

create_table_if_necessary_query = 'CREATE TABLE IF NOT EXISTS informations (
link_id TEXT PRIMARY KEY, 
title TEXT, author TEXT, 
affiliations TEXT, 
keywords TEXT, 
received_date TEXT, 
accepted_date TEXT, 
published_date TEXT, 
abstract TEXT)'

dbSendQuery(con, create_table_if_necessary_query)
iteration <- 1
for (link in links_list[[1]]){
  link <- as.character(link)
  art <- read_html(link)
  
  art %>%
    html_nodes('.contribDegrees .header') %>%
    html_text() -> authors
  
  art %>%
    html_nodes('.abstractInFull p') %>%
    html_text() -> abstract
  
  art %>%
    html_nodes('.dates') %>%
    html_text() -> dates
  
  art %>%
    html_nodes('.abstractKeywords , .abstractKeywords .title') %>%
    html_text() -> keywords
  
  art %>%
    html_nodes('.chaptertitle') %>%
    html_text() -> title
  
  art %>%
    html_nodes('.contribAff') %>%
    html_text() -> affiliations
  
  tryCatch({if (is.na(authors) == FALSE){author_insert <- create_str_from_list(authors)}else{
    author_insert <- 'notfound'
  }}, error=function(e){
    print(paste0('Error on authors: ', e))
    author_insert <- 'notfound'
  })
  
  
  tryCatch({if (is.na(affiliations) == FALSE){affiliations_insert <- create_str_from_list(affiliations)}else{
    affiliations_insert <- 'notfound'
  }},error=function(e){
    print(paste0('Error on affiliation: ', e))
    affiliations_insert <- 'notfound'
  })
  tryCatch({if (is.na(keywords) == FALSE){keywords_insert <- create_str_from_list(keywords[1])}else{
    keywords_insert <- 'notfound'
  }},error=function(e){
    print(paste0('Error on keywords: ', e))
    keywords_insert <- 'notfound'
  })
  tryCatch({if (is.na(abstract) == FALSE){abstract_insert <- clean_list_of_strings(abstract)}else{
    abstract_insert <- 'notfound'
  }},error=function(e){
    print(paste0('Error on abstract: ', e))
    abstract_insert <- 'notfound'
  })
  
  if (is.na(title) == FALSE){
    title_insert <- gsub("[\r\n]", "", title)
    title_insert <- clean_list_of_strings(title_insert)
  }else{
    title_insert <- 'notfound'
  }
  tryCatch({if (is.na(dates) == FALSE){
    date_received <- gsub("[\r\n]", "", dates[1])
    date_received <- gsub("Received:", "", date_received)
    date_accepted <- gsub("[\r\n]", "", dates[2])
    date_accepted <- gsub("Accepted:", "", date_accepted)
    date_published <- gsub("[\r\n]", "", dates[3])
    date_published <- gsub("Published Online:", "", date_published)
  }else{
    print(paste0('Error on dates: ', e))
    date_received <- 'notfound'
    date_accepted <- 'notfound'
    date_published <- 'notfound'
  }},error=function(e){
    
    date_received <- 'notfound'
    date_accepted <- 'notfound'
    date_published <- 'notfound'
  })
  
  links_id_insert <- gsub("http://pubsonline.informs.org/doi/abs/10.1287/", "", link)
  
  insert_query <- paste0("INSERT INTO informations VALUES ('",links_id_insert, "', '", title_insert, "', '", author_insert, "', '",
                         affiliations_insert, "', '", keywords_insert, "', '", date_received, "', '", date_accepted, "', '", 
                         date_published, "', '", abstract_insert, "')")
  
  
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
  print(paste0(title_insert, ' Finished!'))
  print(paste('Iteration: ', iteration, ' | out of: ', total_to_scrape))
  iteration <- iteration + 1
  Sys.sleep(runif(1, min_max_sleep_time[1], min_max_sleep_time[2]))
}


