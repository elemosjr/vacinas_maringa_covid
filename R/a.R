#!/bin/Rscript

suppressWarnings(suppressMessages(library(sparklyr)))
suppressWarnings(suppressMessages(library(tidyverse)))
suppressWarnings(suppressMessages(library(lubridate)))
suppressWarnings(suppressMessages(library(rvest)))
suppressWarnings(suppressMessages(library(glue)))

Sys.setlocale("LC_TIME", "pt_BR.UTF-8")

args <- commandArgs(trailingOnly = TRUE)

if(length(args) == 0) args <- c("F", "F")
if(length(args) == 1) args <- c(args, "F")

# web scrapping
#####
if(as.logical(args[1]))
{
  base_url <- "https://opendatasus.saude.gov.br"
  
  html <- read_html(glue("{base_url}/dataset/covid-19-vacinacao"))
  
  link <- html %>%
    html_node(xpath = "/html/body/div[2]/div/div[3]/div/article/div/section[1]/ul/li[3]/a") %>%
    html_attr("href")
  
  html <- read_html(glue("{base_url}{link}"))
  
  link <- html %>%
    html_node(xpath = "/html/body/div[2]/div/div[3]/section/div/div[2]/ul/li[2]/p/a") %>%
    html_attr("href")
  
  system(glue("wget --output-document ../data/raw_data.csv {link} && mv raw_data.csv data.csv"))
  #download.file(link, "../data/raw_data.csv")
}
#####

# Spark
#####
if(as.logical(args[2]))
{
  #spark_dir <- glue("{Sys.getenv('HOME')}/git/vacinas_br/spark")
  
  config <- spark_config()
  
  config$`sparklyr.cores.local` <- "10"
  config$`spark.driver.memory` <- "4G"
  config$`spark.executor.memory` <- "4G"
  config$`spark.executor.cores` <- "2"
  config$`sparklyr.shell.executor-memory` <- "4G"
  config$`spark.yarn.driver.memoryOverhead` <- "1024"
  
  sc <- spark_connect(master = "local", config = config)
  
  #dados <- spark_read_csv(sc, "dados", "a.csv", delimiter = ";", memory = FALSE)
  dados <- spark_read_csv(sc, "dados", "../data/data.csv", delimiter = ";", memory = FALSE)
  
  maringa <- dados %>%
    filter(estabelecimento_municipio_nome == "MARINGA") %>%
    count(vacina_dataaplicacao) %>%
    collect()
  
  spark_disconnect(sc)
  
  write_csv(maringa, "../data/maringa.csv")
}
#####

#
#####
#maringa <- read_csv("../data/maringa.csv") %>%
#  mutate(data = ymd(vacina_dataaplicacao))
#
#top3 <- top_n(maringa, 3, n)
#
#datas_x <- sort(c(seq(min(maringa$data), max(maringa$data), length.out = 7), top3$data))
#
#grafico <- maringa %>%
#  ggplot(aes(x = data, y = n)) +
#  geom_col() +
#  geom_label(data = top3, aes(x = data, y = n, label = n), nudge_y = 2000) +
#  scale_x_date(breaks = datas_x, labels = format(datas_x, format = "%d/%m")) +
#  labs(x = "Data", y = "Número de vacinados",
#       title = "Número de vacinados por dia em Maringá (PR)",
#       subtitle = "Número de vacinas registradas no sistema ConecteSUS por dia (Pode não representar o número real de vacinas aplicada por dia)",
#       caption = "Fonte: Datasus (https://opendatasus.saude.gov.br/dataset/covid-19-vacinacao)") +
#  theme(axis.text.x = element_text(angle = 90))
#  
#ggsave("../plot/a.pdf", grafico, height = 10, width = 12)
######
