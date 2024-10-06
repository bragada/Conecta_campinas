check_and_install <- function(pkg) {
  if (!require(pkg, character.only = TRUE)) {
    install.packages(pkg, dependencies = TRUE)
    library(pkg, character.only = TRUE)
  }
}

# Lista de pacotes necessários
required_packages <- c("httr", "jsonlite", "janitor", "tidyverse", "aws.s3", "arrow")

# Verificação e instalação dos pacotes
lapply(required_packages, check_and_install)


`%!in%` <- Negate(`%in%`) 



