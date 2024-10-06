install.packages("janitor")
library(httr)
library(jsonlite)
library(janitor)
library(tidyverse)
library(aws.s3)
library(arrow)


# Solicitações ----
sol_extrai_json_api <- function(nome,url,raiz_1,raiz_2){
  
  
  
  
  response <- GET(url, add_headers(`Accept-Encoding` = "gzip"))
  
  dados <- fromJSON(content(response, "text"))
  if (status_code(response) != 200) {
    message("Erro ao acessar a API de ",nome ,". Status code: ", status_code(response))
    return(NULL)
  } 
  
  
  dados <- fromJSON(content(response, "text")) %>% 
    .[["RAIZ"]] %>%
    .[[raiz_1]] %>%
    .[[raiz_2]]
  
  
  if (length(dados) <= 10) {
    message("A base de dados contém 10 ou menos observações. Não será feito o upload.")
    return(NULL)
  }
  
  solicitacoes <- dados %>% 
    clean_names() %>%
    select(protocolo = numero_protocolo,
           data_reclamacao,
           status = desc_status_solicitacao,
           tempo_restante = desc_prazo_restante,
           data_reclamacao,
           id_ocorrencia,
           possui_atendimento_anterior,
           endereco_livre_solicitacao,
           origem_ocorrencia = desc_tipo_origem_solicitacao,
           pontos
    ) %>% 
    mutate(data_reclamacao = as.Date(data_reclamacao,"%d/%m/%Y"),
           semana_marco = week(data_reclamacao)-week(as.Date("2023-02-25")),
           mes = month(data_reclamacao),
           mes = case_when(
             mes == 1 ~ "Janeiro",
             mes == 2 ~ "Fevereiro",
             mes == 3 ~ "Março",
             mes == 4 ~ "Abril",
             mes == 5 ~ "Maio",
             mes == 6 ~ "Junho",
             mes == 7 ~ "Julho",
             mes == 8 ~ "Agosto",
             mes == 9 ~ "Setembro",
             mes == 10 ~ "Outubro",
             mes == 11 ~ "Novembro",
             mes == 12 ~ "Dezembro",
           ),
           mes = factor(mes,levels = c("Janeiro","Fevereiro","Março","Abril","Maio","Junho","Julho","Agosto","Setembro","Outubro","Novembro","Dezembro")),
           dia_semana = wday(data_reclamacao,label = T),
           dia_semana = case_when(
             dia_semana %in% c("dom","Sun") ~ "Dom",
             dia_semana %in% c("seg","Mon") ~ "Seg",
             dia_semana %in% c("ter","Tue") ~ "Ter",
             dia_semana %in% c("qua","Wed") ~ "Qua",
             dia_semana %in% c("qui","Thu") ~ "Qui",
             dia_semana %in% c("sex","Fri") ~ "Sex",
             dia_semana %in% c("sab","Sat") ~ "Sab"
             
           ),
           semana = week(data_reclamacao) - week(floor_date(data_reclamacao,"month")) +1) 
  
  
  arrow::write_parquet(solicitacoes, "tt_solicitacoes.parquet")
  
  put_object(
    file = "tt_solicitacoes.parquet",
    object = "tt_solicitacoes.parquet",
    bucket = "automacao-conecta",
    region = 'sa-east-1'
  )
  
}

sol_extrai_json_api(nome = "Solicitações",
                    raiz_1 = "SOLICITACOES",
                    raiz_2 = "SOLICITACAO",
                    url= "https://conectacampinas.exati.com.br/guia/command/conectacampinas/Solicitacoes.json?CMD_ID_STATUS_SOLICITACAO=-1&CMD_IDS_PARQUE_SERVICO=1,2&CMD_DATA_RECLAMACAO=01/03/2023&CMD_APENAS_EM_ABERTO=0&auth_token=eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJnaW92YW5uYS5hbmRyYWRlQGV4YXRpLmNvbS5iciIsImp0aSI6IjMxOCIsImlhdCI6MTcyNjcwMzY5Nywib3JpZ2luIjoiR1VJQS1TRVJWSUNFIn0.N-NFG7oJSzfzhyApzR9VB5P0AqSmDd_CqZrAEtlZsEs"
) 
print('Solicitações - Ok')

# ----
