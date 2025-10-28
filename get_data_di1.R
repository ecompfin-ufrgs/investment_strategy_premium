library(rb3)
library(dplyr)
library(arrow)

# Busca os contratos a partir de 2000
df <- futures_mget(
  first_date = "2004-01-01",
  last_date = "2024-12-31",
  by = 5
)

df <- df[df$commodity == "DI1", ]


### Salvar em parquet
write_parquet(df, "C:/Users/Bernardo Machado/Downloads/data_di1.parquet")
