# -*- coding: utf-8 -*-
"""
Created on Sun Sep  7 21:15:51 2025

@author: Bernardo Machado
"""

import pandas as pd
from pyettj import ettj
from pandas.tseries.offsets import BDay

# Criar um range de datas úteis de 01/01/2004 até 31/12/2024
datas = pd.date_range(start='2004-01-01', end='2024-12-31', freq=BDay())

# Lista para armazenar todos os DataFrames
lista_dfs = []

# Loop para buscar os dados
for data in datas:
    data_str = data.strftime('%d/%m/%Y')
    try:
        df = ettj.get_ettj(data_str, curva="PRE")
        # Adicionar coluna de data
        df['Data'] = data
        lista_dfs.append(df)
    except Exception:
        # Ignora datas sem dados
        continue

# Concatenar todos os DataFrames em um único
df_final = pd.concat(lista_dfs, ignore_index=True)

# Visualizar
print(df_final.head())


# Caminho do arquivo
caminho_arquivo = r"C:\Users\Bernardo Machado\Downloads\ettj_2004_2024.parquet"

# Salvar o DataFrame em formato Parquet
df_final.to_parquet(caminho_arquivo, engine='pyarrow', index=False)

print(f"Arquivo salvo em: {caminho_arquivo}")

