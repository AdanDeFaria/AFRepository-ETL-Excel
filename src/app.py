import pandas as pd
import numpy as np
import os
import json
import time
import datetime
import ast
from classes.data_base import data_base
from classes.functions import funcoes

#Classes
DB = data_base()
FC = funcoes()

#Variavel da Lista com as Respostas
json_respostas = []

#Local dos Arquivos
local_destino = [
    os.path.join('src', 'utils', 'authors.csv'),
    os.path.join('src', 'utils', 'categories.csv'),
    os.path.join('src', 'utils', 'dataset.csv'),
    os.path.join('src', 'utils', 'dataset.xlsx'),
    os.path.join('src', 'utils', 'formats.csv'),
]

#Criando os DataFrames
df_authors = DB.lendo_arquivo_csv(local_destino[0])
df_categories = DB.lendo_arquivo_csv(local_destino[1])
df_dataset = DB.lendo_arquivo_csv(local_destino[2])
df_formats = DB.lendo_arquivo_csv(local_destino[4])

#Formatação ou Correções:

#DataFrame dos Autores:
df_authors['author_name'] = df_authors['author_name'].replace(np.nan, '', regex=True)

#DataFrame do Dataset:
colunas_manter_dataset = [
    'id',
    'title',
    'authors',
    'bestsellers-rank',
    'categories',
    'format',
    'publication-date',
    'rating-avg'
]

df_dataset = df_dataset[colunas_manter_dataset]

colunas_formatar_para_zero = [
    'bestsellers-rank',
    'rating-avg'
]

df_dataset[colunas_formatar_para_zero] = df_dataset[colunas_formatar_para_zero].replace(np.nan, 0, regex=True)

df_dataset['publication-date'] = df_dataset['publication-date'].replace(np.nan, '1900-01-01 00:00:00', regex=True)

df_dataset['publication-date'] = pd.to_datetime(df_dataset['publication-date'])

colunas_formato_de_lista = [
    'authors',
    'categories',
]

df_dataset[colunas_formato_de_lista] = df_dataset[colunas_formato_de_lista].applymap(ast.literal_eval)

df_dataset.rename(
    columns={
        'authors':"author_id",
        'categories': "category_id",
        'format': 'format_id'
    }, inplace=True
)

df_dataset = df_dataset.drop_duplicates(subset=['id'])

#print(df_dataset)
#print(df_dataset.info())

#Gerando as Respostas:

#Reposta 1:
df_dataset = FC.contagem_livros(df_dataset, json_respostas)

#Reposta 2:
FC.contagem_livros_um_autor(df_dataset, json_respostas)

#Reposta 3:
df_dataset_dp = df_dataset.explode('author_id')

df_dataset_au_dp = DB.join_pandas(df_dataset_dp, df_authors, "author_id", "inner")

FC.listar_5_autores_com_mais_livros(df_dataset_au_dp, json_respostas)

#Reposta 4:
df_dataset_dp = df_dataset.explode('category_id')

df_dataset_ct_dp = DB.join_pandas(df_dataset_dp, df_categories, "category_id", "inner")

FC.contagem_livros_por_categoria(df_dataset_ct_dp, json_respostas)

#Reposta 5:
FC.listar_as_5_categorias_com_mais_livros(df_dataset_ct_dp, json_respostas)

df_dataset_ft = DB.join_pandas(df_dataset, df_formats, "format_id", "inner")

#Reposta 6:
FC.formato_com_maior_quantidade_livros(df_dataset_ft, json_respostas)

#Reposta 7:
FC.listar_livros_no_top_10_de_bestsellers(df_dataset, json_respostas)

#Reposta 8:
FC.listar_livros_no_top_10_de_ratting_avg(df_dataset, json_respostas)

#Reposta 9:
FC.contagem_livros_com_maior_ratting_avg(df_dataset, json_respostas)

#Reposta 10:
FC.contagem_livros_com_data_maior(df_dataset, json_respostas)

#Gerando o Arquivo de Respostas:
df_respostas = pd.json_normalize(json_respostas)

DB.criando_arquivo_csv(df_respostas, txt="respostas")