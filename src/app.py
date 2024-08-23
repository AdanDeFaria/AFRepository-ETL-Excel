import pandas as pd
import numpy as np
import os
import json
import time
import datetime
import ast
from classes.data_base import data_base
from classes.functions import funcoes

DB = data_base()
FC = funcoes()

json_respostas = []

local_destino = [
    os.path.join('src', 'utils', 'authors.csv'),
    os.path.join('src', 'utils', 'categories.csv'),
    os.path.join('src', 'utils', 'dataset.csv'),
    os.path.join('src', 'utils', 'dataset.xlsx'),
    os.path.join('src', 'utils', 'formats.csv'),
]

df_authors = DB.lendo_arquivo_csv(local_destino[0])
df_categories = DB.lendo_arquivo_csv(local_destino[1])
df_dataset = DB.lendo_arquivo_csv(local_destino[2])
df_formats = DB.lendo_arquivo_csv(local_destino[4])

df_authors['author_name'] = df_authors['author_name'].replace(np.nan, '', regex=True)

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

print(df_dataset)
print(df_dataset.info())

df_dataset = FC.contagem_livros(df_dataset, json_respostas)

FC.contagem_livros_um_autor(df_dataset, json_respostas)

df_dataset_dp = df_dataset.explode('author_id')

df_dataset_au_dp = DB.join_pandas(df_dataset_dp, df_authors, "author_id", "inner")

FC.listar_5_autores_com_mais_livros(df_dataset_au_dp, json_respostas)

#json_dataset_ft = DB.dataframe_para_json(df_dataset)

#json_authors = DB.dataframe_para_json(df_authors)

#print(json_authors)

#json_dataset_ft_au = DB.join_json(json_dataset_ft, json_authors, "author_id", "left_json_a_campo_lista_com_dup")

#print(json_dataset_ft_au)

#df_dataset_au = pd.json_normalize(json_dataset_ft_au)

#print(df_dataset_au.head(2))

df_dataset_dp = df_dataset.explode('category_id')

df_dataset_ct_dp = DB.join_pandas(df_dataset_dp, df_categories, "category_id", "inner")

FC.contagem_livros_por_categoria(df_dataset_ct_dp, json_respostas)

FC.listar_as_5_categorias_com_mais_livros(df_dataset_ct_dp, json_respostas)

df_dataset_ft = DB.join_pandas(df_dataset, df_formats, "format_id", "inner")

FC.formato_com_maior_quantidade_livros(df_dataset_ft, json_respostas)

FC.listar_livros_no_top_10_de_bestsellers(df_dataset, json_respostas)

FC.listar_livros_no_top_10_de_ratting_avg(df_dataset, json_respostas)

FC.contagem_livros_com_maior_ratting_avg(df_dataset, json_respostas)

FC.contagem_livros_com_data_maior(df_dataset, json_respostas)

df_respostas = pd.json_normalize(json_respostas)

DB.criando_arquivo_csv(df_respostas, txt="respostas")