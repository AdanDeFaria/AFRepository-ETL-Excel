import pandas as pd
import os
import json
import time
from datetime import datetime
from classes.data_base import data_base

DB = data_base()

class funcoes():
    def __init__(self):
        self.caminho_arquivo = ""
        self.local_destino = os.path.join('src', 'utils')
        self.arquivo = ""

    def remove_duplicatas(self, data, key):
        seen = set()
        result = []
        try:
            for item in data:
                # Use o valor do campo especificado como chave para verificar duplicatas
                value = item[key]
                
                if value not in seen:
                    seen.add(value)
                    result.append(item)
        except Exception as e:
            print(f"Erro: {e}")
        else:
            return result
    
    def reposta_certa(self, json_respostas, pergunta, resposta, tempo_execucao):
        json_reposta = {"Pergunta":pergunta, "Resposta":resposta, "TempoExecucao":tempo_execucao, "DataRegistro":datetime.now()}
        try:
            json_respostas.append(json_reposta)
        except Exception as e:
            print(f"Erro: {e}")
        else:
            print(f"Ok: Reposta Registrada com Sucesso!!!")

    def contagem_livros(self, df, json_respostas):
        start_time = datetime.now()
        pergunta = "Qual a quantidade total de livros da base?"
        resposta = len(df)
        end_time = datetime.now()
        tempo_execucao = end_time - start_time
        self.reposta_certa(json_respostas, pergunta, resposta, tempo_execucao)
        return df
    def contagem_livros_um_autor(self, df, json_respostas):
        start_time = datetime.now()
        pergunta = "Qual a quantidade de livros que possui apenas 1 autor?"
        df = df[df['author_id'].apply(lambda x: len(x) == 1)]
        resposta = len(df)
        end_time = datetime.now()
        tempo_execucao = end_time - start_time
        self.reposta_certa(json_respostas, pergunta, resposta, tempo_execucao)
    def listar_5_autores_com_mais_livros(self, df, json_respostas):
        start_time = datetime.now()
        pergunta = "Quais os 5 autores com a maior quantidade de livros?"
        df_aggregated = df.groupby('author_name').agg(amount=('id', 'count')).reset_index()
        df_aggregated = df_aggregated.sort_values(by='amount', ascending=False)
        df_aggregated['rank'] = df_aggregated['amount'].rank(method='dense', ascending=False).astype(int)
        df_aggregated = df_aggregated.head(5)
        DB.criando_arquivo_csv(df_aggregated, txt="listando_e_rankeando_os_5_melhores_autores_por_quantidade_de_livros")
        resposta = "Está no Arquivo 'src/utils/listando_e_rankeando_os_5_melhores_autores_por_quantidade_de_livros.csv'"
        end_time = datetime.now()
        tempo_execucao = end_time - start_time
        self.reposta_certa(json_respostas, pergunta, resposta, tempo_execucao)
    def contagem_livros_por_categoria(self, df, json_respostas):
        start_time = datetime.now()
        pergunta = "Qual a quantidade de livros por categoria?"
        df_aggregated = df.groupby('category_name').agg(amount=('id', 'count')).reset_index()
        df_aggregated = df_aggregated.sort_values(by='amount', ascending=False)
        df_aggregated['rank'] = df_aggregated['amount'].rank(method='dense', ascending=False).astype(int)
        DB.criando_arquivo_csv(df_aggregated, txt="listando_categorias_por_quantidade_de_livros")
        resposta = "Está no Arquivo 'src/utils/listando_categorias_por_quantidade_de_livros.csv'"
        end_time = datetime.now()
        tempo_execucao = end_time - start_time
        self.reposta_certa(json_respostas, pergunta, resposta, tempo_execucao)
    def listar_as_5_categorias_com_mais_livros(self, df, json_respostas):
        start_time = datetime.now()
        pergunta = "Quais as 5 categorias com a maior quantidade de livros?"
        df_aggregated = df.groupby('category_name').agg(amount=('id', 'count')).reset_index()
        df_aggregated = df_aggregated.sort_values(by='amount', ascending=False)
        df_aggregated['rank'] = df_aggregated['amount'].rank(method='dense', ascending=False).astype(int)
        df_aggregated = df_aggregated.head(5)
        DB.criando_arquivo_csv(df_aggregated, txt="listando_e_rankeando_as_5_melhores_categorias_por_quantidade_de_livros")
        resposta = "Está no Arquivo 'src/utils/listando_e_rankeando_as_5_melhores_categorias_por_quantidade_de_livros.csv'"
        end_time = datetime.now()
        tempo_execucao = end_time - start_time
        self.reposta_certa(json_respostas, pergunta, resposta, tempo_execucao)
    def formato_com_maior_quantidade_livros(self, df, json_respostas):
        start_time = datetime.now()
        pergunta = "Qual o formato com a maior quantidade de livros?"
        df_aggregated = df.groupby('format_name').agg(amount=('id', 'count')).reset_index()
        df_aggregated = df_aggregated.sort_values(by='amount', ascending=False)
        df_aggregated['rank'] = df_aggregated['amount'].rank(method='dense', ascending=False).astype(int)
        df_aggregated = df_aggregated.head(1)
        json_agg = DB.dataframe_para_json(df_aggregated)
        resposta = f"Formato com mais livros é {json_agg[0]['format_name']} com {json_agg[0]['amount']} livros"
        end_time = datetime.now()
        tempo_execucao = end_time - start_time
        self.reposta_certa(json_respostas, pergunta, resposta, tempo_execucao)
    def listar_livros_no_top_10_de_bestsellers(self, df, json_respostas):
        start_time = datetime.now()
        pergunta = 'Considerando a coluna “bestsellers-rank”, quais os 10 livros mais bem posicionados?'
        df = df[df['bestsellers-rank'] != 0]
        df = df.sort_values(by='bestsellers-rank', ascending=True)
        colunas_manter = [
            'title',
            'bestsellers-rank'
        ]
        df = df[colunas_manter]
        df = df.head(10)
        DB.criando_arquivo_csv(df, txt="listando_e_rankeando_os_10_melhores_bestsellers")
        resposta = "Está no Arquivo 'src/utils/listando_e_rankeando_os_10_melhores_bestsellers.csv'"
        end_time = datetime.now()
        tempo_execucao = end_time - start_time
        self.reposta_certa(json_respostas, pergunta, resposta, tempo_execucao)
    def listar_livros_no_top_10_de_ratting_avg(self, df, json_respostas):
        start_time = datetime.now()
        pergunta = 'Considerando a coluna “rating-avg”, quais os 10 livros mais bem posicionados?'
        df = df[df['rating-avg'] != 0]
        df = df.sort_values(by='rating-avg', ascending=False)
        colunas_manter = [
            'title',
            'rating-avg'
        ]
        df = df[colunas_manter]
        df = df.head(10)
        DB.criando_arquivo_csv(df, txt="listando_e_rankeando_os_10_melhores_rating-avg")
        resposta = "Está no Arquivo 'src/utils/listando_e_rankeando_os_10_melhores_rating-avg.csv'"
        end_time = datetime.now()
        tempo_execucao = end_time - start_time
        self.reposta_certa(json_respostas, pergunta, resposta, tempo_execucao)
    def contagem_livros_com_maior_ratting_avg(self, df, json_respostas):
        start_time = datetime.now()
        pergunta = "Quantos livros possuem “rating-avg” maior do que 3,5?"
        df = df[df['rating-avg'] > 3.5]
        df = df.sort_values(by='rating-avg', ascending=False)
        colunas_manter = [
            'title',
            'rating-avg'
        ]
        df = df[colunas_manter]
        resposta = len(df)
        end_time = datetime.now()
        tempo_execucao = end_time - start_time
        self.reposta_certa(json_respostas, pergunta, resposta, tempo_execucao)
    def contagem_livros_com_data_maior(self, df, json_respostas):
        start_time = datetime.now()
        pergunta = "Quantos livros tem data de publicação (publication-date) maior do que 01-01-2020?"
        data_comparacao = pd.to_datetime('2020-01-01')
        df = df[df['publication-date'] > data_comparacao]
        resposta = len(df)
        end_time = datetime.now()
        tempo_execucao = end_time - start_time
        self.reposta_certa(json_respostas, pergunta, resposta, tempo_execucao)