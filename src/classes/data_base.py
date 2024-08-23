import pandas as pd
import os
import json
import time
import datetime


class data_base():
    def __init__(self):
        self.caminho_arquivo = ""
        self.local_destino = os.path.join('src', 'utils')
        self.arquivo = ""
    def lendo_arquivo_csv(self, arquivo):
        df = pd.read_csv(arquivo, sep=",", encoding="UTF-8")
        #print(df)
        #print(df.describe())
        #print(df.dtypes)
        #print(df.info())
        #print(df.shape)
        return df
    def lendo_arquivo_excel(self, arquivo):
        df = pd.read_excel(arquivo)
        #print(df)
        #print(df.describe())
        #print(df.dtypes)
        #print(df.info())
        #print(df.shape)
        return df
    def dataframe_para_json(self, df):
        json_dados = df.to_dict(orient='records')
        return json_dados
    
    def join_pandas(self, df_a, df_b, campo, metodo):
        df_final = pd.merge(df_a, df_b, on=f'{campo}', how=f'{metodo}')
        return df_final

    def criando_arquivo_csv(self, df, local_destino="", txt="", tiposi=""):
        if local_destino == "":
            local_destino = self.local_destino
        if os.path.exists(os.path.join(local_destino, txt + tiposi + '.csv')):
            os.remove(os.path.join(local_destino, txt + tiposi + '.csv'))
            df.to_csv(os.path.join(local_destino, txt + tiposi + '.csv'),
                                header=True,
                                index=False,
                                sep=";",
                                encoding="UTF-8"
                                )
            return print(f"Criamos o arquivo {os.path.join(local_destino, txt + tiposi + '.csv')}!")
        else:
            df.to_csv(os.path.join(local_destino, txt + tiposi + '.csv'),
                                header=True,
                                index=False,
                                sep=";",
                                encoding="UTF-8"
                                )
            return print(f"Criamos o arquivo {os.path.join(local_destino, txt + tiposi + '.csv')}!")
