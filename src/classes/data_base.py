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
    def join_json(self, json_a, json_b, campo, metodo):
        json_final = []
        try:
            if metodo == "left":
                for dado_a in json_a:
                    if campo not in dado_a:
                            print("Não existe o Campo Informado no json_a!!!")
                            continue
                    for dado_b in json_b:
                        if campo not in dado_b:
                            print("Não existe o Campo Informado no json_b!!!")
                            continue
                        else:
                            if dado_a[f'{campo}'] == dado_b[f'{campo}']:
                                del dado_b[f'{campo}']
                                for chave, valor in dado_b.items():
                                    dado_a[f'{chave}'] = valor
                    json_final.append(dado_a)
                return json_final
            elif metodo == "right":
                for dado_b in json_b:
                    if campo not in dado_b:
                            print("Não existe o Campo Informado no json_b!!!")
                            continue
                    for dado_a in json_a:
                        if campo not in dado_a:
                            print("Não existe o Campo Informado no json_a!!!")
                            continue
                        else:
                            if dado_a[f'{campo}'] == dado_b[f'{campo}']:
                                del dado_a[f'{campo}']
                                for chave, valor in dado_a.items():
                                    dado_b[f'{chave}'] = valor
                    json_final.append(dado_b)
                return json_final
            elif metodo == "inner":
                for dado_a in json_a:
                    if campo not in dado_a:
                            print("Não existe o Campo Informado no json_a!!!")
                            continue
                    for dado_b in json_b:
                        if campo not in dado_b:
                            print("Não existe o Campo Informado no json_b!!!")
                            continue
                        else:
                            if dado_a[f'{campo}'] == dado_b[f'{campo}']:
                                del dado_b[f'{campo}']
                                for chave, valor in dado_b.items():
                                    dado_a[f'{chave}'] = valor
                                json_final.append(dado_a)
                return json_final
            elif metodo == "left_json_a_campo_lista_sem_dup":
                for dado_a_list in json_a:
                    list_campo = []
                    if campo not in dado_a_list:
                            print("Não existe o Campo Informado no json_a!!!")
                            continue
                    for dado_b in json_b:
                        if campo not in dado_b:
                            print("Não existe o Campo Informado no json_b!!!")
                            continue
                        else:
                            for dado_a in dado_a_list[f'{campo}']:
                                if dado_a == dado_b[f'{campo}']:
                                    del dado_b[f'{campo}']
                                    for chave, valor in dado_b.items():
                                        list_campo.append(valor)
                                        dado_a_list[f'{chave}'] = list_campo
                    json_final.append(dado_a_list)
            elif metodo == "left_json_a_campo_lista_com_dup":
                for dado_a_list in json_a:
                    if campo not in dado_a_list:
                            print("Não existe o Campo Informado no json_a!!!")
                            continue
                    for dado_a in dado_a_list[f'{campo}']:
                        for dado_b in json_b:
                            if campo not in dado_b:
                                print("Não existe o Campo Informado no json_b!!!")
                                continue
                            else:
                                if dado_a == dado_b[f'{campo}']:
                                    #del dado_b[f'{campo}']
                                    for chave, valor in dado_b.items():
                                        if chave != campo:
                                            dado_a_list[f'{chave}'] = valor
                                    json_final.append(dado_a_list)
                return json_final
            else:
                print("Metodo não Encontrado!!!") 
                return json_final
        except Exception as e:
            print(f"Erro: {e}") 
            return json_final

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

    def criando_arquivo_json(self, dic, local_destino="", txt="", tiposi=""):
        if local_destino == "":
            local_destino = self.local_destino
        if os.path.exists(os.path.join(local_destino, txt + tiposi + '.json')):
            os.remove(os.path.join(local_destino, txt + tiposi + '.json'))
            # abrindo o arquivo json para escrita
            with open(os.path.join(local_destino, txt + tiposi + '.json'), 'w') as json_file:
                # escrevendo o conteúdo no arquivo json
                json.dump(dic, json_file)
                return print(
                    f"Resposta salva com sucesso em {os.path.join(local_destino, txt + tiposi + '.json')}")
        else:
            # abrindo o arquivo json para escrita
            with open(os.path.join(local_destino, txt + tiposi + '.json'), 'w') as json_file:
                # escrevendo o conteúdo no arquivo json
                json.dump(dic, json_file)
                return print(
                    f"Resposta salva com sucesso em {os.path.join(local_destino, txt + tiposi + '.json')}")