# AFRepository-ETL-Excel
 
# Descrição dos Scripts

Este projeto contém scripts organizados em diferentes módulos para facilitar a manipulação de dados e a geração de respostas automatizadas. Abaixo está uma descrição dos principais scripts usados no projeto:

`src/app.py`
O `app.py` é o script principal do projeto, responsável por centralizar e coordenar a execução dos outros módulos. Ele serve como o ponto de entrada da aplicação, integrando as funcionalidades definidas nos scripts data_base.py e functions.py.

`src/classes/data_base.py`
O script `data_base.py` é responsável por operações relacionadas à base de dados, incluindo leitura, transformação e exportação de dados. Ele contém a classe data_base, que oferece as seguintes funcionalidades principais:

`Leitura de Arquivos`:

`lendo_arquivo_csv(arquivo)`: Leitura de arquivos CSV em um DataFrame.
`lendo_arquivo_excel(arquivo)`: Leitura de arquivos Excel em um DataFrame.
`Transformação de Dados`:

`dataframe_para_json(df)`: Converte um DataFrame para o formato JSON.
`join_pandas(df_a, df_b, campo, metodo)`: Realiza junções (join) entre dois DataFrames.
`Exportação de Dados`:

`criando_arquivo_csv(df, local_destino="", txt="", tiposi="")`: Cria e salva um DataFrame como um arquivo CSV no local especificado.
Este módulo é crucial para a gestão de dados, permitindo operações eficientes de leitura, manipulação e exportação de grandes volumes de dados.

`src/classes/functions.py`
O script `functions.py` complementa o data_base.py ao fornecer funcionalidades para análise de dados e formatação de respostas. Ele define a classe funcoes, que realiza operações avançadas em dados, tais como:

`Tratamento de Respostas`:

`remove_duplicatas(data, key)`: Remove duplicatas em uma lista de dicionários.
`reposta_certa(json_respostas, pergunta, resposta, tempo_execucao)`: Formata e registra respostas com informações adicionais.
`Consultas e Análises`:

`contagem_livros(df, json_respostas)`: Conta o total de livros no DataFrame.
`contagem_livros_um_autor(df, json_respostas)`: Conta livros com apenas um autor.
`listar_5_autores_com_mais_livros(df, json_respostas)`: Lista os 5 autores com mais livros e salva em CSV.
`contagem_livros_por_categoria(df, json_respostas)`: Conta livros por categoria e salva em CSV.
`listar_as_5_categorias_com_mais_livros(df, json_respostas)`: Lista as 5 categorias com mais livros e salva em CSV.
`formato_com_maior_quantidade_livros(df, json_respostas)`: Identifica o formato com mais livros.
`listar_livros_no_top_10_de_bestsellers(df, json_respostas)`: Lista os 10 livros mais bem posicionados em "bestsellers-rank" e salva em CSV.
`listar_livros_no_top_10_de_ratting_avg(df, json_respostas)`: Lista os 10 livros com melhores classificações (rating-avg) e salva em CSV.
`contagem_livros_com_maior_ratting_avg(df, json_respostas)`: Conta livros com classificação média maior que 3,5.
`contagem_livros_com_data_maior(df, json_respostas)`: Conta livros publicados após 1º de janeiro de 2020.
Este módulo é vital para realizar análises detalhadas e gerar relatórios automatizados com base nos dados disponíveis, facilitando a tomada de decisão e a extração de insights valiosos.