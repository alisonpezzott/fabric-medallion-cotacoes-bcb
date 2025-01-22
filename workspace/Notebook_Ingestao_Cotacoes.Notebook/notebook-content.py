# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "b8fdb39a-0195-4c04-acc0-c8748dd9395f",
# META       "default_lakehouse_name": "Lakehouse_Bronze",
# META       "default_lakehouse_workspace_id": "43fc4b91-88cc-4d70-a8ad-8cba40bbf749",
# META       "known_lakehouses": [
# META         {
# META           "id": "b8fdb39a-0195-4c04-acc0-c8748dd9395f"
# META         },
# META         {
# META           "id": "65813ff8-3f08-4f1d-830c-63609257a8da"
# META         },
# META         {
# META           "id": "9da8f864-fec9-4557-9ac5-06ee7c35f411"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
import requests
import json
from datetime import *

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Lista das moedas
df_moedas = spark.sql("SELECT * FROM Lakehouse_Gold.dim_moedas")
lista_moedas = [row['Moeda'] for row in df_moedas.collect()]
print(lista_moedas)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Variáveis
# moeda = 'EUR'
data_inicial = '01-01-2022'
# data_final = '12-31-2021'
data_final = datetime.now().strftime('%m-%d-%Y') 
top = 100
skip = 0 

for moeda in lista_moedas:

    skip = 0

    todos_dados = []

    while True:

        url = (
            f"https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata/"
            f"CotacaoMoedaPeriodo(moeda=@moeda,dataInicial=@dataInicial,dataFinalCotacao=@dataFinalCotacao)?"
            f"@moeda='{moeda}'&@dataInicial='{data_inicial}'&@dataFinalCotacao='{data_final}'"
            f"&$top={top}&$skip={skip}&$filter=tipoBoletim%20eq%20'Fechamento'&$format=json&$select=cotacaoCompra,dataHoraCotacao"
        )

        response = requests.get(url)

        dados = response.json()['value']

        if not dados:
            break

        todos_dados.extend(dados)

        skip += top

    if todos_dados:
        df = spark.createDataFrame(todos_dados) \
            .withColumn('moeda', lit(moeda))

    data_inicial_path = datetime.strptime(data_inicial, "%m-%d-%Y").strftime("%Y%m%d")
    data_final_path = datetime.strptime(data_final, "%m-%d-%Y").strftime("%Y%m%d")

    # Certifique-se que o Lakehouse_Bronze é o padrão deste notebook
    path = (
        f"Files/Cotacoes/Novos/"
        f"{moeda}-"
        f"{data_inicial_path}_"
        f"{data_final_path}"
        f".parquet"
    )

    df.write.mode('overwrite').parquet(path)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
