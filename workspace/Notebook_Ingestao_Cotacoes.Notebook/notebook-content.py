# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "ddf1ba0b-54d0-45f1-8f30-5eeffda938fb",
# META       "default_lakehouse_name": "Lakehouse_Bronze",
# META       "default_lakehouse_workspace_id": "6d58cb94-6775-4d5b-9df0-58f6164a6339",
# META       "known_lakehouses": [
# META         {
# META           "id": "ddf1ba0b-54d0-45f1-8f30-5eeffda938fb"
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

df_moedas = spark.sql("SELECT Moeda FROM dim_moedas")
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
