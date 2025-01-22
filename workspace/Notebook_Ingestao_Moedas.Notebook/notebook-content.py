# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "460cfafc-e75c-4d4d-8b18-90e555066014",
# META       "default_lakehouse_name": "Lakehouse_Gold",
# META       "default_lakehouse_workspace_id": "4bfae9a9-f6fb-439e-95c0-acae90efcb15",
# META       "known_lakehouses": [
# META         {
# META           "id": "460cfafc-e75c-4d4d-8b18-90e555066014"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

import requests

url = "https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata/Moedas?$top=100&$format=json&$select=simbolo,nomeFormatado"

response = requests.get(url)

dados = response.json()['value']

df = spark.createDataFrame(dados)

df = df.selectExpr(
    "nomeFormatado AS MoedaNome",
    "simbolo AS Moeda"
)

df.write.mode('overwrite').saveAsTable('dim_moedas')


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT * FROM dim_moedas

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
