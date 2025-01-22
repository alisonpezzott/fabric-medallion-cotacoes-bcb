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
