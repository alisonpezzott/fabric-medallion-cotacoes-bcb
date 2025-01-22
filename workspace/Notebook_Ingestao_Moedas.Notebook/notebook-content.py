# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "f2573e5a-e241-49e9-bed3-5bbda0f1abc3",
# META       "default_lakehouse_name": "Lakehouse_Bronze",
# META       "default_lakehouse_workspace_id": "4bfae9a9-f6fb-439e-95c0-acae90efcb15"
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
