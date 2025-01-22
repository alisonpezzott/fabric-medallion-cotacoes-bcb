# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "9da8f864-fec9-4557-9ac5-06ee7c35f411",
# META       "default_lakehouse_name": "Lakehouse_Silver",
# META       "default_lakehouse_workspace_id": "43fc4b91-88cc-4d70-a8ad-8cba40bbf749",
# META       "known_lakehouses": [
# META         {
# META           "id": "b8fdb39a-0195-4c04-acc0-c8748dd9395f"
# META         },
# META         {
# META           "id": "9da8f864-fec9-4557-9ac5-06ee7c35f411"
# META         },
# META         {
# META           "id": "65813ff8-3f08-4f1d-830c-63609257a8da"
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

# Leitura dos arquivos Parquet na pasta Novos
df = spark.read.parquet(f"abfss://Medallion_BCB@onelake.dfs.fabric.microsoft.com/Lakehouse_Bronze.Lakehouse/Files/Cotacoes/Novos/*.parquet")

df.createOrReplaceTempView("df")

df = spark.sql("""
    SELECT 
        cotacaoCompra AS Cotacao,
        CAST(dataHoraCotacao AS DATE) AS Data,
        moeda AS Moeda
    FROM
        df
    ORDER BY Data ASC
""").dropDuplicates(["Moeda", "Data"])

display(df)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC -- Cria uma tabela no lake se não existir
# MAGIC CREATE TABLE IF NOT EXISTS Lakehouse_Silver.cotacoes (
# MAGIC     Cotacao DOUBLE,
# MAGIC     Data DATE,
# MAGIC     Moeda STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (Moeda)

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Executa o merge na tabela cotacoes

df.createOrReplaceTempView("df_novos")

spark.sql("""
    MERGE INTO Lakehouse_Silver.cotacoes AS e
    USING (
        SELECT
            Cotacao,
            Data,
            Moeda
        FROM
            df_novos
    ) as n
    ON e.Moeda = n.Moeda
        AND e.Data = n.Data
    WHEN NOT MATCHED THEN
        INSERT (Cotacao, Data, Moeda)
        VALUES (n.Cotacao, n.Data, n. Moeda)
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC -- Contagem de linhas
# MAGIC SELECT COUNT(*) FROM Lakehouse_Silver.cotacoes

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Movimentação dos arquivos parquet da pasta Novos para Carregados

from notebookutils import mssparkutils

origem = f"abfss://Medallion_BCB@onelake.dfs.fabric.microsoft.com/Lakehouse_Bronze.Lakehouse/Files/Cotacoes/Novos/"  
destino = f"abfss://Medallion_BCB@onelake.dfs.fabric.microsoft.com/Lakehouse_Bronze.Lakehouse/Files/Cotacoes/Carregados/" 

if not mssparkutils.fs.exists(destino):
    mssparkutils.fs.mkdirs(destino)

arquivos = mssparkutils.fs.ls(origem)

for arquivo in arquivos:
    caminho_origem = arquivo.path
    nome_arquivo = arquivo.name
    caminho_destino = f"{destino}{nome_arquivo}"
    
    mssparkutils.fs.mv(caminho_origem, caminho_destino)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
