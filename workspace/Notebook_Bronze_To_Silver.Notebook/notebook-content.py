# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "9a6b8d46-0684-4a94-b242-f24bf9d41a82",
# META       "default_lakehouse_name": "Lakehouse_Silver",
# META       "default_lakehouse_workspace_id": "4bfae9a9-f6fb-439e-95c0-acae90efcb15",
# META       "known_lakehouses": [
# META         {
# META           "id": "f2573e5a-e241-49e9-bed3-5bbda0f1abc3"
# META         },
# META         {
# META           "id": "9a6b8d46-0684-4a94-b242-f24bf9d41a82"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Endereços dos lakehouses
workspace_id = "Medallion_BCB"
lake_bronze = "Lakehouse_Bronze.Lakehouse"
lake_silver = "Lakehouse_Bronze.Lakehouse" 

prefix = "abfss://"
mid = "@onelake.dfs.fabric.microsoft.com/"

path_bronze_files_novos = f"{prefix}{workspace_id}{mid}{lake_bronze}/Files/Cotacoes/Novos/"  

path_bronze_files_carregados = f"{prefix}{workspace_id}{mid}{lake_bronze}/Files/Cotacoes/Carregados/" 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Leitura dos arquivos Parquet na pasta Novos
df = spark.read.parquet(f"{path_bronze_files_novos}*.parquet")

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
# MAGIC CREATE TABLE IF NOT EXISTS cotacoes (
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
    MERGE INTO cotacoes AS e
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
# MAGIC SELECT COUNT(*) FROM cotacoes

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Movimentação dos arquivos parquet da pasta Novos para Carregados

from notebookutils import mssparkutils

# destino = path_bronze_files_novos 
# origem = path_bronze_files_carregados

origem = path_bronze_files_novos 
destino = path_bronze_files_carregados

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
