# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "65813ff8-3f08-4f1d-830c-63609257a8da",
# META       "default_lakehouse_name": "Lakehouse_Gold",
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

# MAGIC %%sql
# MAGIC 
# MAGIC -- Cria ou substitui a tabela Delta
# MAGIC CREATE OR REPLACE TABLE Lakehouse_Gold.fact_cotacoes
# MAGIC USING DELTA
# MAGIC AS
# MAGIC 
# MAGIC -- Extrai mínimas e máximas datas
# MAGIC WITH min_max AS (
# MAGIC   SELECT MIN(Data) AS min_data, 
# MAGIC          MAX(Data) AS max_data
# MAGIC   FROM Lakehouse_Silver.cotacoes
# MAGIC ),
# MAGIC 
# MAGIC -- Gera sequência de datas
# MAGIC datas_periodo AS (
# MAGIC   SELECT explode(
# MAGIC     sequence(
# MAGIC       d.min_data,
# MAGIC       d.max_data,
# MAGIC       interval 1 day
# MAGIC     )
# MAGIC   ) AS Data
# MAGIC   FROM min_max d
# MAGIC ),
# MAGIC 
# MAGIC -- Moedas distintas
# MAGIC moedas AS (
# MAGIC   SELECT DISTINCT Moeda
# MAGIC   FROM Lakehouse_Gold.dim_moedas
# MAGIC ),
# MAGIC 
# MAGIC -- Faz o plano cartesiano entre moedas e datas
# MAGIC cross_tb AS (
# MAGIC   SELECT m.Moeda, d.Data
# MAGIC   FROM moedas m
# MAGIC   CROSS JOIN datas_periodo d
# MAGIC ),
# MAGIC 
# MAGIC -- Dias sem cotações
# MAGIC datas_faltantes AS (
# MAGIC   SELECT 
# MAGIC     cr.Moeda,
# MAGIC     cr.Data
# MAGIC   FROM cross_tb cr
# MAGIC   LEFT JOIN (
# MAGIC     SELECT DISTINCT 
# MAGIC         Moeda, 
# MAGIC         Data
# MAGIC     FROM Lakehouse_Silver.cotacoes
# MAGIC   ) c
# MAGIC   ON cr.Moeda = c.Moeda AND cr.Data = c.Data
# MAGIC   WHERE c.Data IS NULL
# MAGIC ),
# MAGIC 
# MAGIC -- Adiciona as linhas faltantes
# MAGIC linhas_adicionadas AS (
# MAGIC   SELECT
# MAGIC     dt_falt.Moeda,
# MAGIC     dt_falt.Data,   -- Corrigido para "Data"
# MAGIC     NULL AS Cotacao -- Valores nulos para cotações adicionadas
# MAGIC   FROM datas_faltantes dt_falt
# MAGIC ),
# MAGIC 
# MAGIC -- Combina as cotações existentes com as linhas adicionadas
# MAGIC append AS (
# MAGIC   SELECT
# MAGIC     Moeda,
# MAGIC     Data,
# MAGIC     Cotacao
# MAGIC   FROM Lakehouse_Silver.cotacoes
# MAGIC 
# MAGIC   UNION ALL
# MAGIC 
# MAGIC   SELECT
# MAGIC     Moeda,
# MAGIC     Data,
# MAGIC     Cotacao
# MAGIC   FROM linhas_adicionadas
# MAGIC )
# MAGIC 
# MAGIC -- Forward Fill: Preenche os valores de cotação para as novas linhas
# MAGIC SELECT
# MAGIC   Moeda,
# MAGIC   Data,
# MAGIC   last_value(Cotacao, true) 
# MAGIC     OVER (
# MAGIC       PARTITION BY Moeda 
# MAGIC       ORDER BY Data
# MAGIC       ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
# MAGIC     ) AS Cotacao
# MAGIC FROM append
# MAGIC ORDER BY Data, Moeda;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
