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
# META           "id": "9a6b8d46-0684-4a94-b242-f24bf9d41a82"
# META         },
# META         {
# META           "id": "460cfafc-e75c-4d4d-8b18-90e555066014"
# META         },
# META         {
# META           "id": "f2573e5a-e241-49e9-bed3-5bbda0f1abc3"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

lakehouse_silver = 'Lakehouse_Silver'
lakehouse_bronze = 'Lakehouse_Bronze'
lakehouse_gold = 'Lakehouse_Gold'

spark.sql(f"""
-- Cria ou substitui a tabela Delta
CREATE OR REPLACE TABLE {lakehouse_gold}.fact_cotacoes
USING DELTA
AS

-- Extrai mínimas e máximas datas
WITH min_max AS (
  SELECT MIN(Data) AS min_data, 
         MAX(Data) AS max_data
  FROM {lakehouse_silver}.cotacoes
),

-- Gera sequência de datas
datas_periodo AS (
  SELECT explode(
    sequence(
      d.min_data,
      d.max_data,
      interval 1 day
    )
  ) AS Data
  FROM min_max d
),

-- Moedas distintas
moedas AS (
  SELECT DISTINCT Moeda
  FROM {lakehouse_gold}.dim_moedas
),

-- Faz o plano cartesiano entre moedas e datas
cross_tb AS (
  SELECT m.Moeda, d.Data
  FROM moedas m
  CROSS JOIN datas_periodo d
),

-- Dias sem cotações
datas_faltantes AS (
  SELECT 
    cr.Moeda,
    cr.Data
  FROM cross_tb cr
  LEFT JOIN (
    SELECT DISTINCT 
        Moeda, 
        Data
    FROM {lakehouse_silver}.cotacoes
  ) c
  ON cr.Moeda = c.Moeda AND cr.Data = c.Data
  WHERE c.Data IS NULL
),

-- Adiciona as linhas faltantes
linhas_adicionadas AS (
  SELECT
    dt_falt.Moeda,
    dt_falt.Data,   -- Corrigido para "Data"
    NULL AS Cotacao -- Valores nulos para cotações adicionadas
  FROM datas_faltantes dt_falt
),

-- Combina as cotações existentes com as linhas adicionadas
append AS (
  SELECT
    Moeda,
    Data,
    Cotacao
  FROM {lakehouse_silver}.cotacoes

  UNION ALL

  SELECT
    Moeda,
    Data,
    Cotacao
  FROM linhas_adicionadas
)

-- Forward Fill: Preenche os valores de cotação para as novas linhas
SELECT
  Moeda,
  Data,
  last_value(Cotacao, true) 
    OVER (
      PARTITION BY Moeda 
      ORDER BY Data
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS Cotacao
FROM append
ORDER BY Data, Moeda;
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
