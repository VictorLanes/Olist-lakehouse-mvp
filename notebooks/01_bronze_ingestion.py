# Databricks notebook source
# MAGIC %md
# MAGIC # Camada Bronze — Ingestão dos Dados Brutos (Olist E-Commerce)
# MAGIC
# MAGIC **Objetivo:** ler os arquivos `.csv` originais do dataset *Brazilian E-Commerce Public Dataset by Olist*
# MAGIC (armazenados em um **Volume do Unity Catalog**) e persisti-los, sem transformação de conteúdo,
# MAGIC como tabelas **Delta** na camada Bronze, adicionando apenas metadados de ingestão.
# MAGIC
# MAGIC **Princípio da camada Bronze:** preservar o dado exatamente como veio da fonte (schema-on-read,
# MAGIC tipos majoritariamente `string`), garantindo rastreabilidade e possibilidade de reprocessamento.
# MAGIC
# MAGIC | Item | Valor |
# MAGIC |---|---|
# MAGIC | Fonte | Kaggle — `olistbr/brazilian-ecommerce` |
# MAGIC | Licença | CC BY-NC-SA 4.0 |
# MAGIC | Formato origem | CSV (UTF-8, separador `,`) |
# MAGIC | Formato destino | Delta Table (Unity Catalog) |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Parâmetros (widgets)
# MAGIC Ajuste os widgets abaixo conforme seu ambiente. Se você não tiver permissão para criar um
# MAGIC catálogo novo no Free Edition, use o catálogo padrão `workspace` (já existente na sua conta).

# COMMAND ----------

dbutils.widgets.text("catalog", "olist_lakehouse", "Catálogo Unity Catalog")
dbutils.widgets.text("volume_schema", "bronze", "Schema do Volume (staging)")
dbutils.widgets.text("volume_name", "raw_data", "Nome do Volume")

catalog = dbutils.widgets.get("catalog")
volume_schema = dbutils.widgets.get("volume_schema")
volume_name = dbutils.widgets.get("volume_name")

volume_path = f"/Volumes/{catalog}/{volume_schema}/{volume_name}"
print(f"Catálogo:      {catalog}")
print(f"Volume:        {volume_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Provisionamento do Unity Catalog
# MAGIC Cria o catálogo, os três schemas da Arquitetura Medalhão (`bronze`, `silver`, `gold`) e o
# MAGIC Volume onde os CSVs brutos devem ser enviados (upload manual via UI: *Catalog Explorer > Create > Volume*,
# MAGIC ou `Add data > Upload files to a volume`).

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
spark.sql(f"USE CATALOG {catalog}")

for schema in ["bronze", "silver", "gold"]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

spark.sql(f"""
    CREATE VOLUME IF NOT EXISTS {catalog}.{volume_schema}.{volume_name}
    COMMENT 'Landing zone para os CSVs originais do dataset Olist (upload manual via UI)'
""")

display(spark.sql(f"SHOW VOLUMES IN {catalog}.{volume_schema}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Checklist de upload manual
# MAGIC Antes de continuar, envie os 6 arquivos abaixo para o Volume `{volume_path}`
# MAGIC (Catalog Explorer → navegue até o volume → **Upload to this volume**, ou arraste os arquivos
# MAGIC pela tela *Add data*). Faça isso apenas **uma vez**.
# MAGIC
# MAGIC - `olist_orders_dataset.csv`
# MAGIC - `olist_order_items_dataset.csv`
# MAGIC - `olist_products_dataset.csv`
# MAGIC - `olist_customers_dataset.csv`
# MAGIC - `olist_order_reviews_dataset.csv`
# MAGIC - `olist_order_payments_dataset.csv`

# COMMAND ----------

files_in_volume = dbutils.fs.ls(volume_path)
display(files_in_volume)

expected = {
    "olist_orders_dataset.csv",
    "olist_order_items_dataset.csv",
    "olist_products_dataset.csv",
    "olist_customers_dataset.csv",
    "olist_order_reviews_dataset.csv",
    "olist_order_payments_dataset.csv",
}
found = {f.name for f in files_in_volume}
missing = expected - found
assert not missing, f"Arquivos ausentes no volume, faça o upload antes de continuar: {missing}"
print("OK — todos os arquivos esperados estão presentes no volume.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Função utilitária de ingestão
# MAGIC Cada CSV é lido com `inferSchema` (fase Bronze aceita tipos amplos), recebe três colunas de
# MAGIC metadados de ingestão e é gravado como tabela Delta gerenciada, em modo `overwrite`
# MAGIC (pipeline idempotente — pode ser reexecutado do zero a qualquer momento).

# COMMAND ----------

from pyspark.sql import functions as F

def ingest_csv_to_bronze(file_name: str, table_name: str):
    """Lê um CSV do volume de staging e grava como tabela Delta Bronze, com metadados de ingestão."""
    src_path = f"{volume_path}/{file_name}"

    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .option("multiLine", True)
        .option("escape", '"')
        .csv(src_path)
        .withColumn("_source_file", F.lit(file_name))
        .withColumn("_ingestion_timestamp", F.current_timestamp())
        .withColumn("_ingestion_date", F.current_date())
    )

    full_table_name = f"{catalog}.bronze.{table_name}"
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(full_table_name)
    )

    row_count = spark.table(full_table_name).count()
    print(f"[OK] {full_table_name:<40} {row_count:>8} linhas  <- {file_name}")
    return full_table_name

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Ingestão dos 6 datasets do escopo do MVP

# COMMAND ----------

bronze_tables = {
    "orders_raw":         "olist_orders_dataset.csv",
    "order_items_raw":    "olist_order_items_dataset.csv",
    "products_raw":       "olist_products_dataset.csv",
    "customers_raw":      "olist_customers_dataset.csv",
    "order_reviews_raw":  "olist_order_reviews_dataset.csv",
    "order_payments_raw": "olist_order_payments_dataset.csv",
}

for table_name, file_name in bronze_tables.items():
    ingest_csv_to_bronze(file_name, table_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Validação e evidências (print isto para o README)

# COMMAND ----------

display(spark.sql(f"SHOW TABLES IN {catalog}.bronze"))

# COMMAND ----------

for table_name in bronze_tables:
    full_name = f"{catalog}.bronze.{table_name}"
    cnt = spark.table(full_name).count()
    print(f"{full_name:<40} {cnt:>8} linhas")

# COMMAND ----------

display(spark.table(f"{catalog}.bronze.orders_raw").limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Descrição da tabela (comprovação de persistência em Delta)
# MAGIC Rode `DESCRIBE DETAIL` em qualquer tabela bronze — o campo `format` deve retornar `delta`.

# COMMAND ----------

display(spark.sql(f"DESCRIBE DETAIL {catalog}.bronze.orders_raw"))
