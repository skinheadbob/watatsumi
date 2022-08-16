# Databricks notebook source
# MAGIC %run ./001.install_lib

# COMMAND ----------

# MAGIC %run ./002.env_config

# COMMAND ----------

# MAGIC %run ./003.infra_dbws_setup

# COMMAND ----------

# MAGIC %run ./004.lake_util

# COMMAND ----------

# MAGIC %run ./005.lake_lib/000.define

# COMMAND ----------

def init_nb( specific_enc:str=None ):
  set_env( specific_enc )
  ensure_mnt()
  
  ensure_meta_database_exists()
  use_meta_database()
  
  spark.conf.set("spark.databricks.io.cache.enabled",'true')
  return

# COMMAND ----------

print(f"""
please call "init_nb()" (init_notebook) in the next cell
""")
