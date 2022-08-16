# Databricks notebook source
# DBTITLE 1,define: ensure_meta_database_exists(), use_meta_database()
def ensure_meta_database_exists():
  spark.sql(f'CREATE DATABASE IF NOT EXISTS { get_cfg("infra.dbws.hive_database") }')
  return
def use_meta_database():
  spark.sql(f'USE DATABASE { get_cfg("infra.dbws.hive_database") }')
  return

# COMMAND ----------

# DBTITLE 1,notebook-wise util functions
def _assert_tablename( tablename:str ):
  global _WATATSUMI_ZONE_TABLENAME_PREFIX_DICT
  acceptable_prefixes = list( _WATATSUMI_ZONE_TABLENAME_PREFIX_DICT.values() )
  if not any( map(lambda prefix: tablename.startswith(prefix), acceptable_prefixes)):
    raise RuntimeError(f'bad tablename "{tablename}", must start with one of: {acceptable_prefixes}')
  return

def _get_physical_table_dbws_mount_path( tablename:str ):
  zone = next(zone for zone,prefix in _WATATSUMI_ZONE_TABLENAME_PREFIX_DICT.items() if tablename.startswith(prefix))
  physical_table_dbws_mount_path = f"{get_cfg('infra.dbws.mount_point')}/{zone}_zone/{tablename}"
  return physical_table_dbws_mount_path

# COMMAND ----------

# DBTITLE 1,define: ensure_lake_table_exists()
from pyspark.sql.types import StructType
from typing import List,Union
from delta.tables import DeltaTable

def _ensure_physical_table_exists(tablename:str, tableschema:StructType, partitionBy:List[str], do_schema_check:bool=True):
  physical_table_dbws_mount_path = _get_physical_table_dbws_mount_path(tablename)

  try: 
    dbutils.fs.ls(physical_table_dbws_mount_path)
    physical_table_dbws_mount_path_exists = True
  except:
    physical_table_dbws_mount_path_exists = False

  if physical_table_dbws_mount_path_exists:
    existing_delta_table = DeltaTable.forPath(spark, physical_table_dbws_mount_path)
    existing_spark_df = existing_delta_table.toDF()
    existing_schema = existing_spark_df.schema
    def _equal_park_df_schema( ava, eve, ignore_nullable:bool=True):
      if ignore_nullable:
        return [(f.name,f.dataType) for f in ava] == [(f.name,f.dataType) for f in eve]
      else:
        return [(f.name,f.dataType,f.nullable) for f in ava] == [(f.name,f.dataType,f.nullable) for f in eve]
      
    # ignore column nullable
    if do_schema_check and not _equal_park_df_schema( tableschema, existing_spark_df.schema ):
      raise RuntimeError(f""" 
      physical DeltaTable '{tablename}' exists at '{physical_table_dbws_mount_path}' but schema does not match expectation,
      existing schema: {existing_schema}
      expected schema: {tableschema}
      """)
  else:
    spark.createDataFrame(spark.sparkContext.emptyRDD(), schema=tableschema) \
         .write.format('delta') \
         .partitionBy( partitionBy ) \
         .save( physical_table_dbws_mount_path )
  return DeltaTable.forPath(spark, physical_table_dbws_mount_path)

def _ensure_meta_table_exists( tablename:str, partitionBy:List[str] ):
  meta_database = get_cfg('infra.dbws.hive_database')
  physical_table_dbws_mount_path = _get_physical_table_dbws_mount_path(tablename)

  spark.sql(f"""
    CREATE EXTERNAL TABLE IF NOT EXISTS {meta_database}.{tablename}
    USING DELTA
    LOCATION '{physical_table_dbws_mount_path}'
  """)
  
  # TODO: partitionBy columns are ordered, should check order as well
  existing_partitionBy_cols = { c.name for c in spark.catalog.listColumns(tablename) if c.isPartition }
  if existing_partitionBy_cols != set( partitionBy ):
    raise RuntimeError(f""" table '{tablename}' already exists with different partitionBy columns, 
    existing partitionBy columns: {existing_partitionBy_cols}
       input partitionBy columns: {set(partitionBy)}
    """)
  return

def ensure_lake_table_exists( tablename:str,tableschema:StructType, partitionBy:Union[str,List[str]], do_schema_check:bool=True ):
  f"""
  tablename: must start with {1+2}
  """
  if type(partitionBy) is str:
    partitionBy = [ partitionBy ]
  
  _assert_tablename(tablename)
  _ensure_physical_table_exists(tablename,tableschema,partitionBy,do_schema_check=do_schema_check)
  _ensure_meta_table_exists(tablename,partitionBy)
  return

# COMMAND ----------

# DBTITLE 1,define: destroy_lake_table()
def _destroy_physical_table(tablename:str):
  physical_table_dbws_mount_path = _get_physical_table_dbws_mount_path(tablename)
  remove_dbfs_dir( physical_table_dbws_mount_path )
  return

def _destroy_meta_table(tablename:str):
  spark.sql(f'DROP TABLE IF EXISTS {tablename}')
  return

def destroy_lake_table(tablename:str):
  _destroy_physical_table(tablename)
  _destroy_meta_table(tablename)
  return

# COMMAND ----------

# DBTITLE 1,define: exists_lake_table( tablename:str )
def exists_lake_table( tablename:str, meta_database:str=None ):
  if meta_database is None:
    meta_database = get_cfg('infra.dbws.hive_database')
  return spark._jsparkSession.catalog().tableExists(meta_database, tablename)

# COMMAND ----------

# DBTITLE 1,define: get_lake_table_spark_write_path()
def get_lake_table_spark_write_path(lake_tablename:str):
  _assert_tablename(lake_tablename)
  return _get_physical_table_dbws_mount_path(lake_tablename)

# COMMAND ----------

# DBTITLE 1,define: get_delta_table( )
def get_delta_table(lake_tablename:str):
  _assert_tablename(lake_tablename)
  from delta.tables import DeltaTable
  physical_table_dbws_mount_path = _get_physical_table_dbws_mount_path(lake_tablename)
  delta_table = DeltaTable.forPath(spark, physical_table_dbws_mount_path)
  return delta_table

# COMMAND ----------

# DBTITLE 1,define: load_spark_dataframe_from_azure_blob()
def load_spark_dataframe_from_azure_blob( storage_account_name:str, container_name:str, container_sas:str,delta_table_container_path:str ):
  spark.conf.set(
    f'fs.azure.sas.{container_name}.{storage_account_name}.blob.core.windows.net',
    container_sas )
  return spark.read.format('delta').load(f'wasbs://{container_name}@{storage_account_name}.blob.core.windows.net/{delta_table_container_path.lstrip("/")}')

# COMMAND ----------

# DBTITLE 1,define: assert_spark_primay_key
def assert_spark_primary_key( spark_query:str, pk_cols:List[str] ):
  """ assert that the pk_cols of Spark Query are actually the Primary Key
      Spark Query could be cu_sometable / (SELECT * FROM cu_sometable) / SELECT * FROM cu_sometable
  """
  assert( len( spark.sql(f""" 
    SELECT {','.join(pk_cols)}, COUNT(*) AS rc
      FROM ( {spark_query} )
     GROUP BY {','.join(pk_cols)}
     HAVING rc > 1
     LIMIT 1
    """).collect() ) < 1 )

# COMMAND ----------

# DBTITLE 1,define: compare_spark_df
def compare_spark_df( ava, eve, pk_cols:List[str], 
                      ignore_cols:List[str] = None, ava_alias:str='Ava', eve_alias:str='Eve' ):
  """
  Compare two Spark DataFrames 'ava' and 'eve'. 
  Need to define Primary Key Columns with pk_cols,
  Can explicitly define columns to ignore (i.e. exclude from comparison) with ignore_cols
  
  Return a Spark DataFrame [ diff_type, {pk_cols} ], 
    diff_type could be 'missing in ava' / 'missing in eve' / 'diff' / 'same'
  """
  from uuid import uuid4
  
  if ignore_cols is None:
    ignore_cols = []
    
  ava_schema = sorted( [ f'{c.name} {c.dataType}' for c in ava.schema ] )
  eve_schema = sorted( [ f'{c.name} {c.dataType}' for c in eve.schema ] )
  if ava_schema != eve_schema:
    raise RuntimeError( f""" 
    cannot compare Spark DataFrames with different schemas: 
    {ava_alias} schema = {ava_schema} 
    {eve_alias} schema = {eve_schema} """ )

  def _is_pk_cols( df_, pk_cols_ ):
    from pyspark.sql.functions import count
    return df_.groupBy( pk_cols_ ) \
              .agg( count(pk_cols_[0]).alias('rc') ) \
              .where('rc > 1').limit(1).count() == 0

  logger.debug(f'validating pk_cols Primary Columns ...')
  if not _is_pk_cols(ava, pk_cols):
    raise RuntimeError( f"""
    columns {pk_cols} contains duplicates in {ava_alias}
    """ )
  if not _is_pk_cols(eve, pk_cols):
    raise RuntimeError( f"""
    columns {pk_cols} contains duplicates in {eve_alias}
    """ )

  ava_viewname = f'v_ava_{str(uuid4())[:6]}'
  eve_viewname = f'v_eve_{str(uuid4())[:6]}'

  ava.createOrReplaceTempView(ava_viewname)
  eve.createOrReplaceTempView(eve_viewname)

  comp_cols = [c.name for c in ava.schema if c.name not in ignore_cols and c.name not in pk_cols]
  if len(comp_cols) < 1:
    raise RuntimeError( f"""
    no columns to compare
    """ )

  diff_sql = f"""
    SELECT {','.join( ["COALESCE(ava."+c+","+"eve."+c+") AS "+c for c in pk_cols])},
               CASE WHEN ava.{pk_cols[0]} IS NULL THEN 'missing in {ava_alias}'
                    WHEN eve.{pk_cols[0]} IS NULL THEN 'missing in {eve_alias}'
                    WHEN NOT ({" AND ".join(['ava.'+c+'<=>'+'eve.'+c for c in comp_cols])}) THEN 'diff'
                    ELSE 'same'
                END AS diff_type
          FROM {ava_viewname} ava
          FULL JOIN {eve_viewname} eve
            ON ({' AND '.join( [ "ava."+c+"<=>"+"eve."+c for c in pk_cols] )})   
  """  
  diff_df = spark.sql(diff_sql)  

  spark.sql(f'DROP VIEW IF EXISTS {ava_viewname}')
  spark.sql(f'DROP VIEW IF EXISTS {eve_viewname}')
  return diff_df
