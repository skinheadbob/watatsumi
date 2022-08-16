# Databricks notebook source
# DBTITLE 1,define ABC class: LakeIngester
from abc import ABC, abstractmethod
from typing import Dict,List
import pandas as pd
from pyspark.sql.types import StructType
from pyspark.sql import DataFrame
from tqdm import tqdm

class LakeIngester(ABC):
  def __init__(self, lake_tablename:str, 
               slice_id_lake_expr:str, slice_id_lake_type:str, slice_id_pd_type:str,
               slice_watermark_lake_expr:str, slice_watermark_lake_type:str, slice_watermark_pd_type:str,
               lake_partitionBy:List[str]=None, 
               lake_reorder_put_first_columns:List[str]=None,
               lake_do_schema_check:bool=True,
               do_compare_slice_row_cnt:bool=False,
               do_optimize_per_slice:bool=False,
               do_drop_slice_if_not_in_source:bool=False):
    if do_optimize_per_slice and (lake_partitionBy is None or len(lake_partitionBy)<1):
      raise RuntimeError(f' when "do_optimize_per_slice" is set to True, need to specify the "slice_id" column in "lake_partitionBy" ')
    
    self.lake_tablename = lake_tablename
    self.lake_partitionBy = [] if lake_partitionBy is None else lake_partitionBy
    self.lake_reorder_put_first_columns = [] if lake_reorder_put_first_columns is None else lake_reorder_put_first_columns
    self.lake_do_schema_check = lake_do_schema_check
    self.do_compare_slice_row_cnt = do_compare_slice_row_cnt
    self.do_optimize_per_slice = do_optimize_per_slice
    self.do_drop_slice_if_not_in_source = do_drop_slice_if_not_in_source
    self.slice_id_lake_expr = slice_id_lake_expr
    self.slice_id_lake_type = slice_id_lake_type
    self.slice_id_pd_type = slice_id_pd_type
    self.slice_watermark_lake_expr = slice_watermark_lake_expr
    self.slice_watermark_lake_type = slice_watermark_lake_type
    self.slice_watermark_pd_type = slice_watermark_pd_type
  
  @abstractmethod
  def get_source_spark_schema(self) -> StructType:
    pass
  
  @abstractmethod
  def query_source_slice_meta(self) -> pd.DataFrame:
    """ return Pandas.DF { slice_id, watermark, [row_cnt] } """
    pass
  
  @abstractmethod
  def query_source_slice(self,slice_id) -> DataFrame:
    pass
  
  @property
  def source_spark_schema(self) -> StructType:
    return self.get_source_spark_schema()
  
  @property
  def lake_spark_shcema(self) -> StructType:
    return self.get_lake_spark_schema()
  
  @property
  def slice_meta_pd_type(self) -> Dict[str,str]:
    return {
      'slice_id':self.slice_id_pd_type,
      'watermark':self.slice_watermark_pd_type
    }
    
  def get_lake_spark_schema(self) -> StructType:
    reorder_casefold_colnames = [ col.casefold() for col in self.lake_reorder_put_first_columns ]
    
    source_spark_casefold_colname_col_dict = {
      f.name.casefold():f
      for f in self.source_spark_schema
    }
    
    return StructType(
        [ source_spark_casefold_colname_col_dict[r] for r in reorder_casefold_colnames if r in source_spark_casefold_colname_col_dict ]
      + [f for f in self.source_spark_schema if f.name.casefold() not in reorder_casefold_colnames]
    )

  def setup_lake_table(self) -> None:
    return ensure_lake_table_exists( self.lake_tablename,self.lake_spark_shcema, self.lake_partitionBy, do_schema_check=self.lake_do_schema_check )
  
  def query_lake_slice_meta(self) -> pd.DataFrame:
    """ return Pandas.DF { slice_id, watermark, [row_cnt] } """
    query_lake_meta_sql = f"""
    WITH meta AS (
      SELECT CAST({self.slice_id_lake_expr} AS {self.slice_id_lake_type}) AS slice_id,
             {self.slice_watermark_lake_expr} AS watermark
             { ', COUNT(1) AS row_cnt' if self.do_compare_slice_row_cnt else '' }
        FROM {self.lake_tablename}
       GROUP BY slice_id )
    SELECT slice_id, CAST(watermark AS {self.slice_watermark_lake_type}) AS watermark
           {', row_cnt' if self.do_compare_slice_row_cnt else ''}
      FROM meta
    """
    logger.debug( f'query Lake meta with SQL:\n{query_lake_meta_sql}' )
    return spark.sql(query_lake_meta_sql) \
                .toPandas() \
                .astype( self.slice_meta_pd_type ) \
                .sort_values(by='slice_id',ascending=False) \
                .reset_index(drop=True)
  
  def calc_slice_meta(self) -> pd.DataFrame:
    logger.debug(f'query source_slice_meta...')
    source_slice_meta = self.query_source_slice_meta()
    logger.debug(f'query lake_slice_meta...')
    lake_slice_meta = self.query_lake_slice_meta()
    source_slice_meta['in_source'] = True
    lake_slice_meta['in_lake'] = True

    slice_meta = source_slice_meta.merge( lake_slice_meta, 
                                       on=['slice_id'], how='outer', suffixes=('_source','_lake') ) \
                                .fillna( {'in_lake':False, 'in_source':False} )
    slice_meta['only_in_source'] = ( slice_meta['in_source'] )  & ( ~slice_meta['in_lake'] )
    slice_meta['only_in_lake']   = ( ~slice_meta['in_source'] ) & ( slice_meta['in_lake'] )
    slice_meta['to_upsert_bcz_existence'] = slice_meta['only_in_source'] | ( slice_meta['only_in_lake'] & self.do_drop_slice_if_not_in_source )

    slice_meta['is_source_watermark_lte_lake'] = ( slice_meta['in_source'] )  & ( slice_meta['in_lake'] ) & ( slice_meta['watermark_source'] <= slice_meta['watermark_lake'] )
    slice_meta['to_upsert_bcz_watermark'] = slice_meta['in_source'] & slice_meta['in_lake'] \
                                            & (  slice_meta['watermark_source'].isna() 
                                               | slice_meta['watermark_lake'].isna()
                                               | (slice_meta['watermark_source'] > slice_meta['watermark_lake']) )

    slice_meta['to_upsert_bcz_row_cnt'] = ~( slice_meta['row_cnt_source'] == slice_meta['row_cnt_lake'] ) if self.do_compare_slice_row_cnt else False

    slice_meta['to_upsert'] =   slice_meta['to_upsert_bcz_existence'] \
                              | slice_meta['to_upsert_bcz_watermark'] \
                              | slice_meta['to_upsert_bcz_row_cnt']

    slice_meta_cols = ['slice_id','to_upsert', 
                       'to_upsert_bcz_existence', 'to_upsert_bcz_watermark', 'to_upsert_bcz_row_cnt',
                       'watermark_source','watermark_lake'] + ( ['row_cnt_source','row_cnt_lake'] if self.do_compare_slice_row_cnt else [] )
    slice_meta=slice_meta[ slice_meta_cols ].sort_values(by=['to_upsert','slice_id'],ascending=[False,False]).reset_index(drop=True)
    return slice_meta
  
  def pick_upsert_slice_id_list(self) -> List:
    slice_meta = self.calc_slice_meta()
    logger.debug(f""" Source to Lake slice_meta: 
    { slice_meta.to_string(max_rows=20) } """)

    return sorted( slice_meta[ slice_meta['to_upsert'] ]['slice_id'].tolist(), reverse=True )
  
  def upsert_slice(self, slice_id):
    slice_cond = f"CAST({self.slice_id_lake_expr} AS {self.slice_id_lake_type}) = CAST('{str(slice_id)}' AS {self.slice_id_lake_type})"
    source_slice = self.query_source_slice(slice_id)
    spark_save_path = get_lake_table_spark_write_path(self.lake_tablename)
    return source_slice.write.format('delta') \
             .mode('overwrite') \
             .option('replaceWhere', slice_cond) \
             .option('mergeSchema', str(not self.lake_do_schema_check) ) \
             .save( spark_save_path )
  
  def optimize_slice(self, slice_id):
    slice_cond = f"CAST({self.slice_id_lake_expr} AS {self.slice_id_lake_type}) = CAST('{str(slice_id)}' AS {self.slice_id_lake_type})"
    return spark.sql(f"""
      OPTIMIZE {self.lake_tablename}
         WHERE {slice_cond}
    """)
  
  def ingest(self, limit:int=0) -> DataFrame:
    from datetime import datetime
    self.setup_lake_table()
    
    slice_id_list = self.pick_upsert_slice_id_list()
    if limit>0 and limit < len(slice_id_list):
      logger.info(f'ingesting {limit} out of total {len(slice_id_list)} ingestable slice_ids')
      slice_id_list = slice_id_list[:limit]
    
    for slice_id in tqdm(slice_id_list):
      logger.info(f'upserting slice "{slice_id}" ...')
      start_ts = datetime.now()
      self.upsert_slice(slice_id)
      end_ts = datetime.now()
      logger.info(f'... finished upserting slice "{slice_id}" in {str(end_ts-start_ts)}')
      
      if self.do_optimize_per_slice:
        logger.info(f'optimizing slice "{slice_id}" ...')
        start_ts = datetime.now()
        self.optimize_slice(slice_id)
        end_ts = datetime.now()
        logger.info(f'... finished optimizing slice "{slice_id}" in {str(end_ts-start_ts)}')
          
    return spark.sql(f'SELECT * FROM {self.lake_tablename}')

# COMMAND ----------

# DBTITLE 1,define ABC class: JdbcLakeIngester
class JdbcLakeIngester(LakeIngester, ABC):
  def __init__(self, 
               source_tablename:str,
               slice_id_source_expr:str, slice_id_source_type:str, slice_id_pd_type:str,
               slice_watermark_source_expr:str,  slice_watermark_source_type:str, slice_watermark_pd_type:str,
               lake_tablename:str, 
               
               slice_id_lake_expr:str=None, slice_id_lake_type:str=None,
               slice_watermark_lake_expr:str=None, slice_watermark_lake_type:str=None,
               
               lake_partitionBy:List[str]=None, 
               lake_reorder_put_first_columns:List[str]=None,
               lake_do_schema_check:bool=True,
               do_compare_slice_row_cnt:bool=False,
               do_optimize_per_slice:bool=False,
               do_drop_slice_if_not_in_source:bool=False):
    if slice_id_lake_expr is None:
      slice_id_lake_expr = slice_id_source_expr
    if slice_id_lake_type is None:
      slice_id_lake_type = slice_id_source_type
    if slice_watermark_lake_expr is None:
      slice_watermark_lake_expr = slice_watermark_source_expr
    if slice_watermark_lake_type is None:
      slice_watermark_lake_type = slice_watermark_source_type
    self.source_tablename = source_tablename
    self.slice_id_source_expr = slice_id_source_expr
    self.slice_id_source_type = slice_id_source_type
    self.slice_watermark_source_expr = slice_watermark_source_expr
    self.slice_watermark_source_type = slice_watermark_source_type
    
    super().__init__(lake_tablename,
                   slice_id_lake_expr,slice_id_lake_type,slice_id_pd_type,
                   slice_watermark_lake_expr,slice_watermark_lake_type,slice_watermark_pd_type,
                   lake_partitionBy=lake_partitionBy,
                   lake_reorder_put_first_columns=lake_reorder_put_first_columns,
                   lake_do_schema_check=lake_do_schema_check,
                   do_compare_slice_row_cnt=do_compare_slice_row_cnt,
                   do_optimize_per_slice=do_optimize_per_slice,
                   do_drop_slice_if_not_in_source=do_drop_slice_if_not_in_source)
  
  @abstractmethod
  def get_source_jdbc_reader(self):
    pass
  
  @abstractmethod
  def query_source_df(self,query_sql:str) -> DataFrame:
    pass
  
  def compose_query_source_meta_sql(self, do_include_row_cnt:bool) -> str:
    return f"""
    WITH meta AS (
      SELECT CAST({self.slice_id_source_expr} AS {self.slice_id_source_type}) AS slice_id,
             {self.slice_watermark_source_expr} AS watermark
             { ', COUNT(1) AS row_cnt' if do_include_row_cnt else '' }
        FROM {self.source_tablename}
       GROUP BY slice_id )
    SELECT slice_id, CAST(watermark AS {self.slice_watermark_source_type}) AS watermark
           {', row_cnt' if do_include_row_cnt else ''}
      FROM meta 
    """
  
  def compose_query_source_slice_sql(self, slice_id) -> str:
    return f"""
    SELECT * FROM {self.source_tablename} 
     WHERE CAST({self.slice_id_source_expr} AS {self.slice_id_source_type}) = CAST('{str(slice_id)}' AS {self.slice_id_source_type}) 
    """
  
  def get_source_spark_schema(self):
    return self.get_source_jdbc_reader() \
            .option('query',f'SELECT * FROM {self.source_tablename}') \
            .load().schema
  
  def query_source_slice_meta(self) -> pd.DataFrame:
    """ return Pandas.DF { slice_id, watermark, [row_cnt] } """
    query_source_meta_sql = self.compose_query_source_meta_sql(self.do_compare_slice_row_cnt)
    logger.debug( f'query Source meta with SQL:\n{query_source_meta_sql}' )
    return self.get_source_jdbc_reader() \
            .option('query',query_source_meta_sql) \
            .load().toPandas() \
            .astype( self.slice_meta_pd_type ) \
            .sort_values(by='slice_id',ascending=False) \
            .reset_index(drop=True)
  
  def query_source_slice(self,slice_id) -> DataFrame:
    """ return Spark.DF """
    slice_query_sql = self.compose_query_source_slice_sql(slice_id)
    return self.query_source_df(slice_query_sql)

# COMMAND ----------

# DBTITLE 1,define concrete class: PostgreSqlLakeIngester
class PostgreSqlLakeIngester(JdbcLakeIngester):
  def __init__(self, 
               pg_host:str, pg_port:str, pg_db:str,
               pg_uname:str, pg_pwd:str,
               source_tablename:str,
               slice_id_source_expr:str, slice_id_source_type:str, slice_id_pd_type:str,
               slice_watermark_source_expr:str,  slice_watermark_source_type:str, slice_watermark_pd_type:str,
               lake_tablename:str, 
               
               slice_id_lake_expr:str=None, slice_id_lake_type:str=None,
               slice_watermark_lake_expr:str=None, slice_watermark_lake_type:str=None,
               
               lake_partitionBy:List[str]=None, 
               lake_reorder_put_first_columns:List[str]=None,
               lake_do_schema_check:bool=True,
               do_compare_slice_row_cnt:bool=False,
               do_optimize_per_slice:bool=False,
               do_drop_slice_if_not_in_source:bool=False):
    self.pg_host, self.pg_port, self.pg_db = pg_host, pg_port, pg_db
    self.pg_uname, self.pg_pwd = pg_uname, pg_pwd
    super().__init__(source_tablename,
                  slice_id_source_expr,slice_id_source_type,slice_id_pd_type,
                  slice_watermark_source_expr,slice_watermark_source_type,slice_watermark_pd_type,
                  lake_tablename,
                  slice_id_lake_expr=slice_id_lake_expr,slice_id_lake_type=slice_id_lake_type,
                  slice_watermark_lake_expr=slice_watermark_lake_expr,slice_watermark_lake_type=slice_watermark_lake_type,
                  lake_partitionBy=lake_partitionBy,
                  lake_reorder_put_first_columns=lake_reorder_put_first_columns,
                  lake_do_schema_check=lake_do_schema_check,
                  do_compare_slice_row_cnt=do_compare_slice_row_cnt,
                  do_optimize_per_slice=do_optimize_per_slice,
                  do_drop_slice_if_not_in_source=do_drop_slice_if_not_in_source)
  
  def get_source_jdbc_reader(self):
    jdbc_url = f'jdbc:postgresql://{self.pg_host}:{self.pg_port}/{self.pg_db}'
    return spark.read.format('jdbc') \
              .option('url', jdbc_url) \
              .option('user', self.pg_uname) \
              .option('password', self.pg_pwd )  
  
  def compose_query_source_meta_sql(self,do_include_row_cnt:bool) -> str: 
    return f"""
    WITH meta AS (
      SELECT CAST({self.slice_id_source_expr} AS {self.slice_id_source_type}) AS slice_id,
             {self.slice_watermark_source_expr} AS watermark
             { ', COUNT(1) AS row_cnt' if do_include_row_cnt else '' }
        FROM {self.source_tablename}
       GROUP BY slice_id )
    SELECT slice_id, CAST(watermark AS {self.slice_watermark_source_type}) AS watermark
           {', row_cnt' if do_include_row_cnt else ''}
      FROM meta 
    """
  
  @property
  def jdbc_fetchsize(self) -> int:
    # totally arbitrary
    return 100_000 
  
  def query_source_df(self,query_sql:str) -> DataFrame:
    return self.get_source_jdbc_reader() \
               .option('query', query_sql) \
               .option('fetchsize', self.jdbc_fetchsize) \
               .load()

# COMMAND ----------

# DBTITLE 1,define concrete class: SqlServerLakeIngester
class SqlServerLakeIngester(JdbcLakeIngester):
  def __init__(self, 
               ms_host:str, ms_port:str, ms_db:str,
               ms_uname:str, ms_pwd:str,
               source_tablename:str,
               slice_id_source_expr:str, slice_id_source_type:str, slice_id_pd_type:str,
               slice_watermark_source_expr:str,  slice_watermark_source_type:str, slice_watermark_pd_type:str,
               lake_tablename:str, 
               
               slice_id_lake_expr:str=None, slice_id_lake_type:str=None,
               slice_watermark_lake_expr:str=None, slice_watermark_lake_type:str=None,
               
               lake_partitionBy:List[str]=None, 
               lake_reorder_put_first_columns:List[str]=None,
               lake_do_schema_check:bool=True,
               do_compare_slice_row_cnt:bool=False,
               do_optimize_per_slice:bool=False,
               do_drop_slice_if_not_in_source:bool=False):
    self.ms_host, self.ms_port, self.ms_db = ms_host, ms_port, ms_db
    self.ms_uname, self.ms_pwd = ms_uname, ms_pwd
    super().__init__(source_tablename,
                  slice_id_source_expr,slice_id_source_type,slice_id_pd_type,
                  slice_watermark_source_expr,slice_watermark_source_type,slice_watermark_pd_type,
                  lake_tablename,
                  slice_id_lake_expr=slice_id_lake_expr,slice_id_lake_type=slice_id_lake_type,
                  slice_watermark_lake_expr=slice_watermark_lake_expr,slice_watermark_lake_type=slice_watermark_lake_type,
                  lake_partitionBy=lake_partitionBy,
                  lake_reorder_put_first_columns=lake_reorder_put_first_columns,
                  lake_do_schema_check=lake_do_schema_check,
                  do_compare_slice_row_cnt=do_compare_slice_row_cnt,
                  do_optimize_per_slice=do_optimize_per_slice,
                  do_drop_slice_if_not_in_source=do_drop_slice_if_not_in_source)
  
  def get_source_jdbc_reader(self):
    jdbc_url = f'jdbc:sqlserver://{self.ms_host}:{self.ms_port};databaseName={self.ms_db}'
    return spark.read.format('jdbc') \
              .option('url', jdbc_url) \
              .option('user', self.ms_uname) \
              .option('password', self.ms_pwd )  
  
  def compose_query_source_meta_sql(self,do_include_row_cnt:bool) -> str: 
    return f"""
    SELECT slice_id, CAST(watermark AS {self.slice_watermark_source_type}) AS watermark
           {', row_cnt' if do_include_row_cnt else ''}
      FROM (  SELECT CAST({self.slice_id_source_expr} AS {self.slice_id_source_type}) AS slice_id,
                     {self.slice_watermark_source_expr} AS watermark
                     { ', COUNT(1) AS row_cnt' if do_include_row_cnt else '' }
                FROM {self.source_tablename}
               GROUP BY CAST({self.slice_id_source_expr} AS {self.slice_id_source_type})  ) meta 
    """
  
  @property
  def jdbc_fetchsize(self) -> int:
    # totally arbitrary
    return 100_000 
  
  def query_source_df(self,query_sql:str) -> DataFrame:
    return self.get_source_jdbc_reader() \
               .option('query', query_sql) \
               .option('fetchsize', self.jdbc_fetchsize) \
               .load()

# COMMAND ----------

# DBTITLE 1,define concrete class: OracleLakeIngester
class OracleLakeIngester(JdbcLakeIngester):
  """ NOTE: require Oracle JDBC Driver, Maven coordinate= com.oracle.ojdbc:ojdbc8:19.3.0.0
  """
  def __init__(self, 
               orc_host:str, orc_port:str, orc_sid:str,
               orc_uname:str, orc_pwd:str,
               source_tablename:str,
               slice_id_source_expr:str, slice_id_source_type:str, slice_id_pd_type:str,
               slice_watermark_source_expr:str,  slice_watermark_source_type:str, slice_watermark_pd_type:str,
               lake_tablename:str, 
               
               slice_id_lake_expr:str=None, slice_id_lake_type:str=None,
               slice_watermark_lake_expr:str=None, slice_watermark_lake_type:str=None,
               
               lake_partitionBy:List[str]=None, 
               lake_reorder_put_first_columns:List[str]=None,
               lake_do_schema_check:bool=True,
               do_compare_slice_row_cnt:bool=False,
               do_optimize_per_slice:bool=False,
               do_drop_slice_if_not_in_source:bool=False):
    self.orc_host, self.orc_port, self.orc_sid = orc_host, orc_port, orc_sid
    self.orc_uname, self.orc_pwd = orc_uname, orc_pwd
    super().__init__(source_tablename,
                  slice_id_source_expr,slice_id_source_type,slice_id_pd_type,
                  slice_watermark_source_expr,slice_watermark_source_type,slice_watermark_pd_type,
                  lake_tablename,
                  slice_id_lake_expr=slice_id_lake_expr,slice_id_lake_type=slice_id_lake_type,
                  slice_watermark_lake_expr=slice_watermark_lake_expr,slice_watermark_lake_type=slice_watermark_lake_type,
                  lake_partitionBy=lake_partitionBy,
                  lake_reorder_put_first_columns=lake_reorder_put_first_columns,
                  lake_do_schema_check=lake_do_schema_check,
                  do_compare_slice_row_cnt=do_compare_slice_row_cnt,
                  do_optimize_per_slice=do_optimize_per_slice,
                  do_drop_slice_if_not_in_source=do_drop_slice_if_not_in_source)
  
  def get_source_jdbc_reader(self):
    jdbc_url = f"jdbc:oracle:thin:@{self.orc_host}:{self.orc_port}/{self.orc_sid}"
    return spark.read.format('jdbc') \
              .option('driver', 'oracle.jdbc.driver.OracleDriver' ) \
              .option('url', jdbc_url) \
              .option('user', self.orc_uname) \
              .option('password', self.orc_pwd )  
  
  def compose_query_source_meta_sql(self,do_include_row_cnt:bool) -> str: 
    return f"""
    WITH meta AS (
      SELECT CAST({self.slice_id_source_expr} AS {self.slice_id_source_type}) AS slice_id,
             {self.slice_watermark_source_expr} AS watermark
             { ', COUNT(1) AS row_cnt' if do_include_row_cnt else '' }
        FROM {self.source_tablename}
       GROUP BY CAST({self.slice_id_source_expr} AS {self.slice_id_source_type}) )
    SELECT slice_id, CAST(watermark AS {self.slice_watermark_source_type}) AS watermark
           {', row_cnt' if do_include_row_cnt else ''}
      FROM meta 
    """
  
  @property
  def jdbc_fetchsize(self) -> int:
    # totally arbitrary
    return 100_000 
  
  def query_source_df(self,query_sql:str) -> DataFrame:
    return self.get_source_jdbc_reader() \
               .option('query', query_sql) \
               .option('fetchsize', self.jdbc_fetchsize) \
               .load()

# COMMAND ----------

# DBTITLE 1,define ABC class: LandingLakeIngester
class LandingLakeIngester(LakeIngester):
  """ Landing Zone blobs are stored in this hierarchy: 
        /landing_zone/{source_name}/{slice_id}/{watermark_999}
        /landing_zone/{source_name}/{slice_id}/{watermark_888}
        /landing_zone/{source_name}/{slice_id}/{watermark_777}
      For each slice_id, only the largest watermark will be parsed into Spark.DF
      
      Note that additional two columns will be prepended to the parsed Spark.DF
        _slice_id:STRING, _watermark:STRING
  """
  def __init__(self, lake_tablename:str, 
               additional_lake_partition_by:List[str] = None,
               **kwargs):
    slice_id_lake_expr, slice_id_lake_type, slice_id_pd_type = '_slice_id', 'STRING', 'str'
    slice_watermark_lake_expr, slice_watermark_lake_type, slice_watermark_pd_type = 'MAX(_watermark)', 'STRING', 'str'
    lake_partitionBy = ['_slice_id','_watermark'] + ([] if additional_lake_partition_by is None else additional_lake_partition_by)
    super().__init__( lake_tablename, 
                    slice_id_lake_expr, slice_id_lake_type, slice_id_pd_type, 
                    slice_watermark_lake_expr, slice_watermark_lake_type, slice_watermark_pd_type,
                    lake_partitionBy=lake_partitionBy,
                    **kwargs)
  
  @abc.abstractproperty
  def source_name(self) -> str:
    """ /landing_zone/{source_name}/{slice_id}/{watermark} """
    pass
  
  @abc.abstractproperty
  def landing_schema(self) -> StructType:
    pass
  
  @abc.abstractmethod
  def parse_landing_df(self, dbfs_mnt_dir:str) -> DataFrame:
    """ Given a DBFS dir (the highest watermark of a slice_id), parse into SparkDF
    """
    pass
  
  def get_source_spark_schema(self) -> StructType:
    # prepend _slice_id:STRING, _watermark:STRING
    from pyspark.sql.types import StructField, StringType
    return StructType( [
      StructField( '_slice_id', StringType(), False ),
      StructField( '_watermark', StringType(), False ),
    ] + [ c for c in self.landing_schema] )
    
  def get_highest_landing_watermark(self, slice_id:str):
    return max( [ f.name.rstrip('/') 
                   for f in dbutils.fs.ls(f"{get_cfg('dbws.mount_point')}/landing_zone/{self.source_name}/{slice_id}/") ] )
  
  def query_source_slice_meta(self) -> pd.DataFrame:
    """ return Pandas.DF { slice_id, watermark, [row_cnt] } """
    # TODO
    def list_slice_id():
      return sorted( [ f.name.rstrip('/') 
                      for f in dbutils.fs.ls(f"{get_cfg('dbws.mount_point')}/landing_zone/{self.source_name}/") ], reverse=True )

    col_slice_id = list_slice_id()

    with concurrent.futures.ThreadPoolExecutor(max_workers=multiprocessing.cpu_count()) as executor:
      futures = list(map(lambda slice_id: executor.submit(self.get_highest_landing_watermark, slice_id),
                         col_slice_id))
    col_watermark = list(map(lambda f: f.result(), futures))

    meta = pd.DataFrame({'slice_id': col_slice_id, 'watermark': col_watermark})    
    return meta
  
  def query_source_slice(self,slice_id) -> DataFrame:
    """ prepend '_slice_id','_watermark' to Landing DF
    """
    watermark = self.get_highest_landing_watermark(slice_id)
    dbfs_mnt_dir = f"{get_cfg('dbws.mount_point')}/landing_zone/{self.source_name}/{slice_id}/{watermark}/"
    
    landing_df = self.parse_landing_df(dbfs_mnt_dir)
    if self.landing_schema != landing_df.schema:
      raise RuntimeError(f'"parse_landing_df" returned unexpected schema')
    
    from pyspark.sql.functions import lit
    source_slice = landing_df \
                    .withColumn('_slice_id',lit(slice_id)) \
                    .withColumn('_watermark',lit(watermark)) \
                    .select( '_slice_id', '_watermark', *landing_df.columns )
    return source_slice

# COMMAND ----------

# DBTITLE 1,define concrete class: BigQueryEventsLakeIngester
class BigQueryEventsLakeIngester( LakeIngester ):
  """ BigQuery generatesa new Events table daily
      slice_id = _table_id:str, watermark = _table_last_modified_utc:str
      column [_table_id, _table_last_modified_utc] will be prepended to GA Events schema
  """
  def __init__(self,
               bq_dataset_id:str, bq_cred_json:str,
               lake_tablename:str,
               additional_lake_partition_by:List[str] = None,
               **kwargs):
    self.bq_dataset_id, self.bq_cred_json = bq_dataset_id, bq_cred_json
    slice_id_lake_expr, slice_id_lake_type, slice_id_pd_type = \
      '_table_id', 'STRING', 'str'
    slice_watermark_lake_expr,slice_watermark_lake_type,slice_watermark_pd_type = \
      'MAX(_table_last_modified_utc)', 'STRING', 'str'
    lake_partitionBy=['_table_id','_table_last_modified_utc'] + ([] if additional_lake_partition_by is None else additional_lake_partition_by)
    super().__init__( lake_tablename,
                      slice_id_lake_expr, slice_id_lake_type, slice_id_pd_type,
                      slice_watermark_lake_expr,slice_watermark_lake_type,slice_watermark_pd_type,
                      lake_partitionBy = lake_partitionBy,
                      **kwargs )
    if self.do_compare_slice_row_cnt:
      raise RuntimeError(f' "do_compare_slice_row_cnt" is not supported for BigQueryEventsLakeIngester yet, cannot be set to True ')
    
  @property
  def bq_cred(self):
    from google.oauth2 import service_account
    import json
    return service_account.Credentials.from_service_account_info( json.loads(self.bq_cred_json) )
  
  @property
  def bq_client(self):
    from google.cloud import bigquery
    return bigquery.Client(credentials=self.bq_cred, project=self.bq_cred.project_id)
    
  def get_source_spark_schema(self) -> StructType:
    sample_bq_table_id = next( self.bq_client.list_tables(self.bq_dataset_id) ).table_id
    ga_schema = self.get_spark_reader() \
       .option('table', sample_bq_table_id) \
       .load().schema
    from pyspark.sql.types import StructType, StructField, StringType
    return StructType(
      [StructField('_table_id',StringType(),True),
       StructField('_table_last_modified_utc',StringType(),True),] + [c for c in ga_schema]
    )
    
  def get_table_last_modified_utc(self, table_id:str) -> str:
    bq_table = self.bq_client.get_table(f'{self.bq_cred.project_id}.{self.bq_dataset_id}.{table_id}')
    return bq_table.modified.astimezone(pytz.utc).strftime('%Y%m%d_%H%M%S')
  
  def query_source_slice_meta(self) -> pd.DataFrame:
    """ return Pandas.DF { slice_id, watermark, [row_cnt] } """
    
    table_ids = [str(t.table_id) for t in self.bq_client.list_tables(self.bq_dataset_id)] 
    
    def lookup_slice_meta_row(table_id:str):
      return {'slice_id': table_id,
              'watermark': self.get_table_last_modified_utc(table_id)}
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=multiprocessing.cpu_count()) as executor:
      futures = list( map( lambda table_id: executor.submit( lookup_slice_meta_row, table_id ), table_ids ) )
    
    slice_meta_rows = [f.result() for f in futures]
    return pd.DataFrame(slice_meta_rows).sort_values(by='slice_id',ascending=False).reset_index(drop=True)
  
  def query_source_slice(self,slice_id) -> DataFrame:
    table_id = slice_id
    table_last_modified_utc = self.get_table_last_modified_utc(table_id)
    ga_df = self.get_spark_reader().option('table', table_id).load()
    source_slice = ga_df.selectExpr( 
      f'CAST("{table_id}" AS STRING) AS _table_id', 
      f'CAST("{table_last_modified_utc}" AS STRING) AS _table_last_modified_utc',
      *ga_df.columns )
    return source_slice
  
  def get_spark_reader(self):
    import base64
    return spark.read.format('bigquery') \
                .option('credentials', base64.b64encode(bytes(self.bq_cred_json, 'utf-8')).decode('ascii') ) \
                .option('parentProject' , self.bq_cred.project_id ) \
                .option('project', self.bq_cred.project_id ) \
                .option('dataset', self.bq_dataset_id )  

# COMMAND ----------

# DBTITLE 1,define concrete class: BigQueryPartitionedTableLakeIngester
class BigQueryPartitionedTableLakeIngester( LakeIngester ):
  """ BigQuery Partitioned Table
      slice_id = partitioned_col:str, watermark = COUNT(*) row_cnt
  """
  def __init__(self,
               bq_dataset_id:str, bq_tablename:str, bq_cred_json:str,
               bq_partitioned_col:str, bq_partitioned_col_type:str, bq_partitioned_col_pd_type:str,
               lake_tablename:str,
               additional_lake_partition_by:List[str] = None,
               **kwargs):
    self.bq_dataset_id, self.bq_tablename, self.bq_cred_json = bq_dataset_id, bq_tablename, bq_cred_json
    self.bq_partitioned_col, self.bq_partitioned_col_type, self.bq_partitioned_col_pd_type = \
      bq_partitioned_col, bq_partitioned_col_type, bq_partitioned_col_pd_type
    
    self.slice_id_source_expr, self.slice_id_source_type, self.slice_id_pd_type = \
      bq_partitioned_col, bq_partitioned_col_type, bq_partitioned_col_pd_type
    self.slice_watermark_source_expr, self.slice_watermark_source_type, self.slice_watermark_pd_type = \
      'COUNT(*)', 'BIGINT', 'int64'
    
    slice_id_lake_expr, slice_id_lake_type, slice_id_pd_type = \
       self.slice_id_source_expr, self.slice_id_source_type, self.slice_id_pd_type
    slice_watermark_lake_expr,slice_watermark_lake_type,slice_watermark_pd_type = \
      self.slice_watermark_source_expr, self.slice_watermark_source_type, self.slice_watermark_pd_type
    lake_partitionBy=[ bq_partitioned_col ] + ([] if additional_lake_partition_by is None else additional_lake_partition_by)
    
    super().__init__( lake_tablename,
                      slice_id_lake_expr, slice_id_lake_type, slice_id_pd_type,
                      slice_watermark_lake_expr,slice_watermark_lake_type,slice_watermark_pd_type,
                      lake_partitionBy = lake_partitionBy,
                      **kwargs )
    if self.do_compare_slice_row_cnt:
      raise RuntimeError(f' "do_compare_slice_row_cnt" is not supported for BigQueryPartitionedTableLakeIngester, cannot be set to True ')
      
  @property
  def bq_cred(self):
    from google.oauth2 import service_account
    import json
    return service_account.Credentials.from_service_account_info( json.loads(self.bq_cred_json) )
  
  @property
  def bq_client(self):
    from google.cloud import bigquery
    return bigquery.Client(credentials=self.bq_cred, project=self.bq_cred.project_id)  
  
  def get_spark_reader(self):
    import base64
    return spark.read.format('bigquery') \
               .option('credentials', base64.b64encode(bytes(self.bq_cred_json, 'utf-8')).decode('ascii') ) \
               .option('parentProject' , self.bq_cred.project_id ) \
               .option('project', self.bq_cred.project_id ) \
               .option('dataset', self.bq_dataset_id ) \
               .option('table', self.bq_tablename)
  
  def get_source_spark_schema(self) -> StructType:
    return self.get_spark_reader().load().schema
  
  def query_source_slice_meta(self) -> pd.DataFrame:
    return self.bq_client.query( f"""
      SELECT CAST({self.bq_partitioned_col} AS {self.bq_partitioned_col_type}) AS slice_id,
             COUNT(*) AS watermark
        FROM {self.bq_cred.project_id}.{self.bq_dataset_id}.{self.bq_tablename}
       GROUP BY 1
       ORDER BY 1 DESC
    """ ).result().to_dataframe().astype(self.slice_meta_pd_type)

  def query_source_slice(self,slice_id) -> DataFrame:
    slice_cond = f"CAST({self.bq_partitioned_col} AS {self.bq_partitioned_col_type})=CAST('{slice_id}' AS {self.bq_partitioned_col_type})"
    return self.get_spark_reader().option('filter', slice_cond ).load()

# COMMAND ----------

# DBTITLE 1,define concrete class: BigQuerySnapshotTableLakeIngester
class BigQuerySnapshotTableLakeIngester( LakeIngester ):
  """ prepend column '_last_modified_utc'
  """
  def __init__(self,
               bq_dataset_id:str, bq_tablename:str, bq_cred_json:str,
               lake_tablename:str,
               additional_lake_partition_by:List[str] = None,
               **kwargs):
    self.bq_dataset_id, self.bq_tablename, self.bq_cred_json = bq_dataset_id, bq_tablename, bq_cred_json
    
    slice_id_lake_expr, slice_id_lake_type, slice_id_pd_type = \
       "'snapshot'", 'STRING', 'str'
    slice_watermark_lake_expr,slice_watermark_lake_type,slice_watermark_pd_type = \
      'MAX(_last_modified_utc)', 'STRING', 'str'
    lake_partitionBy=[ '_last_modified_utc' ] + ([] if additional_lake_partition_by is None else additional_lake_partition_by)
    
    super().__init__( lake_tablename,
                      slice_id_lake_expr, slice_id_lake_type, slice_id_pd_type,
                      slice_watermark_lake_expr,slice_watermark_lake_type,slice_watermark_pd_type,
                      lake_partitionBy = lake_partitionBy,
                      **kwargs )
    if self.do_compare_slice_row_cnt:
      raise RuntimeError(f' "do_compare_slice_row_cnt" is not supported for BigQueryEventsLakeIngester yet, cannot be set to True ')
  
  @property
  def bq_cred(self):
    from google.oauth2 import service_account
    import json
    return service_account.Credentials.from_service_account_info( json.loads(self.bq_cred_json) )
  
  @property
  def bq_client(self):
    from google.cloud import bigquery
    return bigquery.Client(credentials=self.bq_cred, project=self.bq_cred.project_id)
  
  def get_bq_table_last_modified_utc(self) -> str:
    fqtn = f'{self.bq_cred.project_id}.{self.bq_dataset_id}.{self.bq_tablename}'
    return self.bq_client.get_table(fqtn).modified.astimezone(pytz.utc).strftime('%Y%m%d_%H%M%S')
  
  def get_spark_reader(self):
    import base64
    return spark.read.format('bigquery') \
               .option('credentials', base64.b64encode(bytes(self.bq_cred_json, 'utf-8')).decode('ascii') ) \
               .option('parentProject' , self.bq_cred.project_id ) \
               .option('project', self.bq_cred.project_id ) \
               .option('dataset', self.bq_dataset_id ) \
               .option('table', self.bq_tablename)
      
  def get_source_spark_schema(self) -> StructType:
    return self.query_source_slice('snapshot').schema
  
  def query_source_slice_meta(self) -> pd.DataFrame:
    return pd.DataFrame( [
      {'slice_id': 'snapshot', 'watermark': self.get_bq_table_last_modified_utc() }
    ] )
  
  def query_source_slice(self,slice_id) -> DataFrame:
    if slice_id != 'snapshot':
      raise RuntimeError(f'unexpected slice_id "{slice_id}", only "snapshot" is supported')
    return self.get_spark_reader().load().selectExpr( f"CAST('{self.get_bq_table_last_modified_utc()}' AS STRING) AS _last_modified_utc", '*' )

# COMMAND ----------

# DBTITLE 1,define concrete class: SparkLakeIngester
class SparkLakeIngester(LakeIngester):
  def __init__(self, 
               source_tablename:str,
               slice_id_source_expr:str, slice_id_source_type:str, slice_id_pd_type:str,
               slice_watermark_source_expr:str,  slice_watermark_source_type:str, slice_watermark_pd_type:str,
               lake_tablename:str, 
               
               slice_id_lake_expr:str=None, slice_id_lake_type:str=None,
               slice_watermark_lake_expr:str=None, slice_watermark_lake_type:str=None,
               
               lake_partitionBy:List[str]=None, 
               lake_reorder_put_first_columns:List[str]=None,
               lake_do_schema_check:bool=True,
               do_compare_slice_row_cnt:bool=False,
               do_optimize_per_slice:bool=False,
               do_drop_slice_if_not_in_source:bool=False):
    """ source_tablename: should be a SparkSQL table, e.g. 'cu_yourtable' or '(SELECT * FROM cu_yourtable LIMIT 10) t'
    """
    if slice_id_lake_expr is None:
      slice_id_lake_expr = slice_id_source_expr
    if slice_id_lake_type is None:
      slice_id_lake_type = slice_id_source_type
    if slice_watermark_lake_expr is None:
      slice_watermark_lake_expr = slice_watermark_source_expr
    if slice_watermark_lake_type is None:
      slice_watermark_lake_type = slice_watermark_source_type
    self.source_tablename = source_tablename
    self.slice_id_source_expr = slice_id_source_expr
    self.slice_id_source_type = slice_id_source_type
    self.slice_watermark_source_expr = slice_watermark_source_expr
    self.slice_watermark_source_type = slice_watermark_source_type
    
    super().__init__(lake_tablename,
                   slice_id_lake_expr,slice_id_lake_type,slice_id_pd_type,
                   slice_watermark_lake_expr,slice_watermark_lake_type,slice_watermark_pd_type,
                   lake_partitionBy=lake_partitionBy,
                   lake_reorder_put_first_columns=lake_reorder_put_first_columns,
                   lake_do_schema_check=lake_do_schema_check,
                   do_compare_slice_row_cnt=do_compare_slice_row_cnt,
                   do_optimize_per_slice=do_optimize_per_slice,
                   do_drop_slice_if_not_in_source=do_drop_slice_if_not_in_source)
    
  def get_source_spark_schema(self) -> StructType:
    return spark.sql(f'SELECT * FROM {self.source_tablename}').schema
  
  def compose_query_source_slice_meta_sql(self) -> str:
    """ must return [slice_id, watermark] """
    return f"""
      WITH meta AS (
        SELECT CAST({self.slice_id_source_expr} AS {self.slice_id_source_type}) AS slice_id,
               {self.slice_watermark_source_expr} AS watermark
               { ', COUNT(1) AS row_cnt' if self.do_compare_slice_row_cnt else '' }
          FROM {self.source_tablename}
         GROUP BY slice_id )
      SELECT slice_id, CAST(watermark AS {self.slice_watermark_source_type}) AS watermark
             {', row_cnt' if self.do_compare_slice_row_cnt else ''}
        FROM meta
    """
  
  def query_source_slice_meta(self) -> pd.DataFrame:
    """ return Pandas.DF { slice_id, watermark, [row_cnt] } """
    query_source_meta_sql = self.compose_query_source_slice_meta_sql()
    logger.debug( f'query Source meta with SQL:\n{query_source_meta_sql}' )
    return spark.sql(query_source_meta_sql) \
                .toPandas() \
                .astype( self.slice_meta_pd_type ) \
                .sort_values(by='slice_id',ascending=False) \
                .reset_index(drop=True)
  
  def query_source_slice(self,slice_id) -> DataFrame:
    slice_cond = f"CAST({self.slice_id_source_expr} AS {self.slice_id_source_type}) = CAST('{str(slice_id)}' AS {self.slice_id_source_type})"
    return spark.sql(f'SELECT * FROM {self.source_tablename} WHERE {slice_cond}')

# COMMAND ----------

# DBTITLE 1,define concrete class: TreasureDataTableLakeIngester
class TreasureDataTableLakeIngester(JdbcLakeIngester):
  """ Require TreasureData JDBC Driver, Maven coordinate= com.treasuredata:td-jdbc:0.5.10
      As of 2022June, TreasureData is built on top of S3, so cannot use Azure Spark to directly read from TreasureData
      * https://docs.treasuredata.com/display/public/PD/Treasure+Data+Apache+Spark+Driver+Release+Notes 
      
      'td_tablename' must be a TreasureData Table, cannot be a query
      will prepend column '_utc_date: VARCHAR' at the original source_tablename
  """
  def __init__(self, 
               td_database:str, 
               td_tablename:str,
               lake_tablename:str, 
               
               td_api_key:str = None,
               additional_lake_partitionBy:List[str] = None,
               do_compare_slice_row_cnt:bool = True,
               **kwargs):
    if td_api_key is None:
      td_api_key = get_secret_cfg('treasuredata.api_key_secret')
    
    self.td_database, self.td_api_key, self.td_tablename = td_database, td_api_key, td_tablename
    
    source_tablename = f'(SELECT CAST( CAST(FROM_UNIXTIME(time) AS DATE) AS VARCHAR) AS _utc_date,  * FROM {td_tablename}) t'
    lake_partitionBy = ['_utc_date'] + [] if additional_lake_partitionBy is None else additional_lake_partitionBy
    
    slice_id_source_expr, slice_id_source_type, slice_id_pd_type = \
      '_utc_date', 'VARCHAR', 'str'
    slice_watermark_source_expr, slice_watermark_source_type, slice_watermark_pd_type = \
      'MAX(time)',  'BIGINT', 'int64'
    
    slice_id_lake_type = 'STRING'
    
    super().__init__(source_tablename,
                  slice_id_source_expr,slice_id_source_type,slice_id_pd_type,
                  slice_watermark_source_expr,slice_watermark_source_type,slice_watermark_pd_type,
                  lake_tablename,
                  lake_partitionBy=lake_partitionBy,
                  do_compare_slice_row_cnt = do_compare_slice_row_cnt,
                  slice_id_lake_type = slice_id_lake_type,
                  do_drop_slice_if_not_in_source = True,
                  **kwargs)
  
  def get_source_jdbc_reader(self):
    return spark.read.format('jdbc') \
              .option('driver', 'com.treasuredata.jdbc.TreasureDataDriver' ) \
              .option('url', f'jdbc:td://api.treasuredata.com/{self.td_database}' ) \
              .option('apikey', self.td_api_key )
  
  def compose_query_source_meta_sql(self,do_include_row_cnt:bool) -> str: 
    return f"""
    WITH meta AS (
      SELECT _utc_date AS slice_id,
             MAX(time) AS watermark
             { ', COUNT(1) AS row_cnt' if do_include_row_cnt else '' }
        FROM {self.source_tablename}
       GROUP BY 1 )
    SELECT slice_id, CAST(watermark AS {self.slice_watermark_source_type}) AS watermark
           {', row_cnt' if do_include_row_cnt else ''}
      FROM meta 
    """
  
  @property
  def jdbc_fetchsize(self) -> int:
    # totally arbitrary
    return 100_000 
  
  def compose_query_source_slice_sql(self, slice_id) -> str:
    utc_date = slice_id
    return f"""
    SELECT CAST(CAST(FROM_UNIXTIME(time) AS DATE) AS VARCHAR) AS _utc_date, *
      FROM {self.td_tablename} 
     WHERE TD_TIME_RANGE( time,'{utc_date}', TD_TIME_ADD('{utc_date}', '1d') ,'UTC'  )
    """
  
  def query_source_df(self,query_sql:str) -> DataFrame:
     # As of 2022June, cannot use Pyspark JDBC to read from TreasureData
     # * td-jdbc 'com.treasuredata.jdbc.TreasureDataDriver' throws exception when reading special string like '%ℝ%'
     # * trino jdbc 'io.trino.jdbc.TrinoDriver' version 350 seems to use PreparedStatement which is not supported by TreasureData
    import pytd
    td_client = pytd.Client(apikey=self.td_api_key, database=self.td_database)
    return spark.createDataFrame( td_client.query( query_sql )['data'], schema=self.get_source_spark_schema() )

# COMMAND ----------

# DBTITLE 1,define concrete class: CompositeLakeIngester
class CompositeLakeIngester(LakeIngester):
  def __init__(self, ingesters:List[LakeIngester]):
    if len({ (i.lake_tablename,
              i.slice_id_lake_expr, i.slice_id_lake_type, i.slice_id_pd_type,
              i.slice_watermark_lake_expr, i.slice_watermark_lake_type, i.slice_watermark_pd_type,
              tuple( i.lake_partitionBy ) if type(i.lake_partitionBy) is list else i.lake_partitionBy, 
              tuple( [(f.name, f.dataType) for f in i.get_lake_spark_schema()] ), 
              i.lake_do_schema_check, 
              i.do_compare_slice_row_cnt, i.do_optimize_per_slice, i.do_drop_slice_if_not_in_source) for i in ingesters}) != 1:
      raise RuntimeError(f""" every ingester i must have the same values for the following properties:
        i.lake_tablename,
        i.slice_id_lake_expr, i.slice_id_lake_type, i.slice_id_pd_type,
        i.slice_watermark_lake_expr, i.slice_watermark_lake_type, i.slice_watermark_pd_type,
        i.lake_partitionBy, [(f.name, f.dataType)for f in i.get_lake_spark_schema()], i.lake_do_schema_check, 
        i.do_compare_slice_row_cnt, i.do_optimize_per_slice, i.do_drop_slice_if_not_in_source
      """)
    self.ingesters = ingesters
    self._ingester_cached_source_slice_meta_dict = None  
    i0 = self.ingesters[0]
    super().__init__(i0.lake_tablename, 
                    i0.slice_id_lake_expr,i0.slice_id_lake_type, i0.slice_id_pd_type,
                    i0.slice_watermark_lake_expr,i0.slice_watermark_lake_type,i0.slice_watermark_pd_type,
                    lake_partitionBy=i0.lake_partitionBy,
                    lake_do_schema_check=i0.lake_do_schema_check,
                    do_compare_slice_row_cnt=i0.do_compare_slice_row_cnt,
                    do_optimize_per_slice=i0.do_optimize_per_slice,
                    do_drop_slice_if_not_in_source=i0.do_drop_slice_if_not_in_source)
    
    
  @property
  def ingester_cached_source_slice_meta_dict(self) -> Dict[LakeIngester, pd.DataFrame]:
    if self._ingester_cached_source_slice_meta_dict is None:
      with concurrent.futures.ThreadPoolExecutor(max_workers=multiprocessing.cpu_count()) as executor:
        slice_meta_futures = [ executor.submit( ingester.calc_slice_meta ) for ingester in self.ingesters]
      slice_metas = [ f.result() for f in slice_meta_futures ]
      self._ingester_cached_source_slice_meta_dict = dict( zip( self.ingesters, slice_metas ) )
    
    return self._ingester_cached_source_slice_meta_dict  
  
  def choose_ingester(self, slice_id) -> LakeIngester:
    """ given a slice_id, choose which ingester to use """

    def convert_to_comparator( ingester, slice_meta ):
      slice_id_exists_in_this_ingester = int( slice_id in slice_meta[ slice_meta['to_upsert'] ]['slice_id'].tolist() )
      priority_of_this_ingester =  len(self.ingesters) - self.ingesters.index( ingester )
      return ( slice_id_exists_in_this_ingester, priority_of_this_ingester, ingester )

    chosen = max([ convert_to_comparator(ingester,slice_meta) for ingester, slice_meta in self.ingester_cached_source_slice_meta_dict.items() ])
    return chosen[2]
  
  def pick_upsert_slice_id_list(self) -> List:
    import itertools
    return sorted( list( set( itertools.chain.from_iterable( 
                                 [ slice_meta[ slice_meta['to_upsert'] ]['slice_id'].tolist() 
                                 for slice_meta in self.ingester_cached_source_slice_meta_dict.values() ] ) ) ), 
                  reverse=True )
  
  def get_lake_spark_schema(self) -> StructType:
    """ all ingesters should return the same """
    return self.ingesters[0].get_lake_spark_schema()
  
  def query_source_slice(self,slice_id) -> DataFrame:
    """ pick one ingester to query_source_slice """
    return self.choose_ingester(slice_id).query_source_slice(slice_id)
  
  def get_source_spark_schema(self) -> StructType:
    raise RuntimeError(f"""
    CompositeLakeIngester should not call   function "get_source_spark_schema" since "get_lake_spark_schema" is overridden """)
  
  def query_source_slice_meta(self) -> pd.DataFrame:
    raise RuntimeError(f"""
    CompositeLakeIngester should not call function "query_source_slice_meta" since "pick_upsert_slice_id_list" is overridden """)
