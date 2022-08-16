# Databricks notebook source
# MAGIC %run ./000.init_flow

# COMMAND ----------

# DBTITLE 1,init notebook
# the ENV has to be 'cicd'
init_nb( 'cicd' )
enable_debug_logging()

# COMMAND ----------

# DBTITLE 1,run test cases: LakeTableDDLTests
import unittest
class LakeTableDDLTests(unittest.TestCase):
  def test_happy_path(self):
    tablename = 'tmp_laketableddltest_happypath'
    tabledf = spark.createDataFrame( [
      {'name':'ava', 'age':19, 'job':'mechanic'},
      {'name':'bob', 'age':35, 'job':'programmer'},
      {'name':'eve', 'age':21, 'job':'lumberjack'},
    ] )
    tableschema = tabledf.schema
    
    # make sure no the table does not exist
    destroy_lake_table( tablename )
    assert( not exists_lake_table( tablename ) )
    
    # create lake table
    ensure_lake_table_exists( tablename, tableschema, ['name'] )
    assert( exists_lake_table( tablename ) )
    assert( spark.sql(f'SELECT * FROM {tablename}').schema == tableschema )
    
    # re-create lake table
    ensure_lake_table_exists( tablename, tableschema, ['name'] )
    assert( exists_lake_table( tablename ) )
    assert( spark.sql(f'SELECT * FROM {tablename}').schema == tableschema )
    
    # should not allow re-create with different order columns 
    # column order matters a bit because DeltaLake only collects statistics on first 32 columns
    # https://docs.microsoft.com/en-us/azure/databricks/kb/delta/zordering-ineffective-column-stats
    with self.assertRaises(RuntimeError, msg=f"""
    should not allow re-create with different order columns 
    column order matters a bit because DeltaLake only collects statistics on first 32 columns
    https://docs.microsoft.com/en-us/azure/databricks/kb/delta/zordering-ineffective-column-stats
    """):
      from pyspark.sql.types import StructType
      mismatch_reordered_schema = StructType( 
        [ f for f in tableschema if f.name=='job' ] +  [ f for f in tableschema if f.name != 'job' ] )
      ensure_lake_table_exists( tablename, mismatch_reordered_schema, ['name'] )
      pass
    
    # destroy lake table
    destroy_lake_table( tablename )
    assert( not exists_lake_table( tablename ) )

_test_res = unittest.TextTestRunner().run(
    unittest.TestLoader().loadTestsFromTestCase(LakeTableDDLTests))

if len(_test_res.errors) or len(_test_res.failures):
  raise RuntimeError(f'{len(_test_res.errors)} tests encountered error, {len(_test_res.failures)} tests failed')

# COMMAND ----------

# DBTITLE 1,run test cases: LakeLibTests
import unittest
class LakeLibTests(unittest.TestCase):
  
  def test_merge_schema(self):
    base_df = spark.createDataFrame( [
      {'col_1':1  , 'col_2':'b'  , 'col_3':'3'   },
      {'col_1':11 , 'col_2':'bb' , 'col_3':'33'  },
      {'col_1':111, 'col_2':'bbb', 'col_3':'333' },
    ] )
    base_df.createOrReplaceTempView('vtmp_test_merge_schema')
    
    destroy_lake_table( 'tmp_test_merge_schema' )
    
    # ingest 'old schema', col_1, col_3
    SparkLakeIngester(
      '(SELECT col_1, col_3 FROM vtmp_test_merge_schema WHERE col_1 = 1) t', # source_tablename:str,
      'col_1', 'INT', 'int', # slice_id_source_expr:str, slice_id_source_type:str, slice_id_pd_type:str,
      'MAX(col_3)', 'BIGINT', 'int64', # slice_watermark_source_expr:str,  slice_watermark_source_type:str, slice_watermark_pd_type:str,
      'tmp_test_merge_schema', # lake_tablename:str, 
      lake_partitionBy = ['col_1'],
      lake_reorder_put_first_columns = ['col_3'],
      do_compare_slice_row_cnt = True,
      do_optimize_per_slice = True,
      do_drop_slice_if_not_in_source = True
    ).ingest()
    
    with self.assertRaises(RuntimeError, msg=f"""
      when 'lake_do_schema_check' is set to True, should not allow schema change
    """):
      SparkLakeIngester(
        '(SELECT col_1, col_2, col_3 FROM vtmp_test_merge_schema) t', # source_tablename:str,
        'col_1', 'INT', 'int', # slice_id_source_expr:str, slice_id_source_type:str, slice_id_pd_type:str,
        'MAX(col_3)', 'BIGINT', 'int64', # slice_watermark_source_expr:str,  slice_watermark_source_type:str, slice_watermark_pd_type:str,
        'tmp_test_merge_schema', # lake_tablename:str, 
        lake_partitionBy = ['col_1'],
        lake_reorder_put_first_columns = ['col_3'],
        lake_do_schema_check = True,
        do_compare_slice_row_cnt = True,
        do_optimize_per_slice = True,
        do_drop_slice_if_not_in_source = True
      ).ingest()
      pass
    assert( len(spark.sql('SELECT * FROM tmp_test_merge_schema').columns) == 2 )
    
    # allow schema change when 'lake_do_schema_check' is set to False
    SparkLakeIngester(
        '(SELECT col_1, col_2, col_3 FROM vtmp_test_merge_schema) t', # source_tablename:str,
        'col_1', 'INT', 'int', # slice_id_source_expr:str, slice_id_source_type:str, slice_id_pd_type:str,
        'MAX(col_3)', 'BIGINT', 'int64', # slice_watermark_source_expr:str,  slice_watermark_source_type:str, slice_watermark_pd_type:str,
        'tmp_test_merge_schema', # lake_tablename:str, 
        lake_partitionBy = ['col_1'],
        lake_reorder_put_first_columns = ['col_3'],
        lake_do_schema_check = False,
        do_compare_slice_row_cnt = True,
        do_optimize_per_slice = True,
        do_drop_slice_if_not_in_source = True
      ).ingest()
    assert( len(spark.sql('SELECT * FROM tmp_test_merge_schema').columns) == 3 )
    
    destroy_lake_table( 'tmp_test_merge_schema' )
    return
    
  
  def test_ingester_spark_happy_path(self):
    expected_test_df = spark.createDataFrame( [
      {'col_1':1  , 'col_2':'b'  , 'col_3':'3'   },
      {'col_1':11 , 'col_2':'bb' , 'col_3':'33'  },
      {'col_1':111, 'col_2':'bbb', 'col_3':'333' },
    ] )
    expected_test_df.createOrReplaceTempView('vtmp_test_spark_lake_ingester')
    
    # clear existing Lake Table and ingest for the first time
    destroy_lake_table( 'tmp_test_spark_lake_ingester' )
    spark_ingester = SparkLakeIngester(
      '(SELECT * FROM vtmp_test_spark_lake_ingester) t', # source_tablename:str,
      'col_1', 'INT', 'int', # slice_id_source_expr:str, slice_id_source_type:str, slice_id_pd_type:str,
      'MAX(col_3)', 'BIGINT', 'int64', # slice_watermark_source_expr:str,  slice_watermark_source_type:str, slice_watermark_pd_type:str,
      'tmp_test_spark_lake_ingester', # lake_tablename:str, 
      lake_partitionBy = ['col_1'],
      lake_reorder_put_first_columns = ['col_3','col_2'],
      do_compare_slice_row_cnt = True,
      do_optimize_per_slice = True,
      do_drop_slice_if_not_in_source = True
    )
    spark_ingester.ingest()
    
    actual1_df = spark.sql(f'SELECT * FROM tmp_test_spark_lake_ingester')
    assert( actual1_df.count() == 3 )
    assert( [f.name for f in actual1_df.schema] == ['col_3','col_2','col_1'] )
    assert( all( ( actual1_df[['col_1','col_2','col_3']].toPandas().sort_values(by='col_1').reset_index(drop=True)['col_1'] 
                == expected_test_df[['col_1','col_2','col_3']].toPandas().sort_values(by='col_1').reset_index(drop=True)['col_1'] ).tolist() ) )
    assert( all( ( actual1_df[['col_1','col_2','col_3']].toPandas().sort_values(by='col_1').reset_index(drop=True)['col_2'] 
                == expected_test_df[['col_1','col_2','col_3']].toPandas().sort_values(by='col_1').reset_index(drop=True)['col_2'] ).tolist() ) )
    assert( all( ( actual1_df[['col_1','col_2','col_3']].toPandas().sort_values(by='col_1').reset_index(drop=True)['col_3'] 
                == expected_test_df[['col_1','col_2','col_3']].toPandas().sort_values(by='col_1').reset_index(drop=True)['col_3'] ).tolist() ) )
    
    # with Lake Table in place, ingest again
    spark_ingester.ingest()
    actual2_df = spark.sql(f'SELECT * FROM tmp_test_spark_lake_ingester')
    assert( actual2_df.count() == 3 )
    assert( [f.name for f in actual2_df.schema] == ['col_3','col_2','col_1'] )
    assert( all( ( actual2_df[['col_1','col_2','col_3']].toPandas().sort_values(by='col_1').reset_index(drop=True)['col_1'] 
                == expected_test_df[['col_1','col_2','col_3']].toPandas().sort_values(by='col_1').reset_index(drop=True)['col_1'] ).tolist() ) )
    assert( all( ( actual2_df[['col_1','col_2','col_3']].toPandas().sort_values(by='col_1').reset_index(drop=True)['col_2'] 
                == expected_test_df[['col_1','col_2','col_3']].toPandas().sort_values(by='col_1').reset_index(drop=True)['col_2'] ).tolist() ) )
    assert( all( ( actual2_df[['col_1','col_2','col_3']].toPandas().sort_values(by='col_1').reset_index(drop=True)['col_3'] 
                == expected_test_df[['col_1','col_2','col_3']].toPandas().sort_values(by='col_1').reset_index(drop=True)['col_3'] ).tolist() ) )
    
    # clean up
    destroy_lake_table( 'tmp_test_spark_lake_ingester' )
    return

_test_res = unittest.TextTestRunner().run(
    unittest.TestLoader().loadTestsFromTestCase(LakeLibTests))

if len(_test_res.errors) or len(_test_res.failures):
  raise RuntimeError(f'{len(_test_res.errors)} tests encountered error, {len(_test_res.failures)} tests failed')

# COMMAND ----------

# DBTITLE 1,run test cases for SCD2 function: _merge_same_pk_rows_by_priority
import unittest
class MergeSamePKRowsTests(unittest.TestCase):
    def test_merge_same_pk_rows(self):
        import dateutil.parser as dp
        from datetime import timedelta
        # test function 'merge_same_pk_rows'
        # input: [ (start_dt, end_dt, priority, data) ]
        def convert_tuple_to_row(t: Tuple) -> Row:
            base_dt = dp.parse('2020-05-01').date()
            start_dt_offset, end_dt_offset, priority, data = t
            return Row(**{
                '_start_jst_dt': base_dt + timedelta(days=start_dt_offset),
                '_end_jst_dt': base_dt + timedelta(days=end_dt_offset),
                '_priority': priority,
                'data': data
            })
        def run_test(test_case_name: str, input_tuples: List[Tuple], expected_tuples: List[Tuple]) -> List[Row]:
            same_pk_rows = [convert_tuple_to_row(t) for t in input_tuples]
            actual_rows = _merge_same_pk_rows_by_priority(same_pk_rows)
            expected_rows = [convert_tuple_to_row(t) for t in expected_tuples]
            self.assertEqual(actual_rows, expected_rows, f"""
                test case '{test_case_name}' failed
                was expecting: 
                {expected_rows}
                 actually got:
                {actual_rows}
            """)

        run_test('happy path',
                 # input: (start_dt, end_dt, priority, data)
                 [   (-1,  2, '001', 'L'),
                     (3,  4, '001', 'L'),
                     (4,  6, '001', 'L'),
                     (9, 15, '001', 'L'),
                     (17, 999, '001', 'L'),
                     (-1,  5, '002', 'H'),
                     (7, 10, '002', 'H'),
                     (19, 999, '002', 'H'), ],
                 # expected output
                 [   (-1,  5, '002', 'H'),
                     (6,  6, '001', 'L'),
                     (7, 10, '002', 'H'),
                     (11, 15, '001', 'L'),
                     (17, 18, '001', 'L'),
                     (19, 999, '002', 'H'), ])
        run_test('happy path unordered',
                 # input: (start_dt, end_dt, priority, data)
                 [   (7, 10, '002', 'H'),
                     (4,  6, '001', 'L'),
                     (-1,  5, '002', 'H'),
                     (9, 15, '001', 'L'),
                     (3,  4, '001', 'L'),
                     (17, 999, '001', 'L'),
                     (19, 999, '002', 'H'),
                     (-1,  2, '001', 'L'), ],
                 # expected output
                 [   (-1,  5, '002', 'H'),
                     (6,  6, '001', 'L'),
                     (7, 10, '002', 'H'),
                     (11, 15, '001', 'L'),
                     (17, 18, '001', 'L'),
                     (19, 999, '002', 'H'), ])
        run_test('single interval, multiple priorities',
                 # input: (start_dt, end_dt, priority, data)
                 [(2, 3, '001', 'L'),
                  (2, 3, '002', 'M'),
                  (2, 3, '003', 'H'), ],
                 # expected output
                 [(2,  3, '003', 'H'), ])

_test_res = unittest.TextTestRunner().run(
    unittest.TestLoader().loadTestsFromTestCase(MergeSamePKRowsTests))

if len(_test_res.errors) or len(_test_res.failures):
  raise RuntimeError(f'{len(_test_res.errors)} tests encountered error, {len(_test_res.failures)} tests failed')

# COMMAND ----------

# DBTITLE 1,run test cases for SCD2 function: _merge_scd2
import unittest
class MergeSCD2Tests(unittest.TestCase):
    def test_happy_path(self):
        import dateutil.parser as dp
        from datetime import timedelta
        ava_tuples = [ (42,-1,2,'001','L'),(42,3,4,'001','L'),(42,3,4,'001','L'),(42,4,6,'001','L'),
                        (42,9,15,'001','L'),(42,17,999,'001','L') ]
        eve_tuples = [ (42,-1,5,'002','H'),(42,7,10,'002','H'),(42,19,999,'002','H'), ]
        expected_tuples = [ (42,-1,5,'002','H'),(42,6,6,'001','L'),(42,7,10,'002','H'),
                            (42,11,15,'001','L'),(42,17,18,'001','L'),(42,19,999,'002','H'), ]
        
        def _convert_tuple_to_row(t: Tuple) -> Row:
          base_dt = dp.parse('2020-05-01').date()
          pk, start_dt_offset, end_dt_offset, priority, data = t
          return Row(**{
              'pk':pk,
              '_start_jst_dt': base_dt + timedelta(days=start_dt_offset),
              '_end_jst_dt': base_dt + timedelta(days=end_dt_offset),
              '_priority': priority,
              'data': data
          }) 
        
        from pyspark.sql.types import StructType,StructField, StringType, DateType, IntegerType
        _eva_schema = StructType([       
            StructField('pk', IntegerType(), True),    
            StructField('_start_jst_dt', DateType(), False), StructField('_end_jst_dt', DateType(), False), StructField('_priority', StringType(), False),
            StructField('data', StringType(),True),
        ])    
        
        _ava = spark.createDataFrame( [_convert_tuple_to_row(t) for t in ava_tuples],schema=_eva_schema )
        _eve = spark.createDataFrame( [_convert_tuple_to_row(t) for t in eve_tuples],schema=_eva_schema ) 

        _expected = spark.createDataFrame( [_convert_tuple_to_row(t) for t in expected_tuples],schema=_eva_schema )
        _actual = _merge_scd2( _ava,_eve,['pk'] )
        _diff = compare_spark_df( _expected,_actual,['pk','_start_jst_dt','_end_jst_dt'],ava_alias='expected',eve_alias='actual' )
        self.assertEqual( _diff.where(  "diff_type != 'same'" ).limit(1).count() , 0 )
        return

_test_res = unittest.TextTestRunner().run(
    unittest.TestLoader().loadTestsFromTestCase(MergeSCD2Tests))

if len(_test_res.errors) or len(_test_res.failures):
  raise RuntimeError(f'{len(_test_res.errors)} tests encountered error, {len(_test_res.failures)} tests failed')
