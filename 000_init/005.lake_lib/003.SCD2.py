# Databricks notebook source
# DBTITLE 1,define the core logic to merge rows: _merge_same_pk_rows_by_priority
from pyspark.sql import Row
def _merge_same_pk_rows_by_priority(rows: List[Row],
                       start_dt_col: str = '_start_jst_dt',
                       end_dt_col: str = '_end_jst_dt',
                       priority_col: str = '_priority') -> List[Row]:
    """
      input: each row require these columns: _start_dt:Date, _end_dt:Date, _priority:String, 
             _start_dt and _end_dt are inclusive and not null
      output: the merged rows by _priority. larger _priority = higher priority, 
              e.g. _priority '99' > _priority '01', 
                   _priority '99' > _priority '100'
    """
    from collections import defaultdict
    from collections import deque
    from datetime import timedelta

    def add_one_day(dt):
        return dt + timedelta(days=1)

    def minus_one_day(dt):
        return dt - timedelta(days=1)

    def consolidate_same_priority(rows: List[Dict]) -> List[Dict]:
        # assume all rows have same priority, consolidate overlapping intervals
        rows.sort(key=lambda r: r[start_dt_col])

        consolidated = []
        for row in rows:
            if not consolidated or consolidated[-1][end_dt_col] < minus_one_day(row[start_dt_col]):
                consolidated.append(row)
            else:
                consolidated[-1][end_dt_col] = max(consolidated[-1][end_dt_col],
                                                   row[end_dt_col])
        return consolidated

    def merge_by_priority(low_p: List[Row], high_p: List[Row]) -> List[Row]:
        lows = deque(consolidate_same_priority([r.asDict() for r in low_p]))
        highs = deque(consolidate_same_priority([r.asDict() for r in high_p]))
        merged = []

        while lows and highs:
            # high itvl starts first
            if highs[0][start_dt_col] <= lows[0][start_dt_col]:
                # drop all low itvl end within current high itvl
                while lows and highs[0][end_dt_col] >= lows[0][end_dt_col]:
                    lows.popleft()
                # if current high itvl ends within the next low itvl,
                # need to set back next low itvl._start_dt
                if lows and highs[0][end_dt_col] > lows[0][start_dt_col]:
                    # highs[0]['_end_dt'] < lows[0]['_end_dt'] is implied
                    lows[0][start_dt_col] = add_one_day(highs[0][end_dt_col])
                merged.append(highs.popleft())
            # low itvl starts first
            else:  # highs[0]['_start_dt'] > lows[0]['_start_dt']
                # current low itvl covers high itvl,
                # split curr.low.itvl into two:
                # 1. adopt earlier.low.itvl.end_dt = high.itvl.start_dt-1,
                # 2. let next iteration handle later.low.itvl.start_dt = high.itvl.end_dt+1
                if highs[0][end_dt_col] < lows[0][end_dt_col]:
                    orig_low = lows.popleft()
                    earlier_half_low = {
                        **orig_low ** {end_dt_col: minus_one_day(highs[0][start_dt_col])}}
                    later_low = {**orig_low, **
                                 {start_dt_col: add_one_day(highs[0][end_dt_col])}}
                    merged.append(earlier_half_low)
                    lows.appendleft(later_low)
                # current low.itvl.end inside high.itvl
                # adopt curr.low.itvl.end_dt = high.itvl.start_dt-1
                elif highs[0][start_dt_col] <= lows[0][end_dt_col]:
                  # highs[0]['_end_dt'] >= lows[0]['_end_dt'] implied
                    lows[0][end_dt_col] = minus_one_day(highs[0][start_dt_col])
                    merged.append(lows.popleft())
                # current low itvl ends
                else:  # highs[0]['_end_dt'] >= lows[0]['_end_dt'] AND highs[0]['_start_dt'] > lows[0]['_end_dt'] implied
                    merged.append(lows.popleft())
        while lows:
            merged.append(lows.popleft())
        while highs:
            merged.append(highs.popleft())
        return [Row(**r) for r in merged]

    priority_dict = defaultdict(list)
    for row in rows:
        priority_dict[row[priority_col]].append(row)
    prioritized_batches = sorted(
        list(priority_dict.values()), key=lambda rows: rows[0][priority_col])
    from functools import reduce
    return reduce(merge_by_priority, prioritized_batches, [])

# COMMAND ----------

# DBTITLE 1,define how to merge two SCD2 Spark.DF: _merge_scd2()
def _merge_scd2( ava,eve,pk_cols:List[str] ):
  """ 
  """
  ava_simp_schema = sorted( [ f'{c.name} {c.dataType}' for c in ava.schema ] )
  eve_simp_schema = sorted( [ f'{c.name} {c.dataType}' for c in eve.schema ])
  if ava_simp_schema != eve_simp_schema:
    raise RuntimeError(f""" input 'ava' and 'eve' should have similar schema (same column name&type, ignore Nullability)
    ava schema: {ava_simp_schema}
    eve schema: {eve_simp_schema}
    """)

  eva = ava.unionAll(eve)
  eva_cols = [ (c.name, str( type(c.dataType) ) ) for c in eva.schema ]

  from pyspark.sql.types import DateType,StringType
  must_have_audit_cols = [ # ( col_name, col_type )
    ('_start_jst_dt', str( DateType ) ),
    ('_end_jst_dt', str( DateType ) ),
    ('_priority', str( StringType) ) ]
  for mhac in must_have_audit_cols:
    if not mhac in eva_cols:
      raise RuntimeError(f""" ava/eve is missing some must-have audit column: {mhac} """)
  for pkc in pk_cols:
    if not pkc in [ c.name for c in eva.schema ]:
      raise RuntimeError(f""" ava/eve is missing pk_col: {pkc} """)
  if eva.where( ' OR '.join( [ f'{mhac} IS NULL' for mhac,datatype in must_have_audit_cols ] ) ).limit(1).count() > 0:
    raise RuntimeError(f""" NULL is not allowed in must-have audit columns: {  [ mhac for mhac,datatype in must_have_audit_cols ] }  """)
    
  return spark.createDataFrame(
           eva.rdd.map( lambda row: ( tuple( [ row[pkc] for pkc in pk_cols] ) , row ) ) \
              .groupByKey() \
              .mapValues( _merge_same_pk_rows_by_priority ) \
              .flatMap(lambda kv: kv[1])
            ,schema=eva.schema)

# COMMAND ----------

# DBTITLE 1,define how to 'upsert' Lake SCD2 Table: persist_lake_scd2_table()
def persist_lake_scd2_table( lake_tablename:str, src_df, pk_cols:List[str], partitionBy:List[str]=None ):
  """ Merge 'src_df' into Lake Table 'lake_tablename' SCD2 style. 
      When two time spans overlap, LARGER '_priority' will win. (e.g. priority '99' will win over '100')
      e.g. given the following two rows, 
        pk1, _start_jst_dt=2022-06-01, _end_jst_dt=20220-06-10, _priority='100'
        pk1, _start_jst_dt=2022-06-06, _end_jst_dt=20220-06-15, _priority='99'
      the expected output will be
        pk1, _start_jst_dt=2022-06-01, _end_jst_dt=20220-06-05, _priority='100'
        pk1, _start_jst_dt=2022-06-06, _end_jst_dt=20220-06-15, _priority='99'
        
      src_df: must have these columns: _start_jst_dt:DATE, _end_jst_dt:DATE, _priority:STRING
  """
  
  if partitionBy is None:
    partitionBy = []
  if not all( map( lambda pkc: pkc in [c.name for c in src_df.schema ] , pk_cols ) ):
    raise RuntimeError(f""" src_df must have all pk_cols """)

  from pyspark.sql.types import DateType,StringType
  src_cols = [ (c.name, str( type(c.dataType) ) ) for c in src_df.schema ]
  must_have_audit_cols = [ # ( col_name, col_type )
    ('_start_jst_dt', str( DateType ) ),
    ('_end_jst_dt', str( DateType ) ),
    ('_priority', str( StringType) ) ]
  for mhac in must_have_audit_cols:
    if not mhac in src_cols:
      raise RuntimeError(f""" src_df is missing some must-have audit column: {mhac} """)
  if src_df.where( ' OR '.join( [ f'{mhac} IS NULL' for mhac,datatype in must_have_audit_cols ] ) ).limit(1).count() > 0:
      raise RuntimeError(f""" NULL is not allowed in must-have audit columns: {  [ mhac for mhac,datatype in must_have_audit_cols ] }  """)

  logger.debug(f'ensure {lake_tablename} exists ....')    
  ensure_lake_table_exists( lake_tablename, src_df.schema, partitionBy )

  base_df = get_delta_table( lake_tablename ).toDF()
  desired_df = _merge_scd2( base_df, src_df, pk_cols )

  # for Delta Table, below is the 'primary key'
  delta_key_cols = pk_cols+['_start_jst_dt','_end_jst_dt','_priority']

  # delete all delta_key that exists in 'base' but not in 'desired'
  logger.debug(f'deleting from {lake_tablename} ...')
  get_delta_table( lake_tablename ).alias('base') \
    .merge( base_df.select(delta_key_cols).exceptAll( desired_df.select(delta_key_cols) ).alias('to_del'), 
            ' AND '.join( [ f'base.{kc} <=> to_del.{kc}' for kc in delta_key_cols ] ) ) \
    .whenMatchedDelete() \
    .execute()

  # upsert 'base' according to 'desired'
  logger.debug(f'upserting {lake_tablename} ...')
  get_delta_table( lake_tablename ).alias('base') \
    .merge( desired_df.alias('desired'),
            ' AND '.join( [ f'base.{kc} <=> desired.{kc}' for kc in delta_key_cols ] ) ) \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()

  logger.debug(f'... SCD2 Lake Table {lake_tablename} updated')
    
  return
