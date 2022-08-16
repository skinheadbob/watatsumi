# Databricks notebook source
# DBTITLE 1,define ABC class: LandingBlobIngester
import abc
import concurrent.futures
import multiprocessing
import re
import tempfile
from abc import ABC
from typing import List, Tuple
from tqdm import tqdm
from datetime import datetime

import pandas as pd
import pytz
from azure.storage.blob import ContainerClient

class LandingBlobIngester(ABC):
    """
      Grab blobs from Source, upload to Azure Blob Container 'Landing Zone'

      Landing Zone directory structure:
        /landing_zone/{source_name}/{slice_id}/{watermark999}
        /landing_zone/{source_name}/{slice_id}/{watermark888}
        /landing_zone/{source_name}/{slice_id}/{watermark777}
      for each slice, even if obsolete watermarks are kept in Landing, only the highest watermark will be kept and ingested into the Lake
    """

    def __init__(self, 
                 storage_account_url: str=None, 
                 container_name: str=None, 
                 container_sas: str=None):
        self.storage_account_url = get_cfg('infra.azure.storage_account.url') if storage_account_url is None else storage_account_url
        self.container_name = get_cfg('infra.azure.storage_account.container.name') if container_name is None else container_name
        self.container_sas = get_cfg('infra.azure.storage_account.container.sas') if container_sas is None else container_sas

    @abc.abstractproperty
    def source_name(self) -> str:
        """ Return the name of the Source, e.g. vendor_abc
            Will be used as Directory name in Landing Zone
        """
        pass

    @abc.abstractmethod
    def list_source_slice_id(self) -> List[str]:
        """ Return all existing slice_id in Source
        """
        pass

    @abc.abstractmethod
    def get_source_watermark(self, slice_id: str) -> str:
        """ Given a slice_id, return the watermark of Source
        """
        pass

    @abc.abstractmethod
    def download_blobs(self, slice_id: str) -> List[Tuple[str, object]]:
        """
        Return List[ (blob_file_name, file_obj) ]
        'file_obj' should be the actual downloaded file object
        """
        pass

    @property
    def container_client(self) -> ContainerClient:
        return ContainerClient(self.storage_account_url, self.container_name, self.container_sas)

    def keep_obsolete_watermarks_in_landing(self) -> bool:
        """ Whether to keep the obsolete watermarks in Landing Zone
            Note that even if the obsolete watermarks are kept in Landing Zone,
            only the highest watermark will be kept in the Lake
        """
        return True

    def query_source_slice_meta(self) -> pd.DataFrame:
        """
        Return 'what can be found in Source' pandas.DF [ slice_id:str, watermark:str ]
        both 'slice_id' and 'watermark' will be used as Directory name
        """
        col_slice_id = self.list_source_slice_id()
        with concurrent.futures.ThreadPoolExecutor(max_workers=multiprocessing.cpu_count()) as executor:
            futures = list(map(lambda slice_id: executor.submit(self.get_source_watermark, slice_id),
                               col_slice_id))
        col_watermark = list(map(lambda f: f.result(), futures))
        meta = pd.DataFrame(
            {'slice_id': col_slice_id, 'watermark': col_watermark})
        return meta

    def list_landing_slice_id(self) -> List[str]:
        return sorted([slice_id_obj.name.split('/')[-2] for slice_id_obj in
                       self.container_client.walk_blobs(name_starts_with=f'/landing_zone/{self.source_name}/',
                                                        delimiter='/')], reverse=True)

    def get_highest_landing_watermark(self, slice_id: str):
        return max(map(lambda wm_obj: wm_obj.name.split('/')[-2],
                       self.container_client.walk_blobs(name_starts_with=f'/landing_zone/{self.source_name}/{slice_id}/',
                                                        delimiter='/')))

    def query_landing_slice_meta(self) -> pd.DataFrame:
        """
        Note: only return the highest watermark for each slice_id
        Return 'what is already in Landing Zone', pandas.DF [ sclice_id:str, watermark:str ]
        both 'slice_id' and 'watermark' come from Directory name
        Azure Blob Container Directory structure: /landing_zone/{source_name}/{slice_id}/{watermark}
        """
        col_slice_id = self.list_landing_slice_id()
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=multiprocessing.cpu_count()) as executor:
            futures = list(map(lambda slice_id: executor.submit(self.get_highest_landing_watermark, slice_id),
                               col_slice_id))
        col_watermark = [ f.result() for f in futures ]

        return pd.DataFrame({'slice_id': col_slice_id, 'watermark': col_watermark})

    def pick_source_to_landing_upsert_slice_id_list(self) -> List:
        logger.debug(f'\tlooking up Source slice_meta...')
        source_slice_meta = self.query_source_slice_meta()
        logger.debug(f'\tlooking up Landing slice_meta...')
        landing_slice_meta = self.query_landing_slice_meta()
        source_slice_meta['in_source'] = True
        landing_slice_meta['in_landing'] = True

        slice_meta = source_slice_meta.merge(landing_slice_meta,
                                             on=['slice_id'], how='outer', suffixes=('_source', '_landing')) \
            .fillna({'in_landing': False, 'in_source': False})
        slice_meta['only_in_source'] = (
            slice_meta['in_source']) & (~slice_meta['in_landing'])
        slice_meta['to_upsert_bcz_existence'] = slice_meta['only_in_source']

        slice_meta['to_upsert_bcz_watermark'] = slice_meta['in_source'] & slice_meta['in_landing'] \
            & (slice_meta['watermark_source'].isna()
               | slice_meta['watermark_landing'].isna()
               | (slice_meta['watermark_source'] > slice_meta['watermark_landing']))

        slice_meta['to_upsert'] = slice_meta['to_upsert_bcz_existence'] | slice_meta['to_upsert_bcz_watermark']

        slice_meta_cols = ['slice_id', 'to_upsert',
                           'to_upsert_bcz_existence', 'to_upsert_bcz_watermark',
                           'watermark_source', 'watermark_landing']
        slice_meta = slice_meta[slice_meta_cols].sort_values(
            by=['to_upsert','slice_id'], ascending=[False,False]).reset_index(drop=True)
        logger.debug(f""" Source to Landing slice_meta: 
        { slice_meta.to_string(max_rows=20) } """)

        return sorted(slice_meta[slice_meta['to_upsert']]['slice_id'].tolist(), reverse=True)

    def delete_landing_slice(self, slice_id: str):
        slice_dir = f'/landing_zone/{self.source_name}/{slice_id}/'
        for blob in self.container_client.list_blobs(name_starts_with=slice_dir):
            self.container_client.delete_blob(blob)
        for blob in self.container_client.list_blobs(name_starts_with=slice_dir.rstrip('/')):
            self.container_client.delete_blob(blob)
        return

    def delete_landing_slice_watermark(self, slice_id: str, watermark: str):
        watermark_dir = f'/landing_zone/{self.source_name}/{slice_id}/{watermark}/'
        for blob in self.container_client.list_blobs(name_starts_with=watermark_dir):
            self.container_client.delete_blob(blob)
        for blob in self.container_client.list_blobs(name_starts_with=watermark_dir.rstrip('/')):
            self.container_client.delete_blob(blob)
        return

    def insert_landing_slice(self, slice_id: str):
        source_watermark = self.get_source_watermark(slice_id)
        self.delete_landing_slice_watermark(slice_id, source_watermark)
        for source_blob_name, source_blob_file_obj in self.download_blobs(slice_id):
            source_blob_file_obj.seek(0)
            self.container_client.upload_blob(
                f'/landing_zone/{self.source_name}/{slice_id}/{source_watermark}/{source_blob_name}',
                source_blob_file_obj)
            source_blob_file_obj.close()
        return

    def upsert_source_to_landing(self, slice_id: str):
        if not self.keep_obsolete_watermarks_in_landing:
            self.delete_landing_slice(slice_id)
        self.insert_landing_slice(slice_id)
        return

    def ingest(self, limit:int=0):
        """ copy blobs from Source to Landing Zone
        """
        logger.debug(
            f'looking up slice_id to upsert from Source to Landing...')
        slice_id_list = self.pick_source_to_landing_upsert_slice_id_list()
        
        if limit>0 and limit < len(slice_id_list):
          logger.info(f'ingesting {limit} out of total {len(slice_id_list)} ingestable slice_ids')
          slice_id_list = slice_id_list[:limit]
        
        logger.debug(f'upserting {len(slice_id_list)} slice_id from Source to Landing...')
        for slice_id in tqdm(slice_id_list):
            self.upsert_source_to_landing(slice_id)
        return

# COMMAND ----------

# DBTITLE 1,define ABC class: S3LandingBlobIngester
class S3LandingBlobIngester(LandingBlobIngester):

    def __init__(self,
                 aws_access_key_id: str,
                 aws_secret_access_key: str,
                 aws_region_name: str,
                 aws_s3_bucket_name: str,
                 **kwargs):
        super().__init__(**kwargs)
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.aws_region_name = aws_region_name
        self.aws_s3_bucket_name = aws_s3_bucket_name

    @abc.abstractmethod
    def get_s3_keys(self, slice_id: str) -> List[str]:
        """ Given a slice_id, return all the s3_keys that belong to this slice_id
        """
        pass

    def get_source_watermark(self, slice_id: str) -> str:
        """ Given a slice_id, return the max last modified UTC time of the corresponding s3_keys
        """
        s3_keys = self.get_s3_keys(slice_id)

        def get_s3_key_last_modified_utc(s3_key: str) -> str:
            return self.s3_client.head_object(Bucket=self.aws_s3_bucket_name, Key=s3_key)['LastModified']\
                .astimezone(pytz.utc).strftime('%Y%m%d_%H%M%S')

        with concurrent.futures.ThreadPoolExecutor(max_workers=multiprocessing.cpu_count()) as executor:
            futures = list(map(lambda s3_key: executor.submit(get_s3_key_last_modified_utc, s3_key),
                               s3_keys))
        return max(list(map(lambda f: f.result(), futures)))

    @property
    def s3_bucket(self):
        import boto3
        return boto3.resource(
            's3',
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
            region_name=self.aws_region_name).Bucket(self.aws_s3_bucket_name)

    @property
    def s3_client(self):
        import boto3
        return boto3.client('s3',
                            aws_access_key_id=self.aws_access_key_id,
                            aws_secret_access_key=self.aws_secret_access_key,
                            region_name=self.aws_region_name)

    def get_list_objects_paginator(self, prefix: str):
        return self.s3_client.get_paginator('list_objects').paginate(
            Bucket=self.aws_s3_bucket_name,
            Prefix=prefix,
            Delimiter='/')

    def get_common_prefixes(self, prefix: str) -> List[str]:
        return [p for p in self.get_list_objects_paginator(prefix).search('CommonPrefixes[].Prefix')]

    def exists_object_with_prefix(self, prefix: str) -> bool:
        for res in self.s3_client.get_paginator('list_objects').paginate(
            Bucket=self.aws_s3_bucket_name,
            Prefix=prefix,
            PaginationConfig={
                'MaxItems': 1
            }
        ):
            return len(res.get('Contents', [])) > 0
        return False

    def download_blobs(self, slice_id: str) -> List[Tuple[str, object]]:
        s3_keys = self.get_s3_keys(slice_id)

        def download(s3_key: str):
            blob_name = s3_key.split('/')[-1]
            file_obj = tempfile.TemporaryFile()
            self.s3_bucket.download_fileobj(s3_key, file_obj)
            return (blob_name, file_obj)
          
        with concurrent.futures.ThreadPoolExecutor(max_workers=multiprocessing.cpu_count()) as executor:
            futures = [executor.submit(download, s3_key) for s3_key in s3_keys]
        return [f.result() for f in futures]

# COMMAND ----------

# DBTITLE 1,define ABC class: GCSLandingBlobIngester
class GCSLandingBlobIngester(LandingBlobIngester):
    from google.cloud import storage

    def __init__(self,
                 gcs_bucket_name: str,
                 gcs_account_credential_json: str,
                 **kwargs):
        super().__init__(**kwargs)
        self.gcs_bucket_name = gcs_bucket_name
        self.gcs_account_credential_json = gcs_account_credential_json

    @abc.abstractmethod
    def get_object_names(self, slice_id: str) -> List[str]:
        """ Given a slice_id, return all the GCS object name that belong to this slice_id
        """
        pass

    def get_source_watermark(self, slice_id: str) -> str:
        """ Given a slice_id, return the max last modified UTC time of the corresponding gcs_keys
        """
        object_names = self.get_object_names(slice_id)

        def get_gcs_obj_last_modified_utf(object_name: str) -> str:
            return self.gcs_bucket.get_blob(object_name).updated.astimezone(pytz.utc).strftime('%Y%m%d_%H%M%S')

        with concurrent.futures.ThreadPoolExecutor(max_workers=multiprocessing.cpu_count()) as executor:
            futures = list(map(lambda object_name: executor.submit(get_gcs_obj_last_modified_utf, object_name),
                               object_names))
        return max(list(map(lambda f: f.result(), futures)))

    @property
    def gcs_client(self) -> storage.Client:
        from google.cloud import storage
        from google.oauth2 import service_account
        import json
        return storage.Client(credentials=service_account.Credentials.from_service_account_info(
            json.loads(self.gcs_account_credential_json, strict=False)
        ))

    @property
    def gcs_bucket(self):
        return self.gcs_client.bucket(self.gcs_bucket_name)

    def download_blobs(self, slice_id: str) -> List[Tuple[str, object]]:
        object_names = self.get_object_names(slice_id)

        to_ret = []
        for object_name in object_names:
            blob_name = object_name.split('/')[-1]
            file_obj = tempfile.NamedTemporaryFile()
            self.gcs_bucket.get_blob(object_name) \
                .download_to_filename(file_obj.name)
            to_ret.append((blob_name, file_obj))
        return to_ret

# COMMAND ----------

# DBTITLE 1,define ABC class: FTPLandingBlobIngester
class FTPLandingBlobIngester(LandingBlobIngester):

    def __init__(self,
                 ftp_host: str, ftp_user: str, ftp_pwd: str,
                 **kwargs):
        super().__init__(**kwargs)
        self.ftp_host = ftp_host
        self.ftp_user = ftp_user
        self.ftp_pwd = ftp_pwd

    @abc.abstractproperty
    def ftp_server_timezone(self) -> str:
        pass

    @abc.abstractmethod
    def get_file_paths(self, slice_id: str) -> List[str]:
        """ Given a slice_id, return all the FTP file paths that belong to this slice_id
        """
        pass

    def open_ftp(self):
        import ftplib
        return ftplib.FTP(host=self.ftp_host,
                          user=self.ftp_user,
                          passwd=self.ftp_pwd)

    def get_source_watermark(self, slice_id: str) -> str:
        """ Given a slice_id, return the max last modified UTC time of the corresponding files
        """
        file_paths = self.get_file_paths(slice_id)

        def get_file_last_modified_utc(file_path: str) -> str:
            import dateutil.parser as dp
            with self.open_ftp() as ftp:
                return pytz.timezone(self.ftp_server_timezone) \
                    .localize(dp.parse(ftp.voidcmd(f'MDTM {file_path}')[4:])) \
                    .astimezone(pytz.utc).strftime('%Y%m%d_%H%M%S')

        with concurrent.futures.ThreadPoolExecutor(max_workers=multiprocessing.cpu_count()) as executor:
            futures = list(map(lambda file_path: executor.submit(get_file_last_modified_utc, file_path),
                               file_paths))
        return max(list(map(lambda f: f.result(), futures)))

    def download_blobs(self, slice_id: str) -> List[Tuple[str, object]]:
        """
        Return List[ (blob_file_name, file_obj) ]
        'file_obj' should be the actual downloaded file object
        """
        to_ret = []
        for file_path in self.get_file_paths(slice_id):
            file_obj = tempfile.TemporaryFile()
            with self.open_ftp() as ftp:
                ftp.retrbinary(f'RETR {file_path}', file_obj.write)
            to_ret.append((file_path, file_obj))
        return to_ret

# COMMAND ----------

# DBTITLE 1,define ABC class: NASLandingBlobIngester
class NASLandingBlobIngester(LandingBlobIngester):
    from smb.SMBConnection import SMBConnection

    def __init__(self,
                 nas_host: str, nas_port: str,
                 nas_uname: str, nas_pwd: str,
                 nas_my_name: str, nas_remote_name: str,
                 nas_service_name: str,
                 **kwargs):
        super().__init__(**kwargs)
        self.nas_host, self.nas_port = nas_host, nas_port
        self.nas_uname, self.nas_pwd = nas_uname, nas_pwd
        self.nas_my_name, self.nas_remote_name = nas_my_name, nas_remote_name
        self.nas_service_name = nas_service_name

    @abc.abstractmethod
    def get_file_paths(self, slice_id: str) -> List[str]:
        """ Given a slice_id, return all the NAS file paths that belong to this slice_id
        """
        pass

    def get_source_watermark(self, slice_id: str) -> str:
        """ Given a slice_id, return the max last modified UTC time of the corresponding files
        """
        filepaths = self.get_file_paths(slice_id)

        def get_last_write_utc(filepath: str) -> str:
            with self.open_smb_conn() as conn:
                return datetime.fromtimestamp(conn.getAttributes(self.nas_service_name, filepath).last_write_time,
                                              tz=pytz.utc).strftime('%Y%m%d_%H%M%S')

        with concurrent.futures.ThreadPoolExecutor(max_workers=multiprocessing.cpu_count()) as executor:
            futures = list(map(lambda filepath: executor.submit(get_last_write_utc, filepath),
                               filepaths))
        return max([f.result() for f in futures])

    def download_blobs(self, slice_id: str) -> List[Tuple[str, object]]:
        filepaths = self.get_file_paths(slice_id)

        to_ret = []
        for filepath in filepaths:
            filename = filepath.split('/')[-1]
            with self.open_smb_conn() as conn:
                file_obj = tempfile.TemporaryFile()
                conn.retrieveFile(self.nas_service_name, filepath, file_obj)
            to_ret.append((filename, file_obj))
        return to_ret

    def open_smb_conn(self) -> SMBConnection:
        from smb.SMBConnection import SMBConnection
        conn = SMBConnection(
            username=self.nas_uname,
            password=self.nas_pwd,
            my_name=self.nas_my_name,
            remote_name=self.nas_remote_name,
            use_ntlm_v2=True)
        conn.connect(self.nas_host, self.nas_port)
        return conn

# COMMAND ----------

# DBTITLE 1,define ABC class: AzureFileShareBlobIngester
class AzureFileShareBlobIngester(LandingBlobIngester):
    from azure.storage.fileshare import ShareClient

    def __init__(self,
                 afs_storage_account_name: str, afs_fileshare_name: str, afs_fileshare_sas: str,
                 **kwargs):
        super().__init__(**kwargs)
        self.afs_storage_account_name = afs_storage_account_name
        self.afs_fileshare_name = afs_fileshare_name
        self.afs_fileshare_sas = afs_fileshare_sas

    @abc.abstractmethod
    def get_file_paths(self, slice_id: str) -> List[str]:
        """ Given a slice_id, return all the NAS file paths that belong to this slice_id
        """
        pass

    @abc.abstractmethod
    def list_source_slice_id(self) -> List[str]:
        pass

    @property
    def afs_share_client(self) -> ShareClient:
        from azure.storage.fileshare import ShareClient
        return ShareClient(
            f"https://{self.afs_storage_account_name}.file.core.windows.net",
            self.afs_fileshare_name,
            credential=self.afs_fileshare_sas)

    def download_blobs(self, slice_id: str) -> List[Tuple[str, object]]:
        filepaths = self.get_file_paths(slice_id)

        to_ret = []
        for filepath in filepaths:
            blobname = f"{slice_id}_{filepath.split('/')[-1]}"
            file_obj = tempfile.TemporaryFile()
            file_obj.write(self.afs_share_client.get_file_client(
                filepath).download_file().readall())
            to_ret.append((blobname, file_obj))
        return to_ret

    def get_source_watermark(self, slice_id: str) -> str:
        """ Given a slice_id, return the max last modified UTC time of the corresponding gcs_keys
        """
        filepaths = self.get_file_paths(slice_id)

        def get_afs_file_last_modified_utf(filepath: str) -> str:
            return self.afs_share_client.get_file_client(filepath).get_file_properties().last_modified.astimezone(pytz.utc).strftime('%Y%m%d_%H%M%S')

        with concurrent.futures.ThreadPoolExecutor(max_workers=multiprocessing.cpu_count()) as executor:
            futures = list(map(lambda filepath: executor.submit(get_afs_file_last_modified_utf, filepath),
                               filepaths))
        return max([f.result() for f in futures])

# COMMAND ----------

# DBTITLE 1,define ABC class: TheTradeDeskLandingBlobIngester
class TheTradeDeskLandingBlobIngester(S3LandingBlobIngester, ABC):
    """
      S3 pattern ( reference https://api.thetradedesk.com/v3/portal/reds/doc/REDSAmazonS3 ):
           s3://{s3_bucket_name}/{ParternId]/redf5/date={yyyy-mm-dd}/hour={H}/{EventType}_{PartnerId}_{uuid stuff}.log.gz
      e.g. s3://thetradedesk-useast-partner-datafeed/z4krsrk/redf5/date=2022-06-15/hour=8/videoevents_z4krsrk_V5_1_2022-06-15T005938_2022-06-15T005959_2022-06-15T011347_a985f2e04627f65c6e91548005b5bc4f.log.gz

      # TTD will scrub data after ~99 days, so we discard REDs older than 90 days
      # https://api.thetradedesk.com/v3/portal/reds/doc/REDSDataPrivacyRetention#data-scrub

      slice_id: %Y%m%d_%H, e.g. 20220515_08. Ignoring the last 6 hours in case more .log.gz files are being inserted to S3.
      watermark: the same as slice_id, based on 'the Big Assumption'
    """

    def __init__(self,
                 aws_access_key_id: str,
                 aws_secret_access_key: str,
                 aws_region_name: str,
                 aws_s3_bucket_name: str,
                 **kwargs):
        super().__init__(aws_access_key_id, aws_secret_access_key,
                         aws_region_name, aws_s3_bucket_name,
                         **kwargs)
        self._cached_s3_key_last_modified_utc = None

    @abc.abstractproperty
    def event_type(self) -> str:
        """ must be one of these: { impressions, clicks, videoevents, conversions } """
        pass

    @abc.abstractproperty
    def partner_id(self) -> str:
        pass

    @property
    def source_name(self) -> str:
        return f'thetradedesk_{self.event_type}'

    @property
    def s3_key_pattern(self) -> str:
        return '^'+self.partner_id+'/redf5/date=([0-9]{4})-([0-9]{2})-([0-9]{2})/hour=([0-9]{1,2})/'+self.event_type+'_'+self.partner_id+'_.*.log.gz'

    @property
    def cutoff_days(self) -> int:
        """ stop reading REDs data older than 'cutoff' days """
        return 28

    @property
    def cached_s3_key_last_modified_utc(self) -> List[Tuple[str, str]]:
        if self._cached_s3_key_last_modified_utc is None:
            from datetime import timedelta
            utc_date = datetime.utcnow().date()
            cutoff_yyyymmdd = (utc_date - timedelta(days=self.cutoff_days)
                               ).strftime('%Y%m%d')

            def is_eligible_yyyymmdd(yyyymmdd_prefix: str):
                m = re.match(
                    '^'+self.partner_id+'/redf5/date=([0-9]{4})-([0-9]{2})-([0-9]{2})/$', yyyymmdd_prefix)
                if m:
                    yyyy, mm, dd = m.group(1), m.group(2), m.group(3)
                    return f'{yyyy}{mm}{dd}' >= cutoff_yyyymmdd
                else:
                    return False

            yyyymmdd_prefixes = [p for p in self.get_common_prefixes(f'{self.partner_id}/redf5/')
                                 if is_eligible_yyyymmdd(p)]

            import itertools
            yyyymmddh_prefixes = list(itertools.chain.from_iterable([[f'{yyyymmdd_prefix}hour={h}/' for h in range(0, 24)]
                                                                     for yyyymmdd_prefix in yyyymmdd_prefixes]))

            def list_s3_key_last_modified_utc(yyyymmddh_prefix: str) -> List[Tuple[str, str]]:
                return [(o.key,
                         o.last_modified.astimezone(pytz.utc).strftime('%Y%m%d_%H%M%S'))
                        for o in self.s3_bucket.objects.filter(Prefix=yyyymmddh_prefix+self.event_type)
                        if re.match(self.s3_key_pattern, o.key)]

            with concurrent.futures.ThreadPoolExecutor(max_workers=multiprocessing.cpu_count() * 4) as executor:
                futures = list(map(lambda yyyymmddh_prefix: executor.submit(list_s3_key_last_modified_utc, yyyymmddh_prefix),
                                   yyyymmddh_prefixes))
            self._cached_s3_key_last_modified_utc = list(
                itertools.chain.from_iterable([f.result() for f in futures]))

        return self._cached_s3_key_last_modified_utc

    @property
    def cached_s3_keys(self) -> List[str]:
        return [kv[0] for kv in self.cached_s3_key_last_modified_utc]

    def list_source_slice_id(self) -> List[str]:
        """ Return all existing slice_id in Source
        """
        def convert_s3_key_to_slice_id(s3_key):
            m = re.match(self.s3_key_pattern, s3_key)
            yyyy, mm, dd, h = m.group(1), m.group(2), m.group(3), m.group(4)
            slice_id = f'{yyyy}{mm}{dd}_{h.zfill(2)}'
            return slice_id

        return sorted(list({convert_s3_key_to_slice_id(s3_key) for s3_key in self.cached_s3_keys}), reverse=True)

    def get_s3_keys(self, slice_id: str) -> List[str]:
        yyyy, mm, dd, h = \
            slice_id[0:4], slice_id[4:6], slice_id[6:8], int(slice_id[9:11])

        prefix = f'{self.partner_id}/redf5/date={yyyy}-{mm}-{dd}/hour={h}/'
        return [s3_key
                for s3_key in self.cached_s3_keys
                if s3_key.startswith(prefix)]

    def get_source_watermark(self, slice_id: str) -> str:
        yyyy, mm, dd, h = \
            slice_id[0:4], slice_id[4:6], slice_id[6:8], int(slice_id[9:11])
        prefix = f'{self.partner_id}/redf5/date={yyyy}-{mm}-{dd}/hour={h}/'
        return max(map(lambda kv: kv[1], filter(lambda kv: kv[0].startswith(prefix), self.cached_s3_key_last_modified_utc)))

# COMMAND ----------

# DBTITLE 1,define ABC class: AzureBlobsLandingBlobIngester
class AzureBlobsLandingBlobIngester(LandingBlobIngester):
    """ ingest from external 'Source' Azure Blob into Lake Landing Zone """

    def __init__(self,
                 lake_sa_url: str, lake_container_name: str, lake_container_sas: str,
                 source_sa_url: str, source_container_name: str, source_container_sas: str):
        self.source_sa_url, self.source_container_name, self.source_container_sas = \
            source_sa_url, source_container_name, source_container_sas

        super().__init__(lake_sa_url, lake_container_name, lake_container_sas)

    @abc.abstractproperty
    def source_name(self) -> str:
        pass

    @abc.abstractmethod
    def list_source_slice_id(self) -> List[str]:
        pass

    @abc.abstractmethod
    def get_blob_directory_path(self, slice_id: str) -> str:
        pass

    @property
    def source_container_client(self) -> ContainerClient:
        return ContainerClient(self.source_sa_url, self.source_container_name, self.source_container_sas)

    def get_blob_names(self, slice_id: str) -> List[str]:
        """ Given a slice_id, return all the blob names that belong to this slice_id
        """
        dir = self.get_blob_directory_path(slice_id)
        return [b.name
                for b in self.source_container_client.walk_blobs(name_starts_with=dir, delimiter='/')]

    def get_source_watermark(self, slice_id: str) -> str:
        """ Given a slice_id, return the max last modified UTC time of the corresponding Azure Blobs
        """
        dir = self.get_blob_directory_path(slice_id)
        return self.source_container_client.get_blob_client(dir).get_blob_properties() \
            .last_modified.astimezone(pytz.utc).strftime('%Y%m%d_%H%M%S')

    def download_blobs(self, slice_id: str) -> List[Tuple[str, object]]:
        blob_names = self.get_blob_names(slice_id)

        to_ret = []
        for blob_name in blob_names:
            bn = blob_name.split('/')[-1]
            file_obj = tempfile.TemporaryFile()
            file_obj.write(self.source_container_client.get_blob_client(blob_name)
                           .download_blob().readall())
            to_ret.append((bn, file_obj))
        return to_ret

# COMMAND ----------

# DBTITLE 1,define ABC class: SharepointLandingBlobIngester
class SharepointLandingBlobIngester(LandingBlobIngester):
    """ ingest from Sharepoint into Lake Landing Zone """
    from office365.sharepoint.files.file import File as SharepointFile

    def __init__(self,
                 sp_site_url: str, sp_client_id: str, sp_client_secret: str,
                 **kwargs):
        """ 
            sp_client_id/sp_client_serect need to have 'FullControl' for 'web' of the SharePoint Site.
            To produce client_id/client_secret: https://promotastic.atlassian.net/wiki/spaces/~459080946/pages/3037200930/Crash+Course+for+SharePoint+Access
        """
        super().__init__(**kwargs)
        self.sp_site_url, self.sp_client_id, self.sp_client_secret = \
            sp_site_url, sp_client_id, sp_client_secret

    @abc.abstractproperty
    def source_name(self) -> str:
        pass

    @abc.abstractmethod
    def list_source_slice_id(self) -> List[str]:
        pass

    @abc.abstractmethod
    def get_sp_files(self, slice_id: str) -> List[SharepointFile]:
        """ Given a slice_id, return all the SharePoint Files that belong to this slice_id
        """
        pass

    @property
    def sp_client_context(self):
        from office365.sharepoint.client_context import ClientContext
        from office365.runtime.auth.client_credential import ClientCredential
        return ClientContext(self.sp_site_url).with_credentials(ClientCredential(self.sp_client_id, self.sp_client_secret))

    def get_source_watermark(self, slice_id: str) -> str:
        """ Given a slice_id, return the max last modified UTC time of the corresponding Azure Blobs
        """
        files = self.get_sp_files(slice_id)
        import dateutil.parser as dp
        return max([dp.parse(f.properties['TimeLastModified']).astimezone(pytz.utc).strftime('%Y%m%d_%H%M%S')
                    for f in files])

    def download_blobs(self, slice_id: str) -> List[Tuple[str, object]]:
        files = self.get_sp_files(slice_id)

        to_ret = []
        for f in files:
            blob_name = f.name
            file_obj = tempfile.TemporaryFile()
            self.sp_client_context.web.get_file_by_server_relative_url(
                f.serverRelativeUrl).download(file_obj).execute_query()
            to_ret.append((blob_name, file_obj))

        return to_ret

# COMMAND ----------

# DBTITLE 1,define concrete class: CompositeLandingBlobIngester
class CompositeLandingBlobIngester(LandingBlobIngester):
  from typing import Dict
  """
    ingesters: the first 'ingester' has the highest priority 
  """
  def __init__(self, ingesters:List[LandingBlobIngester]):
    if len( { (i.source_name, i.storage_account_url, i.container_name, i.container_sas) for i in ingesters } ) != 1:
      raise RuntimeError(f'all "ingesters" must have the same (source_name,storage_account_url,container_name,container_sas)')
    
    super().__init__( ingesters[0].storage_account_url, ingesters[0].container_name, ingesters[0].container_sas )
    
    self.ingesters = ingesters
    self._ingester_cached_source_slice_meta_dict = None
  
  @property
  def source_name(self) -> str:
    return self.ingesters[0].source_name
  
  @property
  def ingester_cached_source_slice_meta_dict(self) -> Dict[LandingBlobIngester, pd.DataFrame]:
    if self._ingester_cached_source_slice_meta_dict is None:
      with concurrent.futures.ThreadPoolExecutor(max_workers=multiprocessing.cpu_count()) as executor:
        slice_meta_futures = [ executor.submit( ingester.query_source_slice_meta ) for ingester in self.ingesters]
      slice_metas = [ f.result() for f in slice_meta_futures ]
      self._ingester_cached_source_slice_meta_dict = dict( zip( self.ingesters, slice_metas ) )
    
    return self._ingester_cached_source_slice_meta_dict
  
  def choose_ingester(self, slice_id) -> LandingBlobIngester:
    """ given a slice_id, choose which ingester to use """
    
    def convert_to_comparator( ingester, slice_meta ):
      slice_id_exists_in_this_ingester = int( slice_id in slice_meta['slice_id'].tolist() )
      priority_of_this_ingester =  len(self.ingesters) - self.ingesters.index( ingester )
      return ( slice_id_exists_in_this_ingester, priority_of_this_ingester, ingester )
    
    chosen = max([ convert_to_comparator(ingester,slice_meta) for ingester, slice_meta in self.ingester_cached_source_slice_meta_dict.items() ])
    return chosen[2]
  
  def list_source_slice_id(self) -> List[str]:
    import itertools
    return sorted( list( set( itertools.chain.from_iterable( source_meta['slice_id'] for source_meta in self.ingester_cached_source_slice_meta_dict.values() ) ) ), reverse=True )
  
  def get_source_watermark(self, slice_id: str) -> str:
    chosen_ingester = self.choose_ingester(slice_id)
    chosen_slice_meta = self.ingester_cached_source_slice_meta_dict[chosen_ingester]
    return chosen_slice_meta.loc[ chosen_slice_meta['slice_id']==slice_id, 'watermark' ].iloc[0]
  
  def download_blobs(self, slice_id: str) -> List[Tuple[str, object]]:
    chosen_ingester = self.choose_ingester(slice_id)
    return chosen_ingester.download_blobs(slice_id)
