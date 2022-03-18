import config
import yaml
from yaml.loader import SafeLoader

MOUNT_POINT = "/mnt/blob"
READ_PATH = "/dbfs" + MOUNT_POINT

def ds_init_spark_and_dbutils():
    if not config.LOCAL:
        from pyspark.sql import SparkSession
        from pyspark.dbutils import DBUtils
        def get_dbutils(spark):
            try:
                dbutils = DBUtils(spark)
            except ImportError:
                import IPython
                dbutils = IPython.get_ipython().user_ns["dbutils"]
            return dbutils
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.appName('abc').getOrCreate()
        dbutils = get_dbutils(spark)
        print('spark session init')
        return spark, dbutils
    else:
        return None, None

def ds_init_mount_ststagingmodel(dbutils, st_secrets_path):
    with open(st_secrets_path) as f:
        blob_secrets = yaml.load(f, Loader=SafeLoader)
    
    def unmount_if_exists(str_path):
        if any(mount.mountPoint == str_path for mount in dbutils.fs.mounts()):
            dbutils.fs.unmount(str_path)
    unmount_if_exists(MOUNT_POINT)

    dbutils.fs.mount(
        source = "wasbs://" + blob_secrets['container'] + "@" + blob_secrets['storage'] + ".blob.core.windows.net",
        mount_point = MOUNT_POINT,
        extra_configs = {
            "fs.azure.account.key." + blob_secrets['storage'] + ".blob.core.windows.net": blob_secrets['key']})

def ds_init_run():
    if not config.LOCAL:
        spark, dbutils = ds_init_spark_and_dbutils()
        print('MOUNT_PATH: ' + str(MOUNT_POINT))
        print('READ_PATH: ' + str(READ_PATH))
        return spark, dbutils
    else:
        return None, None
