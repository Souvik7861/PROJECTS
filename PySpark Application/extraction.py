import logging.config
from pyspark.sql.functions import *
from pyspark.sql.types import *

logging.config.fileConfig('Properties/configuration/logging.config')
loggers = logging.getLogger('Extraction')


def extract_files(df, format, filepath, split_no, headerReq, compressionType):
    try:
        loggers.warning("Executing extract_files method...")
        # split_no is number of split files...
        df.coalesce(split_no).write.mode("overwrite").format(format).save(filepath,header=headerReq,
                                                                          compression=compressionType)

    except Exception as e:
        loggers.error("An error occurred at extract_files method:::",str(e))
        raise
    else:
        loggers.warning("extract_files method successfully executed...")
