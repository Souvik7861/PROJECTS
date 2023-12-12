from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging.config
import sys

logging.config.fileConfig('Properties\Configuration\logging.config')

loggers = logging.getLogger('Validate')


def check_for_nulls(df, dfName):
    try:
        loggers.info("check for nulls method executing.......for {}".format(dfName))
        #check_null_df = df.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in df.columns])
        check_null_df = df.summary("count")

    except Exception as e:
        loggers.error('An error occur while working on check_for_nulls===', str(e))
    else:
        loggers.warning('Check_for_nulls executed successfully...')
    return check_null_df


def get_current_date(spark):
    try:
        loggers.warning('started the get_current_date method...')
        output = spark.sql("""select current_date""")
        loggers.warning('Validating spark object by current date : ' + str(output.collect()))
    except Exception as exp:
        loggers.error('An error occurred in get_current_date', str(exp))

        raise
    else:
        loggers.warning('Validation done go frwd...')


def print_schema(df,dfName):
    try:
        loggers.warning('print_schema method executing...{} '.format(dfName))

        sch = df.schema.fields

        for i in sch:
            loggers.info(f"\t{i}")

    except Exception as e:
        loggers.error("An error in print_schema::",str(e))
        raise
    else:
        loggers.info("print_schema done, go frwd...")
