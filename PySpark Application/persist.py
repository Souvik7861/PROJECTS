import logging.config
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime as date

logging.config.fileConfig('Properties/configuration/logging.config')
loggers = logging.getLogger('Persist')


def data_hive_persist(spark,df,dfname,partitionBy,mode):
    try:
        loggers.warning("Persisting the data into Hive Table for {}".format(dfname))
        loggers.warning("Creating Database")

        spark.sql(""" create database if not exists cities """)
        spark.sql(""" use cities """)

        loggers.warning("Now writing {} into Hive_table by {} ".format(df,partitionBy))

        df.write.saveAsTable(dfname,partitionBy=partitionBy,mode=mode)

    except Exception as e:
        loggers.error("An error occurred in data_hive_persist method:::",str(e))
        raise
    else:
        loggers.warning('Data Successfully persisted into Hive_table...')


def data_hive_persist_presc(spark,df,dfname,partitionBy,mode):
    try:
        loggers.warning("Persisting the data into Hive Table for {}".format(dfname))
        loggers.warning("Creating Database")

        spark.sql(""" create database if not exists presc """)
        spark.sql(""" use presc """)

        loggers.warning("Now writing {} into Hive_table by {} ".format(df,partitionBy))

        df.write.saveAsTable(dfname,partitionBy=partitionBy,mode=mode)

    except Exception as e:
        loggers.error("An error occurred in data_hive_persist method:::",str(e))
        raise
    else:
        loggers.warning('Data Successfully persisted into Hive_table...')


def persist_data_mysql(spark,df,dfname,url,dbtable,mode,user,password):
    try:
        loggers.warning('Executing persist_data_mysql method with {}'.format(dfname))

        df.write.format('jdbc').option("url",url).option("dbtable",dbtable) \
            .mode(mode).option("user",user).option("password",password).save()

    except Exception as e:
        loggers.error("An error occurred in persist_data_mysql method:::", str(e))
        raise
    else:
        loggers.warning('Data Successfully persisted into MySQL...')
