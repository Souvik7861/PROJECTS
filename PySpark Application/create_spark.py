import logging.config

from pyspark.sql import SparkSession

logging.config.fileConfig('Properties/Configuration/logging.config')

logger = logging.getLogger('Create_spark')


def get_spark_object(envn, appName):
    try:
        logger.info('get_spark_object method started')
        if envn == 'DEV':
            master = 'local'
        else:
            master = 'Yarn'

        logger.info('master is {}'.format(master))

        spark = SparkSession.builder \
                 .master(master) \
                 .appName(appName) \
                 .enableHiveSupport() \
                 .config('spark.driver.extraclasspath','mysql-connector-j-8.0.31.jar') \
                 .getOrCreate()

        return spark

    except Exception as exp:
        logger.error('An error occured in get_spark_object====',str(exp))
        raise
    else:
        logger.info('Spark object created....')
    return spark
