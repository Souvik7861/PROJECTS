import logging.config
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import *
from udf import *

logging.config.fileConfig('Properties\Configuration\logging.config')
loggers = logging.getLogger('Data_transformation')


def data_report1(df_city_sel, df_presc_sel):
    try:
        loggers.warning("Processing data_report1 method...")
        loggers.warning("Calculating total zip counts in {}".format(df_city_sel))

        df_city_split = df_city_sel.withColumn('zipscounts', column_split_count(df_city_sel.zips))

        loggers.warning("Calculating distinct prescribers and total tx_count...")

        df_presc_grp = df_presc_sel.groupBy(df_presc_sel.presc_city, df_presc_sel.presc_state) \
            .agg(countDistinct('presc_id').alias('presc_counts'), sum('tx_cnt').alias('tx_count'))

        loggers.warning(
            "Don't report city if no prescriber assigned to it...therefore let's join df_city_sel and df_presc_sel ")

        df_city_join = df_city_split.join(df_presc_grp, (df_city_sel.state_id == df_presc_grp.presc_state) & (
                    df_city_sel.city == df_presc_grp.presc_city), 'inner')

        df_final = df_city_join.select('city', 'state_name', 'county_name', 'population', 'zipscounts', 'presc_counts')

    except Exception as e:
        loggers.error("An error occured in data_report1...", str(e))
        raise
    else:
        loggers.warning('data_report1 successfully executed...')

    return df_final


def data_report2(df_presc_sel):
    try:
        loggers.warning("Executing data_report2 method...")
        loggers.warning("Executing the task::: vonsider the prescriber only from 20 to 50 years_of_exp and"
                        "rank the prescribers based on their tx_cnt for each state")

        windowSpec = Window.partitionBy("presc_state").orderBy(col('tx_cnt').desc())

        df_presc_report = df_presc_sel.select('presc_id', 'presc_fullname', 'presc_state', 'Country_name',
                                              'years_of_exp', 'tx_cnt', 'total_day_supply', 'total_drug_cost') \
            .filter((df_presc_sel.years_of_exp >= 20) & (df_presc_sel.years_of_exp <= 50)) \
            .withColumn("dense_rank", dense_rank().over(windowSpec)).filter(col("dense_rank") <= 5)

    except Exception as e:
        loggers.error("An error occurred while processing data_report2 method:::", str(e))
        raise
    else:
        loggers.warning("data_report2 method Executed , go frwd...")

    return df_presc_report


