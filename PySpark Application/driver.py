import os
import sys
from time import perf_counter

import get_env_variables
import get_env_variables as gav
from create_spark import get_spark_object
from validate import get_current_date , print_schema, check_for_nulls
from ingest import load_files, display_df, df_count
from data_processing import *
from data_transformation import *
from extraction import *
from persist import *
import logging
import logging.config

logging.config.fileConfig('Properties/configuration/logging.config')
start_time = perf_counter()


def main():
    global file_format, file_dir, header, inferSchema, file_dir2
    try:
        logging.info('i am in the main method..')
        # print(gav.header)
        # print(gav.src_olap)
        logging.info('calling spark object')
        spark = get_spark_object(gav.envn, gav.appName)

        logging.info('Validating spark object..........')
        get_current_date(spark)

        for file in os.listdir(gav.src_olap):
            print("File is :", file)

            file_dir = gav.src_olap + '\\' + file
            print(file_dir)

            if file.endswith('.parquet'):
                file_format = 'parquet'
                header = 'NA'
                inferSchema = 'NA'

            elif file.endswith('.csv'):
                file_format = 'csv'
                header = gav.header
                inferSchema = gav.inferSchema
        logging.info('reading file which is of > {}'.format(file_format))
        df_city = load_files(spark=spark, file_dir=file_dir, file_format=file_format, header=header,
                             inferSchema=inferSchema)

        logging.info("displaying file df_city")
        display_df(df_city, 'df_city')

        logging.info("here to validate the df")

        print('df_city row count :',df_count(df_city, 'df_city'))

        logging.info('checking for the files in the Fact...')

        for files in os.listdir(gav.src_oltp):
            print("Src Files::" + files)

            file_dir = gav.src_oltp + '\\' + files
            print(file_dir)

            if files.endswith('.parquet'):
                file_format = 'parquet'
                header = 'NA'
                inferSchema = 'NA'

            elif files.endswith('.csv'):
                file_format = 'csv'
                header = gav.header
                inferSchema = gav.inferSchema
        logging.info('reading file which is of > {}'.format(file_format))

        df_fact = load_files(spark=spark, file_dir=file_dir, file_format=file_format, header=header,
                             inferSchema=inferSchema)

        logging.info('displaying the df_fact dataframe')
        display_df(df_fact, 'df_fact')

        # validate::
        print('df_fact row count :',df_count(df_fact, 'df_fact'))

        logging.info("implementing data_processing methods...")

        df_city_sel, df_presc_sel = data_clean(df_city, df_fact)

        display_df(df_city_sel, 'df_city')

        display_df(df_presc_sel, 'df_fact')

        logging.info("validating schema for the dataframes....")

        print_schema(df_city_sel, 'df_city_sel')

        print_schema(df_presc_sel, 'df_presc_Sel')

        check_df = check_for_nulls(df_presc_sel,'df_fact')

        display_df(check_df,'df_fact')

        logging.info("data transformation executed...")

        df_report_1 = data_report1(df_city_sel, df_presc_sel)

        logging.info("displaying the df_report_1")

        display_df(df_report_1,'data_report')

        logging.info("Displaying data_report2 method....")

        df_report_2 = data_report2(df_presc_sel)

        display_df(df_report_2,"data_report_2")

        logging.info("extracting files to output...")

        city_path = gav.city_path
        extract_files(df_report_1,'orc',city_path,1,False,'snappy')

        presc_path = gav.presc_path
        extract_files(df_report_2,'parquet',presc_path,2,False,'snappy')

        logging.info("Extracting files to output completed...")

        logging.info("Writing data into Hive_table")

        data_hive_persist(spark=spark,df=df_report_1,dfname='df_city',partitionBy='state_name',mode='append')
        data_hive_persist_presc(spark=spark,df=df_report_2,dfname='df_presc',partitionBy='presc_state',mode='append')

        logging.info("Successfully written into Hive")

        loggers.info("Now writing {} into MySQL ".format(df_report_1))

        persist_data_mysql(spark=spark,df=df_report_1,dfname='df_city',url="jdbc:mysql://localhost:3306/datapipeline",
                           dbtable='city_df',mode='append',user= gav.user,password=gav.password)

        logging.info("Successfully Data written into Mysql")

    except Exception as exp:
        logging.error("An error occurred when calling main() please check the trace=== ", str(exp))
        sys.exit(1)


if __name__ == '__main__':
    main()
    end_time = perf_counter()
    logging.info("Total amount of time taken : {:.2f} seconds".format(end_time-start_time))
    logging.info('Application done')
