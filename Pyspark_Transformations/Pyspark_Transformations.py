# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import *
from datetime import datetime

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.Monthly Percentage Difference 
# MAGIC
# MAGIC Q. Given a table of purchases by date, calculate the month-over-month percentage change in revenue. The output should include the year-month date (YYYY-MM) and percentage change, rounded to the 2nd decimal point, and sorted from the beginning of the year to the end of the year.
# MAGIC The percentage change column will be populated from the 2nd month forward and can be calculated as ((this month's revenue - last month's revenue) / last month's revenue)*100.    
# MAGIC [link](https://platform.stratascratch.com/coding/10319-monthly-percentage-difference?code_type=3)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Spark-Dataframe API

# COMMAND ----------

sf_transactions = spark.read.format('csv')\
                        .option('header','true')\
                        .option('inferschema','true')\
                        .option('mode','PERMISSIVE')\
                        .load('/FileStore/tables/one.csv')

sf_transactions.show(5)
sf_transactions.printSchema()

# COMMAND ----------

result_df = sf_transactions.select(
            date_format(col("created_at"), "yyyy-MM").alias('ym'),col('value'))\
            .groupBy('ym').agg(sum('value').alias('sum(value)'))\
            .withColumn('pre', lag('sum(value)', 1).over(Window.orderBy('ym')))\
            .withColumn('revenue_diff_pct',round(((col('sum(value)')-col('pre'))/col('pre'))*100,2))\
            
result_df.select('ym','revenue_diff_pct').show()

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Spark-SQL

# COMMAND ----------

sf_transactions.createOrReplaceTempView('sf_transactions_table')

# COMMAND ----------

spark.sql("""
select ym,round(((pre-value)/pre)*100,2) as revenue_diff_pct
from 
    (select * , lag(T.value) over(order by ym) as pre
    from 
        (select 
        DATE_FORMAT(created_at,"yyyy-MM") as ym,
        sum(value) as value
        from 
        sf_transactions_table
        group by ym 
        ) AS T
    ) as U
          """).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Top Percentile Fraud
# MAGIC
# MAGIC Q. ABC Corp is a mid-sized insurer in the US and in the recent past their fraudulent claims have increased significantly for their personal auto insurance portfolio. They have developed a ML based predictive model to identify propensity of fraudulent claims. Now, they assign highly experienced claim adjusters for top 5 percentile of claims identified by the model.
# MAGIC Your objective is to identify the top 5 percentile of claims from each state. Your output should be policy number, state, claim cost, and fraud score.  
# MAGIC [link](https://platform.stratascratch.com/coding/10303-top-percentile-fraud?code_type=3)
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### Spark-Dataframe API

# COMMAND ----------

fraud_score_df = spark.read.format('csv')\
                        .option('header','true')\
                        .option('inferschema','true')\
                        .option('mode','PERMISSIVE')\
                        .load('/FileStore/tables/two.csv')

fraud_score_df.show(5)
fraud_score_df.printSchema()

# COMMAND ----------

fraud_score_df.withColumn('rank',rank().over(Window.partitionBy(col('state'))\
                .orderBy(col('fraud_score').desc())))\
                .withColumn('rnk_filter',ceil(max(col('rank')).over(Window.partitionBy(col('state')))*0.05))\
                .filter(col('rank')<=col('rnk_filter'))\
                .select('policy_num','state','claim_cost','fraud_score').show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Spark_SQL

# COMMAND ----------

fraud_score_df.createOrReplaceTempView('fraud_score_Table')

# COMMAND ----------

spark.sql("""
        select 
        policy_num,
        state,
        claim_cost,
        fraud_score
        from (  select * ,
            ceil(((count(*) over(partition by state))*0.05)) as select_rnk ,
            rank() over(partition by state order by fraud_score desc) as rnk
            from fraud_score_Table
            ) as T
        where rnk <= select_rnk 
          """).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Premium vs Freemium
# MAGIC
# MAGIC Q. Find the total number of downloads for paying and non-paying users by date. Include only records where non-paying customers have more downloads than paying customers. The output should be sorted by earliest date first and contain 3 columns date, non-paying downloads, paying downloads.    
# MAGIC [link](https://platform.stratascratch.com/coding/10300-premium-vs-freemium?code_type=3)
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### Spark-Dataframe API

# COMMAND ----------

ms_user_dimension_df = spark.read.format('csv')\
                        .option('header','true')\
                        .option('inferschema','true')\
                        .option('mode','PERMISSIVE')\
                        .load('/FileStore/tables/3_one.csv')

ms_acc_dimension_df =   spark.read.format('csv')\
                        .option('header','true')\
                        .option('inferschema','true')\
                        .option('mode','PERMISSIVE')\
                        .load('/FileStore/tables/3_two.csv')

ms_download_facts =     spark.read.format('csv')\
                        .option('header','true')\
                        .option('inferschema','true')\
                        .option('mode','PERMISSIVE')\
                        .load('/FileStore/tables/3_three.csv')

ms_user_dimension_df.show(5)
ms_user_dimension_df.printSchema()

ms_acc_dimension_df.show(5)
ms_acc_dimension_df.printSchema()

ms_download_facts.show(5)
ms_download_facts.printSchema()

# COMMAND ----------

ms_download_facts.join(ms_user_dimension_df ,ms_download_facts.user_id == ms_user_dimension_df.user_id, 'left' )\
        .join(ms_acc_dimension_df , ms_user_dimension_df.acc_id == ms_acc_dimension_df.acc_id, 'left')\
        .select('date',ms_download_facts.user_id,'downloads',ms_user_dimension_df.acc_id,'paying_customer')\
        .orderBy(col('date').asc())\
        .withColumn('non_paying',sum('downloads').over(Window.partitionBy('date','paying_customer')))\
        .select('date','paying_customer','non_paying').distinct()\
        .withColumn('paying',lead('non_paying').over(Window.partitionBy('date').orderBy('date')))\
        .filter(col('paying').isNotNull() ).select('date','non_paying','paying')\
        .filter(col('non_paying')>col('paying')).show()


# COMMAND ----------

# MAGIC %md
# MAGIC #### Spark_SQL

# COMMAND ----------

ms_user_dimension_df.createOrReplaceTempView('ms_user_dimension_table')
ms_acc_dimension_df.createOrReplaceTempView('ms_acc_dimension_table')
ms_download_facts.createOrReplaceTempView('ms_download_facts_table')

# COMMAND ----------

spark.sql("""
select *
from
    (select date,
    non_paying,
    lead(non_paying) over(partition by date order by paying_customer) as paying
    from
        (select distinct date , 
        sum(downloads) over(partition by date,paying_customer) as non_paying, 
        paying_customer 
        from ms_download_facts_table as a
        left join ms_user_dimension_table as b
        on a.user_id = b.user_id
        left join ms_acc_dimension_table as c
        on b.acc_id = c.acc_id
        ) as T
    ) as U
where paying is not null
and non_paying > paying ;
          """).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Popularity Percentage
# MAGIC
# MAGIC Q. Find the popularity percentage for each user on Meta/Facebook. The popularity percentage is defined as the total number of friends the user has divided by the total number of users on the platform, then converted into a percentage by multiplying by 100.
# MAGIC Output each user along with their popularity percentage. Order records in ascending order by user id.
# MAGIC The 'user1' and 'user2' column are pairs of friends.    
# MAGIC [link](https://platform.stratascratch.com/coding/10284-popularity-percentage?code_type=3)
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### Spark-Dataframe API

# COMMAND ----------

my_data = ([(2,1),
            (1,3),
            (4,1),
            (1,5),
            (1,6),
            (2,6),
            (7,2),
            (8,3),
            (3,9),
          ])

my_schema = StructType([
    StructField('user1',IntegerType()),
    StructField('user2',IntegerType())
                      ])

facebook_friends_df = spark.createDataFrame(my_data,my_schema)

facebook_friends_df.show(5)
facebook_friends_df.printSchema()

# COMMAND ----------

facebook_friends_df.select('user1').union(facebook_friends_df.select('user2').alias('user1'))\
                .withColumn('user_pop',count('user1').over(Window.partitionBy('user1'))) \
                .withColumn('total_user', lit(result.select(col('user1')).distinct().count()))\
                .distinct()\
                .withColumn('popularity_percent',round(col('user_pop')/col('total_user')*100,3))\
                .drop(col('user_pop'),col('total_user')).show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Spark_SQL

# COMMAND ----------

facebook_friends_df.createOrReplaceTempView('facebook_friends_df_table')

# COMMAND ----------

spark.sql("""
select user1, round((pop_count/total_count)*100,3) as popularity_percent
from
    (select  distinct user1 , 
     count(user1) over(partition by user1)  as pop_count ,
     (count(user1) over())/2 as total_count
     from
        (SELECT user1, user2 
        FROM facebook_friends_df_table 
        UNION 
        SELECT user2 AS user1, user1 AS user2 
        FROM facebook_friends_df_table
        ) as T
    ) as U
""").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Top 5 States With 5 Star Businesses
# MAGIC
# MAGIC Q. Find the top 5 states with the most 5 star businesses. Output the state name along with the number of 5-star businesses and order records by the number of 5-star businesses in descending order. In case there are ties in the number of businesses, return all the unique states. If two states have the same result, sort them in alphabetical order.     
# MAGIC [Link](https://platform.stratascratch.com/coding/10046-top-5-states-with-5-star-businesses?code_type=3)
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### Spark-Dataframe API

# COMMAND ----------

yelp_business_df = spark.read.format('csv')\
                   .option('header','true')\
                   .option('inferschema','true')\
                   .load('/FileStore/tables/five-1.csv')

yelp_business_df.show(5)
yelp_business_df.printSchema()

# COMMAND ----------

yelp_business_df.select('state').filter(col('stars')==5)\
        .groupBy('state').agg(count('state').alias('five_star_counts')).orderBy(col('five_star_counts').desc())\
        .withColumn('rnk',rank().over(Window.orderBy(col('five_star_counts').desc())))\
        .orderBy('rnk','state').filter(col('rnk')<=5).drop(col('rnk')).show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Spark_SQL

# COMMAND ----------

yelp_business_df.createOrReplaceTempView('yelp_business_df_table')

# COMMAND ----------

spark.sql("""
select state,five_star_counts
from
    (select *,rank() over(order by five_star_counts desc) as rnk
    from 
        (select state , count(state) as five_star_counts 
        from yelp_business_df_table
        where stars = 5
        group by state
        order by five_star_counts desc , state 
        ) as T 
    ) as U
where rnk <= 5
          """).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Host Popularity Rental Prices
# MAGIC
# MAGIC Q. You’re given a table of rental property searches by users. The table consists of search results and outputs host information for searchers. Find the minimum, average, maximum rental prices for each host’s popularity rating. The host’s popularity rating is defined as below:  
# MAGIC 0 reviews: New  
# MAGIC 1 to 5 reviews: Rising  
# MAGIC 6 to 15 reviews: Trending Up  
# MAGIC 16 to 40 reviews: Popular  
# MAGIC more than 40 reviews: Hot 
# MAGIC
# MAGIC
# MAGIC Tip: The id column in the table refers to the search ID. You'll need to create your own host_id by concating price, room_type, host_since, zipcode, and number_of_reviews.
# MAGIC
# MAGIC
# MAGIC Output host popularity rating and their minimum, average and maximum rental prices.    
# MAGIC [Link](https://platform.stratascratch.com/coding/9632-host-popularity-rental-prices?code_type=3)
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### Spark-Dataframe API

# COMMAND ----------

airbnb_host_searches_df = spark.read.format('csv')\
                   .option('header','true')\
                   .option('inferschema','true')\
                   .load('/FileStore/tables/six_modified-1.csv')

airbnb_host_searches_df.show(5)
airbnb_host_searches_df.printSchema()

# COMMAND ----------

airbnb_host_searches_df.select(concat('price','room_type','host_since','zipcode','number_of_reviews')\
                                .alias('host_id'),'price','number_of_reviews').distinct()\
                                .withColumn('host_popularity',when(col('number_of_reviews') == 0,'New')\
                                .when((col('number_of_reviews') >= 1) & (col('number_of_reviews') <=5),'Rising')\
                                .when((col('number_of_reviews')>=6) & (col('number_of_reviews')<=15),'Trending Up')\
                                .when((col('number_of_reviews') >= 16) & (col('number_of_reviews')<=40),'Popular')\
                                .when((col('number_of_reviews')>40),'Hot'))\
                                .drop(col('host_id'),col('number_of_reviews'))\
                                .groupBy(col('host_popularity'))\
                                    .agg(round(min(col('price')),2).alias('min_price'),\
                                         round(avg(col('price')),2).alias('avg_price'),\
                                         round(max(col('price')),2).alias('max_price')).show()
 

# COMMAND ----------

# MAGIC %md
# MAGIC #### Spark_SQL

# COMMAND ----------

airbnb_host_searches_df.createOrReplaceTempView('airbnb_host_searches_df_table')

# COMMAND ----------

spark.sql("""
select 
    case 
        when number_of_reviews = 0 then 'New' 
        when (number_of_reviews>=1 and number_of_reviews<=5) then 'Rising'
        when (number_of_reviews>=6 and number_of_reviews<=15) then 'Trending Up'
        when (number_of_reviews>=16 and number_of_reviews<=40) then 'Popular'
        when number_of_reviews > 40 then 'Hot' 
    end as host_popularity , 
    round(min(price),2) as min_price,
    round(avg(price),2) as avg_price,
    round(max(price),2) as max_price
from
    (
    select 
    distinct concat(price,room_type,host_since,zipcode,number_of_reviews) as host_id,
    price , 
    number_of_reviews
    from airbnb_host_searches_df_table
    ) as U
group by host_popularity
          """).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Cookbook Recipes
# MAGIC
# MAGIC Q. You are given the table with titles of recipes from a cookbook and their page numbers. You are asked to represent how the recipes will be distributed in the book.
# MAGIC Produce a table consisting of three columns: left_page_number, left_title and right_title. The k-th row (counting from 0), should contain the number and the title of the page with the number (2 x K) in the first and second columns respectively, and the title of the page with the number (2 x k)+1 in the third column.
# MAGIC Each page contains at most 1 recipe. If the page does not contain a recipe, the appropriate cell should remain empty (NULL value). Page 0 (the internal side of the front cover) is guaranteed to be empty.     
# MAGIC [Link](https://platform.stratascratch.com/coding/2089-cookbook-recipes?code_type=3)
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### Spark-Dataframe API

# COMMAND ----------

my_data = ([
            (1	,'Scrambled eggs'),
            (2	,'Fondue'),
            (3	,'Sandwich'),
            (4	,'Tomato soup'),
            (6	,'Liver'),
            (11	,'Fried duck'),
            (12	,'Boiled duck'),
            (15	,'Baked chicken')
          ])

my_schema = StructType([ StructField('page_number',IntegerType()) ,
                        StructField('title',StringType()) ])

cookbook_titles_df = spark.createDataFrame(my_data,my_schema)

cookbook_titles_df.show()
cookbook_titles_df.printSchema()

# COMMAND ----------

cookbook_titles_df1 = cookbook_titles_df.withColumn('left_page_number', ((row_number().over(Window.orderBy('page_number')))-1)*2)

cookbook_titles_df_alias = cookbook_titles_df.alias("df")
cookbook_titles_df1_alias = cookbook_titles_df1.alias("df1")

result  =   cookbook_titles_df1.join(cookbook_titles_df,cookbook_titles_df1_alias['left_page_number'] == 
                    cookbook_titles_df_alias['page_number'] ,"left")\
                    .withColumn('right_pgno',col('left_page_number') +1)\
                    .drop(cookbook_titles_df1_alias['title'],cookbook_titles_df1_alias['page_number'],'page_number')\
                    .withColumnRenamed("title","left_title")

result.join(cookbook_titles_df,result["right_pgno"] == cookbook_titles_df["page_number"],"left")\
                                                        .drop("right_pgno","page_number")\
                                                        .withColumnRenamed("title","right_title").show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Spark_SQL

# COMMAND ----------

cookbook_titles_df.createOrReplaceTempView('cookbook_titles_df_table')

# COMMAND ----------

spark.sql("""
select left_page_number, 
b.title as left_title ,
c.title as right_title
from 
    (
        select 
        (row_number() over(ORDER BY page_number)-1)*2 as left_page_number ,
        (row_number() over(ORDER BY page_number)-1)*2+1 as right_page_number
        from
        cookbook_titles_df_table
    ) as a
left join cookbook_titles_df_table b
on a.left_page_number = b.page_number
left join cookbook_titles_df_table c
on a.right_page_number = c.page_number
          """).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Retention Rate
# MAGIC
# MAGIC Q. Find the monthly retention rate of users for each account separately for Dec 2020 and Jan 2021. Retention rate is the percentage of active users an account retains over a given period of time. In this case, assume the user is retained if he/she stays with the app in any future months. For example, if a user was active in Dec 2020 and has activity in any future month, consider them retained for Dec. You can assume all accounts are present in Dec 2020 and Jan 2021. Your output should have the account ID and the Jan 2021 retention rate divided by Dec 2020 retention rate.           
# MAGIC [Link](https://platform.stratascratch.com/coding/2053-retention-rate?code_type=3)
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### Spark-Dataframe API

# COMMAND ----------

 sf_events_df = spark.read.format('csv')\
                        .option('header','true')\
                        .option('inferschema','true')\
                        .option('mode','PERMISSIVE')\
                        .load('/FileStore/tables/eight.csv')

sf_events_df.show(5)
sf_events_df.printSchema()

# COMMAND ----------

sf_events_df.select(date_format(col(' date'),"yyyy-MM").alias('date'),"account_id")\
        .withColumn('presence_in_month', lit('1'))\
        .distinct().sort('date','account_id')\
        .withColumn('dec_retension',lead('presence_in_month').over(Window.partitionBy('account_id').orderBy('date')))\
        .withColumn('jan_retension',lead('dec_retension').over(Window.partitionBy('account_id').orderBy('date')))\
        .filter(col('date') == '2020-12')\
        .withColumn('retention_rate',coalesce(col('jan_retension')/col('dec_retension'),lit(0)))\
        .select('account_id','retention_rate').show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Spark_SQL

# COMMAND ----------

sf_events_df.createOrReplaceTempView('sf_events_df_table')

# COMMAND ----------

spark.sql(""" 
select account_id , coalesce((jan_retension/dec_retension),0) as retention_rate
from
(select mnth,account_id , dec_retension,
lead(dec_retension) over(Partition by account_id order by mnth) as jan_retension
from 
    (select mnth , account_id , 
    lead(1) over(Partition by account_id order by mnth) as dec_retension 
    from
        ( select date_format( `sf_events_df_table`.` date` ,"yyyy-MM") as mnth,
        account_id ,
        count(user_id) as cnt
        from sf_events_df_table
        group by mnth , account_id
        order by account_id 
        ) as U
    ) as T
where mnth != '2021-02' 
) as V
where mnth != '2021-01' 
          """).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. The Most Popular Client_Id Among Users Using Video and Voice Calls
# MAGIC
# MAGIC Q. Select the most popular client_id based on a count of the number of users who have at least 50% of their events from the following list: 'video call received', 'video call sent', 'voice call received', 'voice call sent'.   
# MAGIC [Link](https://platform.stratascratch.com/coding/2029-the-most-popular-client_id-among-users-using-video-and-voice-calls?code_type=3)
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### Spark-Dataframe API

# COMMAND ----------

fact_events_df = spark.read.format('csv')\
                        .option('header','true')\
                        .option('inferschema','true')\
                        .option('mode','PERMISSIVE')\
                        .load('/FileStore/tables/nine.csv')

fact_events_df.show(5)
fact_events_df.printSchema()

# COMMAND ----------

fact_events_df.select('client_id','user_id','event_type')\
        .withColumn('tot_user_events',count('event_type').over(Window.partitionBy('user_id').orderBy('user_id')))\
        .withColumn('favorable_event',when(((col('event_type') == 'video call received') | 
                                           (col('event_type') == 'video call sent') |
                                           (col('event_type') == 'voice call received') |
                                           (col('event_type') == 'voice call sent') ) , 1 ))\
        .drop(col('event_type')).groupBy('user_id','client_id','tot_user_events')\
                                .agg(count('favorable_event').alias('favorable_event'))\
        .withColumn('prcnt_event',col('favorable_event')/col('tot_user_events'))\
        .filter(col('prcnt_event')>= 0.5).select('client_id')\
        .groupby('client_id').agg(count('client_id').alias('cnt')).sort(col('cnt').desc()).limit(1)\
        .select('client_id').show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Spark_SQL

# COMMAND ----------

fact_events_df.createOrReplaceTempView('fact_events_df_table')

# COMMAND ----------

spark.sql("""
with cte as
(select user_id 
from
(select distinct
user_id,
round(((sum(special_event) over(partition by user_id) )/tot_user_events)*100,2) as prcnt_event
from
    (select user_id,
    event_type,
    count(event_type) over(partition by user_id) as tot_user_events,
    case when event_type='video call received'or
              event_type='video call sent'or
              event_type='voice call received'or
              event_type='voice call sent'
        then  1
    end as special_event
    from fact_events_df_table
    order by user_id
    ) as U
) as V
where prcnt_event>=50)

select client_id 
from cte a
left join fact_events_df_table b
on a.user_id = b.user_id
group by client_id
order by count(client_id) desc
limit 1
          """).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Marketing Campaign Success [Advanced]
# MAGIC
# MAGIC Q. You have a table of in-app purchases by user. Users that make their first in-app purchase are placed in a marketing campaign where they see call-to-actions for more in-app purchases. Find the number of users that made additional in-app purchases due to the success of the marketing campaign.
# MAGIC The marketing campaign doesn't start until one day after the initial in-app purchase so users that only made one or multiple purchases on the first day do not count, nor do we count users that over time purchase only the products they purchased on the first day.   
# MAGIC [Link](https://platform.stratascratch.com/coding/514-marketing-campaign-success-advanced?code_type=3)
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### Spark-Dataframe API

# COMMAND ----------


marketing_campaign_df = spark.read.format('csv')\
                        .option('header','true')\
                        .option('inferschema','true')\
                        .option('mode','PERMISSIVE')\
                        .load('/FileStore/tables/ten.csv')

marketing_campaign_df.show(5)
marketing_campaign_df.printSchema()

# COMMAND ----------

window_spec_1 = Window.partitionBy(" user_id").orderBy("created_at")
window_spec_2 = Window.partitionBy(" user_id" , "product_id").orderBy("created_at")

marketing_campaign_df.withColumn("date_rank", dense_rank().over(window_spec_1)) \
            .withColumn("prod_rank", row_number().over(window_spec_2))\
            .filter((col('date_rank')>1) & (col('prod_rank') == 1))\
            .select(' user_id').distinct().count()


# COMMAND ----------

# MAGIC %md
# MAGIC #### Spark_SQL

# COMMAND ----------

marketing_campaign_df.createOrReplaceTempView('marketing_campaign_df_table')

# COMMAND ----------

spark.sql("""
select count(distinct `U`.` user_id`)
from          
    (select *,
    (dense_rank() over(partition by `marketing_campaign_df_table`.` user_id` order by created_at)) as date_rank ,
    (dense_rank() over(partition by `marketing_campaign_df_table`.` user_id`, product_id order by created_at)) as prod_rank
    from
    marketing_campaign_df_table
    ) as U
where (date_rank>1) 
and (prod_rank==1)
          """).show()
