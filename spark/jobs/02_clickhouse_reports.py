from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, desc, avg, count, month, year, round as spark_round, when, lit, max as spark_max, min as spark_min
import requests, logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

spark = SparkSession.builder.appName("CH_Reports").config("spark.jars", "/opt/spark/jars/postgresql-42.7.3.jar").getOrCreate()

def ch(q):
    try:
        r = requests.post("http://clickhouse_db:8123/", data=q.encode('utf-8'))
        return r.status_code in [200, 201]
    except:
        return False

fact = spark.read.format("jdbc").option("url", "jdbc:postgresql://postgres_db:5432/retail_db").option("dbtable", "fact_sales").option("user", "admin").option("password", "admin123").option("driver", "org.postgresql.Driver").load()
prod = spark.read.format("jdbc").option("url", "jdbc:postgresql://postgres_db:5432/retail_db").option("dbtable", "dim_product").option("user", "admin").option("password", "admin123").option("driver", "org.postgresql.Driver").load()
cust = spark.read.format("jdbc").option("url", "jdbc:postgresql://postgres_db:5432/retail_db").option("dbtable", "dim_customer").option("user", "admin").option("password", "admin123").option("driver", "org.postgresql.Driver").load()
date = spark.read.format("jdbc").option("url", "jdbc:postgresql://postgres_db:5432/retail_db").option("dbtable", "dim_date").option("user", "admin").option("password", "admin123").option("driver", "org.postgresql.Driver").load()
mock = spark.read.format("jdbc").option("url", "jdbc:postgresql://postgres_db:5432/retail_db").option("dbtable", "mock_data").option("user", "admin").option("password", "admin123").option("driver", "org.postgresql.Driver").load()

sp = fact.join(prod, "product_id")
sc = fact.join(cust, "customer_id")
sd = fact.join(date, "date_id")

ch("CREATE DATABASE IF NOT EXISTS reports_db")

reports = []

reports.append(("top_products", sp.groupBy("name").agg(sum("quantity").alias("total_sold")).orderBy(desc("total_sold")).limit(10)))
reports.append(("revenue_by_category", sp.groupBy("category").agg(sum("total_price").alias("total_revenue")).orderBy(desc("total_revenue"))))
reports.append(("product_rating", prod.groupBy("name").agg(avg("rating").alias("avg_rating"), sum("reviews").alias("total_reviews")).orderBy(desc("avg_rating"))))

reports.append(("top_customers", sc.groupBy("first_name", "last_name").agg(sum("total_price").alias("total_spent")).orderBy(desc("total_spent")).limit(10)))
reports.append(("customers_country", cust.groupBy("country").agg(count("*").alias("customer_count")).orderBy(desc("customer_count"))))
reports.append(("avg_check_cust", sc.groupBy("first_name", "last_name").agg(spark_round(avg("total_price"), 2).alias("avg_check")).orderBy(desc("avg_check")).limit(50)))

reports.append(("monthly_sales", sd.groupBy("year", "month").agg(sum("total_price").alias("total_revenue"), count("*").alias("total_orders")).orderBy("year", "month")))
reports.append(("yearly_sales", sd.groupBy("year").agg(sum("total_price").alias("total_revenue"), count("*").alias("total_orders")).orderBy("year")))
reports.append(("avg_monthly", sd.groupBy("year", "month").agg(spark_round(avg("total_price"), 2).alias("avg_order_value")).orderBy("year", "month")))

reports.append(("top_stores", mock.groupBy("store_name").agg(sum("sale_total_price").alias("total_revenue")).orderBy(desc("total_revenue")).limit(5)))
reports.append(("sales_city", mock.groupBy("store_city", "store_country").agg(sum("sale_total_price").alias("total_revenue")).orderBy(desc("total_revenue"))))
reports.append(("avg_check_store", mock.groupBy("store_name").agg(spark_round(avg("sale_total_price"), 2).alias("avg_check")).orderBy(desc("avg_check")).limit(20)))

reports.append(("top_suppliers", mock.groupBy("supplier_name").agg(sum("sale_total_price").alias("total_revenue")).orderBy(desc("total_revenue")).limit(5)))
reports.append(("avg_price_supp", mock.groupBy("supplier_name").agg(spark_round(avg("product_price"), 2).alias("avg_price")).orderBy(desc("avg_price")).limit(20)))
reports.append(("supplier_country", mock.groupBy("supplier_country").agg(sum("sale_total_price").alias("total_revenue")).orderBy(desc("total_revenue"))))

max_r = prod.agg(spark_max("rating")).collect()[0][0]
min_r = prod.agg(spark_min("rating")).collect()[0][0]
reports.append(("high_low_rating", prod.filter((col("rating") == max_r) | (col("rating") == min_r)).select("name", "rating", when(col("rating") == max_r, lit("highest")).otherwise(lit("lowest")).alias("rating_type"))))
reports.append(("rating_correlation", spark.createDataFrame([(prod.stat.corr("rating", "reviews") or 0.0,)], ["correlation"])))
reports.append(("most_reviewed", prod.groupBy("name").agg(sum("reviews").alias("total_reviews")).orderBy(desc("total_reviews")).limit(20)))

for name, df in reports:
    cols = ", ".join([f"{c} String" for c in df.columns])
    ch(f"CREATE TABLE IF NOT EXISTS reports_db.{name} ({cols}) ENGINE = MergeTree() ORDER BY tuple()")
    ch(f"TRUNCATE TABLE IF EXISTS reports_db.{name}")
    
    for row in df.collect():
        vals = ", ".join([f"'{str(v)}'" if v is not None else "NULL" for v in row])
        ch(f"INSERT INTO reports_db.{name} VALUES ({vals})")
    
    logger.info(f"  {name}: {df.count()} строк")

logger.info(f"Готово! Создано {len(reports)} таблиц")
spark.stop()
