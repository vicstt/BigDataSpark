from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, desc, avg, count, month, year, round as spark_round, when, lit, max as spark_max, min as spark_min
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_spark_session():
    return SparkSession.builder \
        .appName("CH_Reports") \
        .config("spark.jars", "/opt/spark/jars/postgresql-42.7.3.jar,/opt/spark/jars/clickhouse-jdbc-0.4.6-all.jar") \
        .config("spark.driver.extraClassPath", "/opt/spark/jars/postgresql-42.7.3.jar:/opt/spark/jars/clickhouse-jdbc-0.4.6-all.jar") \
        .config("spark.executor.extraClassPath", "/opt/spark/jars/postgresql-42.7.3.jar:/opt/spark/jars/clickhouse-jdbc-0.4.6-all.jar") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()

def write_to_clickhouse(df, table_name):
    """Запись DataFrame в ClickHouse через JDBC"""
    try:
        df.write \
            .format("jdbc") \
            .option("url", "jdbc:clickhouse://clickhouse_db:8123/reports_db") \
            .option("dbtable", table_name) \
            .option("user", "default") \
            .option("password", "") \
            .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
            .option("socket_timeout", "300000") \
            .option("connect_timeout", "300000") \
            .option("custom_http_params", "async=false") \
            .option("custom_http_headers", "X-ClickHouse-User=default") \
            .option("createTableOptions", "ENGINE = MergeTree() ORDER BY tuple()") \
            .mode("overwrite") \
            .save()
        return True
    except Exception as e:
        logger.error(f"{table_name}: ошибка записи")
        logger.error(f"Ошибка: {str(e)[:200]}")
        return False

def main():
    spark = create_spark_session()
    
    PG_URL = "jdbc:postgresql://postgres_db:5432/retail_db"
    PG_OPTIONS = {
        "url": PG_URL,
        "user": "admin",
        "password": "admin123",
        "driver": "org.postgresql.Driver"
    }
    
    try:
        logger.info("=" * 50)
        logger.info("Загрузка данных из PostgreSQL...")
        
        fact = spark.read.format("jdbc").options(**PG_OPTIONS).option("dbtable", "fact_sales").load()
        prod = spark.read.format("jdbc").options(**PG_OPTIONS).option("dbtable", "dim_product").load()
        cust = spark.read.format("jdbc").options(**PG_OPTIONS).option("dbtable", "dim_customer").load()
        date = spark.read.format("jdbc").options(**PG_OPTIONS).option("dbtable", "dim_date").load()
        mock = spark.read.format("jdbc").options(**PG_OPTIONS).option("dbtable", "mock_data").load()
        
        logger.info("Данные загружены, выполняем join'ы...")
        
        sp = fact.join(prod, "product_id")
        sc = fact.join(cust, "customer_id")
        sd = fact.join(date, "date_id")
        
        reports = []
        
        logger.info("Создание top_products...")
        reports.append(("top_products", sp.groupBy("name")
                       .agg(sum("quantity").alias("total_sold"))
                       .orderBy(desc("total_sold"))
                       .limit(10)))
        
        logger.info("Создание revenue_by_category...")
        reports.append(("revenue_by_category", sp.groupBy("category")
                       .agg(sum("total_price").alias("total_revenue"))
                       .orderBy(desc("total_revenue"))))
        
        logger.info("Создание product_rating...")
        reports.append(("product_rating", prod.groupBy("name")
                       .agg(avg("rating").alias("avg_rating"), 
                            sum("reviews").alias("total_reviews"))
                       .orderBy(desc("avg_rating"))))
        
        logger.info("Создание top_customers...")
        reports.append(("top_customers", sc.groupBy("first_name", "last_name")
                       .agg(sum("total_price").alias("total_spent"))
                       .orderBy(desc("total_spent"))
                       .limit(10)))
        
        logger.info("Создание customers_country...")
        reports.append(("customers_country", cust.groupBy("country")
                       .agg(count("*").alias("customer_count"))
                       .orderBy(desc("customer_count"))))
        
        logger.info("Создание avg_check_cust...")
        reports.append(("avg_check_cust", sc.groupBy("first_name", "last_name")
                       .agg(spark_round(avg("total_price"), 2).alias("avg_check"))
                       .orderBy(desc("avg_check"))
                       .limit(50)))
        
        logger.info("Создание monthly_sales...")
        reports.append(("monthly_sales", sd.groupBy("year", "month")
                       .agg(sum("total_price").alias("total_revenue"), 
                            count("*").alias("total_orders"))
                       .orderBy("year", "month")))
        
        logger.info("Создание yearly_sales...")
        reports.append(("yearly_sales", sd.groupBy("year")
                       .agg(sum("total_price").alias("total_revenue"), 
                            count("*").alias("total_orders"))
                       .orderBy("year")))
        
        logger.info("Создание avg_monthly...")
        reports.append(("avg_monthly", sd.groupBy("year", "month")
                       .agg(spark_round(avg("total_price"), 2).alias("avg_order_value"))
                       .orderBy("year", "month")))
        
        logger.info("Создание top_stores...")
        reports.append(("top_stores", mock.groupBy("store_name")
                       .agg(sum("sale_total_price").alias("total_revenue"))
                       .orderBy(desc("total_revenue"))
                       .limit(5)))
        
        logger.info("Создание sales_city...")
        reports.append(("sales_city", mock.groupBy("store_city", "store_country")
                       .agg(sum("sale_total_price").alias("total_revenue"))
                       .orderBy(desc("total_revenue"))))
        
        logger.info("Создание avg_check_store...")
        reports.append(("avg_check_store", mock.groupBy("store_name")
                       .agg(spark_round(avg("sale_total_price"), 2).alias("avg_check"))
                       .orderBy(desc("avg_check"))
                       .limit(20)))
        
        logger.info("Создание top_suppliers...")
        reports.append(("top_suppliers", mock.groupBy("supplier_name")
                       .agg(sum("sale_total_price").alias("total_revenue"))
                       .orderBy(desc("total_revenue"))
                       .limit(5)))
        
        logger.info("Создание avg_price_supp...")
        reports.append(("avg_price_supp", mock.groupBy("supplier_name")
                       .agg(spark_round(avg("product_price"), 2).alias("avg_price"))
                       .orderBy(desc("avg_price"))
                       .limit(20)))
        
        logger.info("Создание supplier_country...")
        reports.append(("supplier_country", mock.groupBy("supplier_country")
                       .agg(sum("sale_total_price").alias("total_revenue"))
                       .orderBy(desc("total_revenue"))))
        
        logger.info("Создание high_low_rating...")
        max_r = prod.agg(spark_max("rating")).collect()[0][0]
        min_r = prod.agg(spark_min("rating")).collect()[0][0]
        high_low_rating_df = prod.filter((col("rating") == max_r) | (col("rating") == min_r)) \
            .select("name", "rating", 
                   when(col("rating") == max_r, lit("highest"))
                   .otherwise(lit("lowest")).alias("rating_type"))
        reports.append(("high_low_rating", high_low_rating_df))
        
        logger.info("Создание rating_correlation...")
        correlation = prod.stat.corr("rating", "reviews") or 0.0
        correlation_df = spark.createDataFrame(
            [(float(correlation),)], ["correlation_coefficient"]
        )
        reports.append(("rating_correlation", correlation_df))
        
        logger.info("Создание most_reviewed...")
        reports.append(("most_reviewed", prod.groupBy("name")
                       .agg(sum("reviews").alias("total_reviews"))
                       .orderBy(desc("total_reviews"))
                       .limit(20)))
        
        logger.info("Запись отчетов в ClickHouse...")
        
        success_count = 0
        for name, df in reports:
            if write_to_clickhouse(df, name):
                logger.info(f" {name}: записано успешно")
                success_count += 1
        
        logger.info(f"Готово! Успешно создано {success_count} из {len(reports)} таблиц в ClickHouse")
        
    except Exception as e:
        logger.error(f"Критическая ошибка: {str(e)}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()