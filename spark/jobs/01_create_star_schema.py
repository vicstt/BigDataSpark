from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, dayofmonth, quarter, weekofyear, to_date
from pyspark.sql.types import IntegerType
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_spark_session():
    return SparkSession.builder \
        .appName("ETL_Star_Schema") \
        .config("spark.jars", "/opt/spark/jars/postgresql-42.7.3.jar") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .getOrCreate()

def create_dim_customers(df):
    """Создание измерения клиентов"""
    dim_customer = df.select(
        col("sale_customer_id").alias("customer_id"),
        col("customer_first_name").alias("first_name"),
        col("customer_last_name").alias("last_name"),
        col("customer_age").alias("age"),
        col("customer_email").alias("email"),
        col("customer_country").alias("country"),
        col("customer_postal_code").alias("postal_code"),
        col("customer_pet_type").alias("pet_type"),
        col("customer_pet_name").alias("pet_name"),
        col("customer_pet_breed").alias("pet_breed")
    ).distinct().filter(col("customer_id").isNotNull())
    return dim_customer

def create_dim_products(df):
    """Создание измерения продуктов"""
    dim_product = df.select(
        col("sale_product_id").alias("product_id"),
        col("product_name").alias("name"),
        col("product_category").alias("category"),
        col("product_price").alias("price"),
        col("product_brand").alias("brand"),
        col("product_color").alias("color"),
        col("product_size").alias("size"),
        col("product_material").alias("material"),
        col("product_weight").alias("weight"),
        col("product_rating").alias("rating"),
        col("product_reviews").alias("reviews"),
        col("product_release_date").alias("release_date"),
        col("product_expiry_date").alias("expiry_date")
    ).distinct().filter(col("product_id").isNotNull())
    return dim_product

def create_dim_sellers(df):
    """Создание измерения продавцов"""
    dim_seller = df.select(
        col("sale_seller_id").alias("seller_id"),
        col("seller_first_name").alias("first_name"),
        col("seller_last_name").alias("last_name"),
        col("seller_email").alias("email"),
        col("seller_country").alias("country"),
        col("seller_postal_code").alias("postal_code")
    ).distinct().filter(col("seller_id").isNotNull())
    return dim_seller

def create_dim_date(df):
    """Создание измерения дат"""
    dates = df.select(to_date(col("sale_date")).alias("full_date")) \
        .distinct() \
        .filter(col("full_date").isNotNull())
    
    dim_date = dates.select(
        col("full_date"),
        (year("full_date") * 10000 + month("full_date") * 100 + dayofmonth("full_date")).cast(IntegerType()).alias("date_id"),
        year("full_date").alias("year"),
        quarter("full_date").alias("quarter"),
        month("full_date").alias("month"),
        dayofmonth("full_date").alias("day"),
        weekofyear("full_date").alias("week_of_year")
    )
    return dim_date

def create_fact_sales(df, dim_date):
    """Создание таблицы фактов"""
    fact_sales = df.select(
        col("sale_customer_id").alias("customer_id"),
        col("sale_product_id").alias("product_id"),
        col("sale_seller_id").alias("seller_id"),
        to_date(col("sale_date")).alias("sale_date"),
        col("sale_quantity").alias("quantity"),
        col("sale_total_price").alias("total_price")
    ).filter(
        col("customer_id").isNotNull() &
        col("product_id").isNotNull() &
        col("seller_id").isNotNull() &
        col("sale_date").isNotNull()
    )
    
    fact_sales = fact_sales.join(
        dim_date.select("full_date", "date_id"),
        fact_sales.sale_date == dim_date.full_date,
        "inner"
    ).drop("sale_date")
    
    return fact_sales

def write_to_postgres(df, table_name):
    """Запись DataFrame в PostgreSQL"""
    df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres_db:5432/retail_db") \
        .option("dbtable", table_name) \
        .option("user", "admin") \
        .option("password", "admin123") \
        .option("driver", "org.postgresql.Driver") \
        .mode("overwrite") \
        .save()

def main():
    spark = create_spark_session()
    
    try:
        logger.info("=" * 50)
        logger.info("Загрузка данных из PostgreSQL...")
        
        df = spark.read \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://postgres_db:5432/retail_db") \
            .option("dbtable", "mock_data") \
            .option("user", "admin") \
            .option("password", "admin123") \
            .option("driver", "org.postgresql.Driver") \
            .load()
        
        total_rows = df.count()
        logger.info(f"Загружено {total_rows} строк из mock_data")
        
        logger.info("Создание dim_customer...")
        dim_customer = create_dim_customers(df)
        write_to_postgres(dim_customer, "dim_customer")
        logger.info(f"  Создано {dim_customer.count()} записей")
        
        logger.info("Создание dim_product...")
        dim_product = create_dim_products(df)
        write_to_postgres(dim_product, "dim_product")
        logger.info(f"  Создано {dim_product.count()} записей")
        
        logger.info("Создание dim_seller...")
        dim_seller = create_dim_sellers(df)
        write_to_postgres(dim_seller, "dim_seller")
        logger.info(f"  Создано {dim_seller.count()} записей")
        
        logger.info("Создание dim_date...")
        dim_date = create_dim_date(df)
        write_to_postgres(dim_date, "dim_date")
        logger.info(f"  Создано {dim_date.count()} записей")
        
        logger.info("Создание fact_sales...")
        fact_sales = create_fact_sales(df, dim_date)
        write_to_postgres(fact_sales, "fact_sales")
        logger.info(f"  Создано {fact_sales.count()} записей")
        
        logger.info("=" * 50)
        logger.info("✅ Star schema успешно создана!")
        
    except Exception as e:
        logger.error(f"Ошибка: {str(e)}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()