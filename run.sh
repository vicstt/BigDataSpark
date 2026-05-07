#!/bin/bash

set -e

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "\n${YELLOW}[1/6] Подготовка папок...${NC}"
mkdir -p spark/jobs data clickhouse-config

echo -e "\n${YELLOW}[2/6] Проверка CSV файлов...${NC}"
if ! ls data/MOCK_DATA*.csv 1>/dev/null 2>&1; then
    echo -e "${RED}Ошибка: CSV файлы не найдены в ./data/${NC}"
    exit 1
fi
echo -e "${GREEN}CSV файлы найдены${NC}"

echo -e "\n${YELLOW}[3/6] Остановка старых контейнеров...${NC}"
docker-compose down -v 2>/dev/null || true

echo -e "\n${YELLOW}[4/6] Запуск Docker контейнеров...${NC}"
docker-compose up -d

echo -e "\n${YELLOW}[5/6] Ожидание готовности сервисов...${NC}"
sleep 20

echo -n "  PostgreSQL..."
until docker exec postgres_db pg_isready -U admin -d retail_db &>/dev/null; do
    sleep 2
    echo -n "."
done
echo -e " ${GREEN}готов${NC}"

echo -n "  ClickHouse..."
until docker exec clickhouse_db clickhouse-client --query "SELECT 1" &>/dev/null; do
    sleep 2
    echo -n "."
done
echo -e " ${GREEN}готов${NC}"

echo -e "\n${YELLOW}[6/6] Запуск ETL процесса...${NC}"

docker exec postgres_db mkdir -p /data
for file in data/MOCK_DATA*.csv; do
    docker cp "$file" postgres_db:/data/
done

echo "Создание таблицы mock_data..."
docker exec -i postgres_db psql -U admin -d retail_db << 'SQL'
DROP TABLE IF EXISTS mock_data CASCADE;
CREATE TABLE mock_data (
    id INTEGER,
    customer_first_name VARCHAR(100),
    customer_last_name VARCHAR(100),
    customer_age INTEGER,
    customer_email VARCHAR(200),
    customer_country VARCHAR(100),
    customer_postal_code VARCHAR(20),
    customer_pet_type VARCHAR(50),
    customer_pet_name VARCHAR(100),
    customer_pet_breed VARCHAR(100),
    seller_first_name VARCHAR(100),
    seller_last_name VARCHAR(100),
    seller_email VARCHAR(200),
    seller_country VARCHAR(100),
    seller_postal_code VARCHAR(20),
    product_name VARCHAR(200),
    product_category VARCHAR(100),
    product_price DECIMAL(10,2),
    product_quantity INTEGER,
    sale_date DATE,
    sale_customer_id INTEGER,
    sale_seller_id INTEGER,
    sale_product_id INTEGER,
    sale_quantity INTEGER,
    sale_total_price DECIMAL(10,2),
    store_name VARCHAR(200),
    store_location VARCHAR(200),
    store_city VARCHAR(100),
    store_state VARCHAR(50),
    store_country VARCHAR(100),
    store_phone VARCHAR(50),
    store_email VARCHAR(200),
    pet_category VARCHAR(50),
    product_weight DECIMAL(10,2),
    product_color VARCHAR(50),
    product_size VARCHAR(50),
    product_brand VARCHAR(100),
    product_material VARCHAR(100),
    product_description TEXT,
    product_rating DECIMAL(3,1),
    product_reviews INTEGER,
    product_release_date DATE,
    product_expiry_date DATE,
    supplier_name VARCHAR(200),
    supplier_contact VARCHAR(200),
    supplier_email VARCHAR(200),
    supplier_phone VARCHAR(50),
    supplier_address TEXT,
    supplier_city VARCHAR(100),
    supplier_country VARCHAR(100)
);

DO $$
DECLARE
    csv_file TEXT;
BEGIN
    FOR csv_file IN SELECT * FROM pg_ls_dir('/data/') WHERE pg_ls_dir LIKE '%.csv' LOOP
        EXECUTE format('COPY mock_data FROM %L DELIMITER '','' CSV HEADER', '/data/' || csv_file);
        RAISE NOTICE 'Импортирован: %', csv_file;
    END LOOP;
END $$;
SQL

TOTAL=$(docker exec postgres_db psql -U admin -d retail_db -t -c "SELECT COUNT(*) FROM mock_data;" 2>/dev/null | tr -d ' ')
echo -e "${GREEN}Загружено $TOTAL строк в mock_data${NC}"

echo -e "\n${YELLOW}[1/2] Создание Star Schema...${NC}"
docker exec spark /opt/spark/bin/spark-submit \
    --master local[*] \
    --driver-memory 2g \
    --executor-memory 2g \
    --jars /opt/spark/jars/postgresql-42.7.3.jar \
    /opt/app/01_create_star_schema.py

echo "Проверка Star Schema:"
docker exec postgres_db psql -U admin -d retail_db -c "
SELECT 'dim_customer' as table_name, COUNT(*) as rows FROM dim_customer
UNION ALL SELECT 'dim_product', COUNT(*) FROM dim_product
UNION ALL SELECT 'dim_seller', COUNT(*) FROM dim_seller
UNION ALL SELECT 'dim_date', COUNT(*) FROM dim_date
UNION ALL SELECT 'fact_sales', COUNT(*) FROM fact_sales
ORDER BY table_name;
"

echo -e "\n${YELLOW}[2/2] Создание отчетов в ClickHouse...${NC}"
docker exec spark /opt/spark/bin/spark-submit \
    --master local[*] \
    --driver-memory 2g \
    --executor-memory 2g \
    --jars /opt/spark/jars/postgresql-42.7.3.jar \
    /opt/app/02_clickhouse_reports.py

echo -e "\n${GREEN}Проверка отчетов в ClickHouse:${NC}"
docker exec clickhouse_db clickhouse-client --query "
SELECT 
    table,
    total_rows
FROM system.tables
WHERE database = 'reports_db'
ORDER BY table
FORMAT PrettyCompact
"

echo -e "${GREEN}PostgreSQL: localhost:5432 (admin/admin123)${NC}"
echo -e "${GREEN}ClickHouse: localhost:8123${NC}"
echo -e "${GREEN}Spark UI:  localhost:4040${NC}"