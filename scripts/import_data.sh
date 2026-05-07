#!/bin/bash

echo "========================================="
echo "Импорт данных в PostgreSQL"
echo "========================================="

for file in data/MOCK_DATA*.csv; do
    filename=$(basename "$file")
    echo "Импорт: $filename"
    docker exec postgres_db psql -U admin -d retail_db -c "\COPY mock_data FROM '/data/$filename' WITH (FORMAT CSV, HEADER, DELIMITER ',');"
done

echo "========================================="
echo "Проверка количества строк:"
docker exec postgres_db psql -U admin -d retail_db -c "SELECT COUNT(*) FROM mock_data;"
echo "========================================="