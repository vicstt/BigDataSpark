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

CREATE OR REPLACE FUNCTION import_all_csv_files()
RETURNS VOID AS $$
DECLARE
    csv_file TEXT;
BEGIN
    FOR csv_file IN SELECT * FROM pg_ls_dir('/data/') WHERE pg_ls_dir LIKE '%.csv' LOOP
        EXECUTE format('COPY mock_data FROM %L DELIMITER '','' CSV HEADER', '/data/' || csv_file);
        RAISE NOTICE 'Импортирован: %', csv_file;
    END LOOP;
END;
$$ LANGUAGE plpgsql;
