-- Walmart DW Star Schema

CREATE SCHEMA IF NOT EXISTS WalmartDW;
SET search_path TO WalmartDW;

-- ENUMs: only create if they don't exist
DO $$ BEGIN
    CREATE TYPE WalmartDW.gender_enum AS ENUM ('M', 'F');
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

DO $$ BEGIN
    CREATE TYPE WalmartDW.marital_enum AS ENUM ('0', '1');
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

DO $$ BEGIN
    CREATE TYPE WalmartDW.age_group_enum AS ENUM ('0-17','18-25','26-35','36-45','46-50','51-55','55+');
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

CREATE TABLE IF NOT EXISTS WalmartDW.Customer (
    customer_id               INT PRIMARY KEY,
    gender                    WalmartDW.gender_enum NOT NULL,
    age_group                 WalmartDW.age_group_enum NOT NULL,
    occupation                INT NOT NULL,
    city_category             VARCHAR(1) NOT NULL,
    marital_status            WalmartDW.marital_enum NOT NULL,
    stay_in_current_city_years INT NOT NULL CHECK (stay_in_current_city_years >= 0)
);

CREATE TABLE IF NOT EXISTS WalmartDW.Product (
    product_id       TEXT PRIMARY KEY,
    product_category TEXT NOT NULL,
    price            NUMERIC(12,2) NOT NULL CHECK (price > 0)
);

CREATE TABLE IF NOT EXISTS WalmartDW.Store (
    store_id  INTEGER PRIMARY KEY,
    storeName VARCHAR NOT NULL
);

CREATE TABLE IF NOT EXISTS WalmartDW.Supplier (
    supplier_id  INTEGER PRIMARY KEY,
    supplierName VARCHAR NOT NULL
);

CREATE TABLE IF NOT EXISTS WalmartDW.Date (
    date_id          INTEGER PRIMARY KEY,
    transaction_date DATE NOT NULL,
    dayNum           INT NOT NULL,
    monthNum         INT NOT NULL,
    year             INT NOT NULL,
    dayofweek        VARCHAR NOT NULL,
    quarter_num      INTEGER NOT NULL,
    is_weekend       BOOLEAN NOT NULL
);

CREATE TABLE IF NOT EXISTS WalmartDW.Sales (
    sales_id     INTEGER PRIMARY KEY,
    order_id     INTEGER NOT NULL,
    customer_id  INTEGER NOT NULL REFERENCES WalmartDW.Customer(customer_id),
    product_id   TEXT    NOT NULL REFERENCES WalmartDW.Product(product_id),
    date_id      INTEGER NOT NULL REFERENCES WalmartDW.Date(date_id),
    store_id     INTEGER NOT NULL REFERENCES WalmartDW.Store(store_id),
    supplier_id  INTEGER NOT NULL REFERENCES WalmartDW.Supplier(supplier_id),
    sales_amount NUMERIC(12,2) NOT NULL CHECK (sales_amount >= 0),
    quantity     INTEGER NOT NULL CHECK (quantity >= 0)
);

CREATE SEQUENCE IF NOT EXISTS WalmartDW.sales_id_seq START 1;
