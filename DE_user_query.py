TRUNCATE_n_INSERT_INTO_table_comm_list = [
"""TRUNCATE TABLE public.customers;
    INSERT INTO public.customers
    SELECT DISTINCT * FROM public.customers_tmp;""",

"""TRUNCATE TABLE public.order_items;
    INSERT INTO public.order_items
    SELECT DISTINCT * FROM public.order_items_tmp;""",
    
"""TRUNCATE TABLE public.order_payments;
    INSERT INTO public.order_payments
    SELECT DISTINCT * FROM public.order_payments_tmp;""",
    
"""TRUNCATE TABLE public.order_reviews;
    INSERT INTO public.order_reviews
    SELECT DISTINCT * FROM public.order_reviews_tmp;""",
    
"""TRUNCATE TABLE public.orders;
    INSERT INTO public.orders
    SELECT DISTINCT * FROM public.orders_tmp;""",
    
"""TRUNCATE TABLE public.products;
    INSERT INTO public.products
    SELECT DISTINCT * FROM public.products_tmp;"""
]


COPY_TMP_table_comm_list = [
"""COPY public.customers_tmp 
    FROM '/home/thai/Documents/WORK_SPACE/DE_OUTPUT/customers_dir/flink.customers_dataset.csv' 
    DELIMITER ',' CSV HEADER;""", 

"""COPY public.order_items_tmp 
    FROM '/home/thai/Documents/WORK_SPACE/DE_OUTPUT/order_items_dir/flink.order_items_dataset.csv' 
    DELIMITER ',' CSV HEADER;""", 

"""COPY public.order_payments_tmp 
    FROM '/home/thai/Documents/WORK_SPACE/DE_OUTPUT/order_payments_dir/flink.order_payments_dataset.csv' 
    DELIMITER ',' CSV HEADER;""", 

"""COPY public.order_reviews_tmp 
    FROM '/home/thai/Documents/WORK_SPACE/DE_OUTPUT/order_reviews_dir/flink.order_reviews_dataset.csv' 
    DELIMITER ',' CSV HEADER;""", 

"""COPY public.orders_tmp 
    FROM '/home/thai/Documents/WORK_SPACE/DE_OUTPUT/orders_dir/flink.orders_dataset.csv' 
    DELIMITER ',' CSV HEADER;""", 

"""COPY public.products_tmp 
    FROM '/home/thai/Documents/WORK_SPACE/DE_OUTPUT/products_dir/flink.products_dataset.csv' 
    DELIMITER ',' CSV HEADER;""", 
]

COPY_table_comm_list = [
"""COPY public.customers 
    FROM '/home/thai/Documents/WORK_SPACE/DE_DATASET/olist_customers_dataset.csv' 
    DELIMITER ',' CSV HEADER;""", 

"""COPY public.geolocation
    FROM '/home/thai/Documents/WORK_SPACE/DE_DATASET/olist_geolocation_dataset.csv' 
    DELIMITER ',' CSV HEADER;""", 

"""COPY public.leads_closed 
    FROM '/home/thai/Documents/WORK_SPACE/DE_DATASET/olist_closed_deals_dataset.csv' 
    DELIMITER ',' CSV HEADER;""", 

"""COPY public.leads_qualified 
    FROM '/home/thai/Documents/WORK_SPACE/DE_DATASET/olist_marketing_qualified_leads_dataset.csv' 
    DELIMITER ',' CSV HEADER;""", 

"""COPY public.order_items 
    FROM '/home/thai/Documents/WORK_SPACE/DE_DATASET/olist_order_items_dataset.csv' 
    DELIMITER ',' CSV HEADER;""", 

"""COPY public.order_payments 
    FROM '/home/thai/Documents/WORK_SPACE/DE_DATASET/olist_order_payments_dataset.csv' 
    DELIMITER ',' CSV HEADER;""", 

"""COPY public.order_reviews 
    FROM '/home/thai/Documents/WORK_SPACE/DE_DATASET/olist_order_reviews_dataset.csv' 
    DELIMITER ',' CSV HEADER;""", 

"""COPY public.orders 
    FROM '/home/thai/Documents/WORK_SPACE/DE_DATASET/olist_orders_dataset.csv' 
    DELIMITER ',' CSV HEADER;""", 

"""COPY public.product_category_name_translation 
    FROM '/home/thai/Documents/WORK_SPACE/DE_DATASET/product_category_name_translation.csv' 
    DELIMITER ',' CSV HEADER;""", 

"""COPY public.products 
    FROM '/home/thai/Documents/WORK_SPACE/DE_DATASET/olist_products_dataset.csv' 
    DELIMITER ',' CSV HEADER;""", 

"""COPY public.sellers 
    FROM '/home/thai/Documents/WORK_SPACE/DE_DATASET/olist_sellers_dataset.csv' 
    DELIMITER ',' CSV HEADER;"""
]

UNLOAD_table_comm_list = [
"""UNLOAD ('SELECT * FROM public.customers') 
    TO '/home/thai/Documents/WORK_SPACE/DE_DATASET/backup.olist_customers.csv' 
    CSV DELIMITER '|' ALLOWOVERWRITE PARALLEL OFF;""",

"""UNLOAD ('SELECT * FROM public.geolocation') 
    TO '/home/thai/Documents/WORK_SPACE/DE_DATASET/backup.olist_geolocation.csv' 
    CSV DELIMITER '|' ALLOWOVERWRITE PARALLEL OFF;""",

"""UNLOAD ('SELECT * FROM public.leads_closed') 
    TO '/home/thai/Documents/WORK_SPACE/DE_DATASET/backup.olist_closed_deals.csv' 
    CSV DELIMITER '|' ALLOWOVERWRITE PARALLEL OFF;""",

"""UNLOAD ('SELECT * FROM public.leads_qualified') 
    TO '/home/thai/Documents/WORK_SPACE/DE_DATASET/backup.olist_marketing_qualified_leads.csv' 
    CSV DELIMITER '|' ALLOWOVERWRITE PARALLEL OFF;""",

"""UNLOAD ('SELECT * FROM public.order_items') 
    TO '/home/thai/Documents/WORK_SPACE/DE_DATASET/backup.olist_order_items.csv' 
    CSV DELIMITER '|' ALLOWOVERWRITE PARALLEL OFF;""",

"""UNLOAD ('SELECT * FROM public.order_payments') 
    TO '/home/thai/Documents/WORK_SPACE/DE_DATASET/backup.olist_order_payments.csv' 
    CSV DELIMITER '|' ALLOWOVERWRITE PARALLEL OFF;""",

"""UNLOAD ('SELECT * FROM public.order_reviews') 
    TO '/home/thai/Documents/WORK_SPACE/DE_DATASET/backup.olist_order_reviews.csv' 
    CSV DELIMITER '|' ALLOWOVERWRITE PARALLEL OFF;""",

"""UNLOAD ('SELECT * FROM public.orders') 
    TO '/home/thai/Documents/WORK_SPACE/DE_DATASET/backup.olist_orders.csv' 
    CSV DELIMITER '|' ALLOWOVERWRITE PARALLEL OFF;""",

"""UNLOAD ('SELECT * FROM public.product_category_name_translation') 
    TO '/home/thai/Documents/WORK_SPACE/DE_DATASET/bakup.product_category_name_translation.csv' 
    CSV DELIMITER '|' ALLOWOVERWRITE PARALLEL OFF;""",

"""UNLOAD ('SELECT * FROM public.products') 
    TO '/home/thai/Documents/WORK_SPACE/DE_DATASET/bakup.olist_products.csv' 
    CSV DELIMITER '|' ALLOWOVERWRITE PARALLEL OFF;""",

"""UNLOAD ('SELECT * FROM public.sellers') 
    TO '/home/thai/Documents/WORK_SPACE/DE_DATASET/backup.olist_sellers.csv' 
    CSV DELIMITER '|' ALLOWOVERWRITE PARALLEL OFF;"""

]

TRUNCATE_TMP_table_comm_list = [
"""TRUNCATE TABLE public.customers_tmp""",
"""TRUNCATE TABLE public.order_items_tmp""",
"""TRUNCATE TABLE public.order_payments_tmp""",
"""TRUNCATE TABLE public.order_reviews_tmp""",
"""TRUNCATE TABLE public.orders_tmp""",
"""TRUNCATE TABLE public.products_tmp""",
]

TRUNCATE_table_comm_list = [
"""TRUNCATE TABLE public.customers""",
"""TRUNCATE TABLE public.geolocation""",
"""TRUNCATE TABLE public.leads_closed""",
"""TRUNCATE TABLE public.leads_qualified""",
"""TRUNCATE TABLE public.order_items""",
"""TRUNCATE TABLE public.order_payments""",
"""TRUNCATE TABLE public.order_reviews""",
"""TRUNCATE TABLE public.orders""",
"""TRUNCATE TABLE public.product_category_name_translation""",
"""TRUNCATE TABLE public.products""",
"""TRUNCATE TABLE public.sellers""",
]

DROP_TMP_table_comm_list = [
"""DROP TABLE IF EXISTS public.customers_tmp""",
"""DROP TABLE IF EXISTS public.order_items_tmp""",
"""DROP TABLE IF EXISTS public.order_payments_tmp""",
"""DROP TABLE IF EXISTS public.order_reviews_tmp""",
"""DROP TABLE IF EXISTS public.orders_tmp""",
"""DROP TABLE IF EXISTS public.products_tmp""",
]

DROP_table_comm_list = [
"""DROP TABLE IF EXISTS public.customers""",
"""DROP TABLE IF EXISTS public.geolocation""",
"""DROP TABLE IF EXISTS public.leads_closed""",
"""DROP TABLE IF EXISTS public.leads_qualified""",
"""DROP TABLE IF EXISTS public.order_items""",
"""DROP TABLE IF EXISTS public.order_payments""",
"""DROP TABLE IF EXISTS public.order_reviews""",
"""DROP TABLE IF EXISTS public.orders""",
"""DROP TABLE IF EXISTS public.product_category_name_translation""",
"""DROP TABLE IF EXISTS public.products""",
"""DROP TABLE IF EXISTS public.sellers""",
]

CREATE_TMP_table_comm_list = [
"""CREATE TABLE IF NOT EXISTS public.customers_tmp (
    customer_id character varying(256),
    customer_unique_id character varying(256),
    customer_zip_code_prefix integer,
    customer_city character varying(256),
    customer_state character varying(256)
);""",

"""CREATE TABLE IF NOT EXISTS public.order_items_tmp (
    order_id character varying(256),
    order_item_id integer,
    product_id character varying(256),
    seller_id character varying(256),
    shipping_limit_date timestamp without time zone,
    price real,
    freight_value real
);""",

"""CREATE TABLE IF NOT EXISTS public.order_payments_tmp (
    order_id character varying(256),
    payment_sequential integer,
    payment_type character varying(256),
    payment_installments integer,
    payment_value real
);""",

"""CREATE TABLE IF NOT EXISTS public.order_reviews_tmp (
    review_id character varying(256),
    order_id character varying(256),
    review_score integer,
    review_comment_title character varying(256),
    review_comment_message character varying(1024),
    review_creation_date timestamp without time zone,
    review_answer_timestamp timestamp without time zone
);""",

"""CREATE TABLE IF NOT EXISTS public.orders_tmp (
    order_id character varying(256),
    customer_id character varying(256),
    order_status character varying(256),
    order_purchase_timestamp timestamp without time zone,
    order_approved_at timestamp without time zone,
    order_delivered_carrier_date timestamp without time zone,
    order_delivered_customer_date timestamp without time zone,
    order_estimated_delivery_date timestamp without time zone
);""",

"""CREATE TABLE IF NOT EXISTS public.products_tmp (
    product_id character varying(256),
    product_category_name character varying(256),
    product_name_lenght integer,
    product_description_lenght integer,
    product_photos_qty integer,
    product_weight_g integer,
    product_length_cm integer,
    product_height_cm integer,
    product_width_cm integer
);""",
]

CREATE_table_comm_list = [
"""CREATE TABLE IF NOT EXISTS public.customers (
    customer_id character varying(256),
    customer_unique_id character varying(256),
    customer_zip_code_prefix integer,
    customer_city character varying(256),
    customer_state character varying(256)
);""",

"""CREATE TABLE IF NOT EXISTS public.geolocation (
    geolocation_zip_code_prefix integer,
    geolocation_lat character varying(256),
    geolocation_lng character varying(256),
    geolocation_city character varying(256),
    geolocation_state character varying(256)
);""",

"""CREATE TABLE IF NOT EXISTS public.leads_closed (
    mql_id character varying(256),
    seller_id character varying(256),
    sdr_id character varying(256),
    sr_id character varying(256),
    won_date timestamp without time zone,
    business_segment character varying(256),
    lead_type character varying(256),
    lead_behaviour_profile character varying(256),
    has_company boolean,
    has_gtin boolean,
    average_stock character varying(256),
    business_type character varying(256),
    declared_product_catalog_size real,
    declared_monthly_revenue real
);""",

"""CREATE TABLE IF NOT EXISTS public.leads_qualified (
    mql_id character varying(256),
    first_contact_date date,
    landing_page_id character varying(256),
    origin character varying(256)
);""",

"""CREATE TABLE IF NOT EXISTS public.order_items (
    order_id character varying(256),
    order_item_id integer,
    product_id character varying(256),
    seller_id character varying(256),
    shipping_limit_date timestamp without time zone,
    price real,
    freight_value real
);""",

"""CREATE TABLE IF NOT EXISTS public.order_payments (
    order_id character varying(256),
    payment_sequential integer,
    payment_type character varying(256),
    payment_installments integer,
    payment_value real
);""",

"""CREATE TABLE IF NOT EXISTS public.order_reviews (
    review_id character varying(256),
    order_id character varying(256),
    review_score integer,
    review_comment_title character varying(256),
    review_comment_message character varying(1024),
    review_creation_date timestamp without time zone,
    review_answer_timestamp timestamp without time zone
);""",

"""CREATE TABLE IF NOT EXISTS public.orders (
    order_id character varying(256),
    customer_id character varying(256),
    order_status character varying(256),
    order_purchase_timestamp timestamp without time zone,
    order_approved_at timestamp without time zone,
    order_delivered_carrier_date timestamp without time zone,
    order_delivered_customer_date timestamp without time zone,
    order_estimated_delivery_date timestamp without time zone
);""",

"""CREATE TABLE IF NOT EXISTS public.product_category_name_translation (
    col0 character varying(256),
    col1 character varying(256)
);""",

"""CREATE TABLE IF NOT EXISTS public.products (
    product_id character varying(256),
    product_category_name character varying(256),
    product_name_lenght integer,
    product_description_lenght integer,
    product_photos_qty integer,
    product_weight_g integer,
    product_length_cm integer,
    product_height_cm integer,
    product_width_cm integer
);""",

"""CREATE TABLE IF NOT EXISTS public.sellers (
    seller_id character varying(256),
    seller_zip_code_prefix integer,
    seller_city character varying(256),
    seller_state character varying(256)
);"""
]

## Function
def COMM_for_all_database(cursor, comn_typ):
    if comn_typ == "COPY":
        table_comm_list = COPY_table_comm_list
    elif comn_typ == "UNLOAD":
        table_comm_list = UNLOAD_table_comm_list
    elif comn_typ == "TRUNCATE":
        table_comm_list = TRUNCATE_table_comm_list
    elif comn_typ == "DROP":
        table_comm_list = DROP_table_comm_list
    elif comn_typ == "CREATE":
        table_comm_list = CREATE_table_comm_list
    else:
        print("COMN not found!!")
        return

    print(f"## {comn_typ} all TABLES ##")
    for comm in table_comm_list:
        print(comm)
        cursor.execute(comm)
    return

def COMM_for_table(cursor, comn_typ, table_name, data_tuple=None):
    if comn_typ == "SELECT":
        comm = f"SELECT * FROM public.{table_name};"
        print(comm)
        cursor.execute(comm)
    elif comn_typ == "INSERT":
        comm = f"INSERT INTO public.{table_name} VALUES {str(data_tuple)};"
        print(comm)
        cursor.execute(comm)
    else:
        print("COMN not found!!")
    return

def list_all_table(cursor, schema):
    cursor.execute(f"SHOW TABLES FROM SCHEMA {schema}.public;")
    table_list = cursor.fetchall()
    for table in table_list:
        print(table)
    