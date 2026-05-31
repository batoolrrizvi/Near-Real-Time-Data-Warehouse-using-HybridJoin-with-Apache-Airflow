# walmart_dw_dag.py
from datetime import datetime, timedelta
import os, csv, time, threading, queue
from collections import defaultdict, deque
import psycopg2, psycopg2.extras, pandas as pd
from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator

# DB credentials 
def get_db_credentials():
    conn = BaseHook.get_connection("postgres_walmart")
    return {
        "host": conn.host,
        "database": conn.schema,
        "user": conn.login,
        "password": conn.password,
        "port": conn.port,
    }

DB = get_db_credentials()

DATA_DIR = "/opt/airflow/data"

# DAG config 
default_args = {
    "owner": "batool_rizvi",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="walmart_dw_pipeline",
    default_args=default_args,
    description="Walmart DWH: schema → master data → hybrid join → queries",
    schedule=None,  
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["walmart", "etl", "data-warehouse"],
) as dag:

    # TASK 1: Create schema + tables 
    def create_schema():
        conn = psycopg2.connect(**DB)
        conn.autocommit = True
        cur = conn.cursor()

        cur.execute("CREATE SCHEMA IF NOT EXISTS WalmartDW;")
        cur.execute("SET search_path TO WalmartDW;")

        cur.execute("""
        DO $$ BEGIN
            CREATE TYPE WalmartDW.gender_enum AS ENUM ('M', 'F');
        EXCEPTION WHEN duplicate_object THEN NULL; END $$;
        """)
        cur.execute("""
        DO $$ BEGIN
            CREATE TYPE WalmartDW.marital_enum AS ENUM ('0', '1');
        EXCEPTION WHEN duplicate_object THEN NULL; END $$;
        """)
        cur.execute("""
        DO $$ BEGIN
            CREATE TYPE WalmartDW.age_group_enum AS ENUM ('0-17','18-25','26-35','36-45','46-50','51-55','55+');
        EXCEPTION WHEN duplicate_object THEN NULL; END $$;
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS WalmartDW.Customer (
            customer_id INT PRIMARY KEY,
            gender WalmartDW.gender_enum NOT NULL,
            age_group WalmartDW.age_group_enum NOT NULL,
            occupation INT NOT NULL,
            city_category VARCHAR(1) NOT NULL,
            marital_status WalmartDW.marital_enum NOT NULL,
            stay_in_current_city_years INT NOT NULL CHECK (stay_in_current_city_years >= 0)
        );""")

        cur.execute("""
        CREATE TABLE IF NOT EXISTS WalmartDW.Product (
            product_id TEXT PRIMARY KEY,
            product_category TEXT NOT NULL,
            price NUMERIC(12,2) NOT NULL CHECK (price > 0)
        );""")

        cur.execute("""
        CREATE TABLE IF NOT EXISTS WalmartDW.Store (
            store_id INTEGER PRIMARY KEY,
            storeName VARCHAR NOT NULL
        );""")

        cur.execute("""
        CREATE TABLE IF NOT EXISTS WalmartDW.Supplier (
            supplier_id INTEGER PRIMARY KEY,
            supplierName VARCHAR NOT NULL
        );""")

        cur.execute("""
        CREATE TABLE IF NOT EXISTS WalmartDW.Date (
            date_id INTEGER PRIMARY KEY,
            transaction_date DATE NOT NULL,
            dayNum INT NOT NULL,
            monthNum INT NOT NULL,
            year INT NOT NULL,
            dayofweek VARCHAR NOT NULL,
            quarter_num INTEGER NOT NULL,
            is_weekend BOOLEAN NOT NULL
        );""")

        cur.execute("""
        CREATE TABLE IF NOT EXISTS WalmartDW.Sales (
            sales_id INTEGER PRIMARY KEY,
            order_id INTEGER NOT NULL,
            customer_id INTEGER NOT NULL REFERENCES WalmartDW.Customer(customer_id),
            product_id TEXT NOT NULL REFERENCES WalmartDW.Product(product_id),
            date_id INTEGER NOT NULL REFERENCES WalmartDW.Date(date_id),
            store_id INTEGER NOT NULL REFERENCES WalmartDW.Store(store_id),
            supplier_id INTEGER NOT NULL REFERENCES WalmartDW.Supplier(supplier_id),
            sales_amount NUMERIC(12,2) NOT NULL CHECK (sales_amount >= 0),
            quantity INTEGER NOT NULL CHECK (quantity >= 0)
        );""")

        cur.execute("CREATE SEQUENCE IF NOT EXISTS WalmartDW.sales_id_seq START 1;")

        cur.execute("INSERT INTO WalmartDW.Supplier (supplier_id, supplierName) VALUES (1, 'Default Supplier') ON CONFLICT DO NOTHING;")
        cur.execute("INSERT INTO WalmartDW.Store (store_id, storeName) VALUES (1, 'Default Store') ON CONFLICT DO NOTHING;")

        cur.close()
        conn.close()
        print("[Schema created successfully]")

    create_schema_task = PythonOperator(
        task_id="create_schema",
        python_callable=create_schema,
    )

    # TASK 2: Load master data 
    def load_master_data():
        conn = psycopg2.connect(**DB)
        conn.autocommit = False
        cur = conn.cursor()
        cur.execute("SET search_path TO WalmartDW;")

        product_md = pd.read_csv(os.path.join(DATA_DIR, "product_master_data.csv"))
        customer_md = pd.read_csv(os.path.join(DATA_DIR, "customer_master_data.csv"))

        supplier_df = product_md[["supplierID", "supplierName"]].drop_duplicates()
        store_df = product_md[["storeID", "storeName"]].drop_duplicates()
        product_df = product_md[["Product_ID", "Product_Category", "price$"]].drop_duplicates()

        cur.executemany(
            "INSERT INTO WalmartDW.Supplier (supplier_id, supplierName) VALUES (%s,%s) ON CONFLICT DO NOTHING;",
            list(supplier_df.itertuples(index=False, name=None)),
        )
        cur.executemany(
            "INSERT INTO WalmartDW.Store (store_id, storeName) VALUES (%s,%s) ON CONFLICT DO NOTHING;",
            list(store_df.itertuples(index=False, name=None)),
        )
        cur.executemany(
            "INSERT INTO WalmartDW.Product (product_id, product_category, price) VALUES (%s,%s,%s) ON CONFLICT DO NOTHING;",
            list(product_df.itertuples(index=False, name=None)),
        )
        customer_vals = list(
            customer_md[["Customer_ID", "Gender", "Age", "Occupation",
                          "City_Category", "Marital_Status", "Stay_In_Current_City_Years"]]
            .astype({"Marital_Status": str}) 
            .itertuples(index=False, name=None)
        )
        cur.executemany(
            """INSERT INTO WalmartDW.Customer
               (customer_id,gender,age_group,occupation,city_category,marital_status,stay_in_current_city_years)
               VALUES (%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING;""",
            customer_vals,
        )
        conn.commit()
        cur.close()
        conn.close()
        print(f"[Master data loaded] suppliers={len(supplier_df)} stores={len(store_df)} "
              f"products={len(product_df)} customers={len(customer_vals)}")

    load_master_data_task = PythonOperator(
        task_id="load_master_data",
        python_callable=load_master_data,
    )

    # TASK 3: Hybrid Join 
    def run_hybrid_join():
        from datetime import datetime as dt

        CSV_PATH = os.path.join(DATA_DIR, "transactional_data.csv")
        HASH_SLOTS = 10000
        STREAM_BUFFER_SIZE = 5000
        BATCH_SIZE = 1000
        COMMIT_INTERVAL = 5000
        DEFAULT_SUPPLIER = 1
        DEFAULT_STORE = 1

        conn = psycopg2.connect(**DB)
        conn.autocommit = False
        cur  = conn.cursor()
        cur.execute("SET search_path TO WalmartDW;")

        product_cache  = {}
        date_cache = {}
        customer_cache = set()
        pending_dates = {}

        cur.execute("SELECT product_id, price FROM WalmartDW.Product;")
        for r in cur.fetchall():
            product_cache[str(r[0])] = {"price": float(r[1] or 0),
                                         "supplier_id": DEFAULT_SUPPLIER,
                                         "store_id":    DEFAULT_STORE}

        cur.execute("SELECT transaction_date, date_id FROM WalmartDW.Date;")
        for r in cur.fetchall():
            date_cache[r[0].strftime("%Y-%m-%d")] = r[1]

        cur.execute("SELECT customer_id FROM WalmartDW.Customer;")
        for r in cur.fetchall():
            customer_cache.add(r[0])

        print(f"[Cache] products={len(product_cache)} dates={len(date_cache)} customers={len(customer_cache)}")

        cur.execute("SELECT last_value FROM WalmartDW.sales_id_seq;")
        counter = [cur.fetchone()[0]]
        counter_lock = threading.Lock()

        def next_id():
            with counter_lock:
                counter[0] += 1
                return counter[0]

        def parse_date(s):
            for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%m/%d/%Y", "%Y/%m/%d"):
                try: return dt.strptime(s, fmt).date()
                except: pass
            return dt.today().date()

        def flush_dates():
            if not pending_dates: return
            cur.execute("SELECT COALESCE(MAX(date_id),0) FROM WalmartDW.Date;")
            nid = cur.fetchone()[0] + 1
            rows = []
            for ds, p in pending_dates.items():
                rows.append((nid, p, p.day, p.month, p.year,
                              p.strftime("%A"), (p.month-1)//3+1, p.weekday()>=5))
                date_cache[ds] = nid
                nid += 1
            psycopg2.extras.execute_values(cur,
                "INSERT INTO WalmartDW.Date (date_id,transaction_date,dayNum,monthNum,year,"
                "dayofweek,quarter_num,is_weekend) VALUES %s ON CONFLICT DO NOTHING;",
                rows, page_size=100)
            pending_dates.clear()

        def get_date_id(date_str):
            if date_str in date_cache: return date_cache[date_str]
            p = parse_date(date_str)
            key = p.strftime("%Y-%m-%d")
            if key in date_cache: return date_cache[key]
            pending_dates[key] = p
            if len(pending_dates) >= 100: flush_dates()
            return date_cache.get(key, 1)

        stream_buf = queue.Queue(maxsize=STREAM_BUFFER_SIZE)
        htable = defaultdict(list)
        arrival_q  = deque()
        free = [HASH_SLOTS]
        used = [0]
        lock = threading.Lock()
        done = [False]

        def stream_loader():
            with open(CSV_PATH, newline="", encoding="utf-8") as f:
                count = 0
                for row in csv.DictReader(f):
                    stream_buf.put({k.strip(): v.strip() if isinstance(v,str) else v
                              for k,v in row.items()})
                    count += 1
                    if count % 10000 == 0: print(f"  [Stream] {count} rows pushed")
            done[0] = True
            print(f"[Stream done] total={count}")

        def join_worker():
            ibuf = []
            total = 0
            last = 0

            while True:
                while free[0] > 0 and not stream_buf.empty():
                    tup = stream_buf.get()
                    try: key = int(tup.get("Customer_ID") or tup.get("customer_id"))
                    except: continue
                    with lock:
                        htable[key].append(tup)
                        arrival_q.append(key)
                        free[0] -= 1
                        used[0] += 1

                if used[0] == 0 and done[0]:
                    if ibuf:
                        psycopg2.extras.execute_values(cur,
                            "INSERT INTO WalmartDW.Sales (sales_id,order_id,customer_id,product_id,"
                            "date_id,store_id,supplier_id,sales_amount,quantity) VALUES %s;",
                            ibuf, page_size=500)
                        flush_dates()
                        conn.commit()
                    print(f"[HybridJoin done] total_inserted={total}")
                    break

                if used[0] == 0 or not arrival_q:
                    time.sleep(0.01)
                    continue

                key = arrival_q[0]
                if key not in customer_cache:
                    with lock:
                        freed = len(htable[key])
                        used[0] -= freed; free[0] += freed
                        del htable[key]
                        while arrival_q and arrival_q[0] == key:
                            arrival_q.popleft()
                    continue

                for tup in list(htable.get(key, [])):
                    oid = tup.get("orderID") or tup.get("order_id")
                    pid = str(tup.get("Product_ID") or tup.get("product_id") or "").strip()
                    qty = tup.get("quantity")
                    date = tup.get("date")

                    if not all([oid, pid, qty, date]):
                        with lock:
                            try: htable[key].remove(tup); used[0]-=1; free[0]+=1
                            except: pass
                        continue

                    try:   
                        qty = int(float(qty))
                    except: 
                        qty = 0

                    info = product_cache.get(pid, {"price":0.0,
                               "supplier_id":DEFAULT_SUPPLIER,"store_id":DEFAULT_STORE})
                    amt = round(qty * info["price"], 2)
                    did = get_date_id(date)
                    sid = next_id()

                    ibuf.append((int(sid), int(oid), int(key), pid,
                                  int(did), int(info["store_id"]),
                                  int(info["supplier_id"]), float(amt), int(qty)))
                    total += 1

                    with lock:
                        try: htable[key].remove(tup); used[0]-=1; free[0]+=1
                        except: pass

                    if len(ibuf) >= BATCH_SIZE:
                        psycopg2.extras.execute_values(cur,
                            "INSERT INTO WalmartDW.Sales (sales_id,order_id,customer_id,product_id,"
                            "date_id,store_id,supplier_id,sales_amount,quantity) VALUES %s;",
                            ibuf, page_size=500)
                        ibuf.clear()
                        if total - last >= COMMIT_INTERVAL:
                            flush_dates(); conn.commit()
                            last = total
                            print(f"  [Committed] {total} records")

                while arrival_q and len(htable.get(arrival_q[0], [])) == 0:
                    arrival_q.popleft()

            flush_dates()
            cur.execute("SELECT setval('WalmartDW.sales_id_seq',%s);", (counter[0],))
            conn.commit()
            cur.close()
            conn.close()

        t1 = threading.Thread(target=stream_loader, daemon=True)
        t2 = threading.Thread(target=join_worker, daemon=True)
        t1.start()
        t2.start()
        t1.join()
        t2.join()

    run_hybrid_join_task = PythonOperator(
        task_id="run_hybrid_join",
        python_callable=run_hybrid_join,
        execution_timeout=timedelta(hours=2),
    )

    # TASK 4: Run analytical queries 
    def run_queries():
        conn = psycopg2.connect(**DB)
        conn.autocommit = True
        cur = conn.cursor()

        with open("/opt/airflow/dags/queries.sql", "r") as f:
            sql = f.read()

        # split and run statement by statement, skip blanks
        for statement in sql.split(";"):
            clean = statement.strip()
            if clean:
                try:
                    cur.execute(clean)
                except Exception as e:
                    print(f"[Query warning] {e}")

        cur.close()
        conn.close()
        print("[All queries executed]")

    run_queries_task = PythonOperator(
        task_id="run_analytical_queries",
        python_callable=run_queries,
    )

    # Task order 
    create_schema_task >> load_master_data_task >> run_hybrid_join_task >> run_queries_task
