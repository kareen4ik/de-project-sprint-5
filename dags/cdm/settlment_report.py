from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.decorators import dag, task
from lib import ConnectionBuilder
from datetime import datetime
import logging
from lib import PgConnect


def update_settlement_report():
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")
    query = """
    INSERT INTO cdm.dm_settlement_report (
        restaurant_id, 
        restaurant_name, 
        settlement_date, 
        orders_count, 
        orders_total_sum, 
        orders_bonus_payment_sum, 
        orders_bonus_granted_sum, 
        order_processing_fee, 
        restaurant_reward_sum
    )
    SELECT
        r.id AS restaurant_id,
        r.restaurant_name,
        DATE(t.ts) AS settlement_date,
        COUNT(distinct o.id) AS orders_count,
        SUM(COALESCE(ps.total_sum, 0)) AS orders_total_sum,
        SUM(COALESCE(ps.bonus_payment, 0)) AS orders_bonus_payment_sum,
        SUM(COALESCE(ps.bonus_grant, 0)) AS orders_bonus_granted_sum,
        SUM(COALESCE(ps.total_sum, 0)) * 0.25 AS order_processing_fee,
        SUM(COALESCE(ps.total_sum, 0)) - SUM(COALESCE(ps.bonus_payment, 0)) - SUM(COALESCE(ps.total_sum, 0)) * 0.25 AS restaurant_reward_sum
    FROM
        dds.dm_orders o
    JOIN
        dds.dm_restaurants r ON o.restaurant_id = r.id
    JOIN
        dds.dm_timestamps t ON o.timestamp_id = t.id
    JOIN
        dds.fct_product_sales ps ON o.id = ps.order_id
    WHERE
        o.order_status = 'CLOSED'
    GROUP BY
        r.id, r.restaurant_name, DATE(t.ts)
    ON CONFLICT (restaurant_id, settlement_date) DO UPDATE
    SET
        orders_count = EXCLUDED.orders_count,
        orders_total_sum = EXCLUDED.orders_total_sum,
        orders_bonus_payment_sum = EXCLUDED.orders_bonus_payment_sum,
        orders_bonus_granted_sum = EXCLUDED.orders_bonus_granted_sum,
        order_processing_fee = EXCLUDED.order_processing_fee,
        restaurant_reward_sum = EXCLUDED.restaurant_reward_sum;
    """
    with dwh_pg_connect.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            conn.commit()

default_args = {
    'start_date': datetime(2022, 1, 1),
    'retries': 3
}

with DAG(
    dag_id="settlement_report_dag",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False
) as dag:
    
    load_settlement_report = PythonOperator(
        task_id="update_settlement_report",
        python_callable=update_settlement_report
    )

    load_settlement_report
