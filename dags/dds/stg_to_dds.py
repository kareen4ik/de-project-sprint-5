import logging
import pendulum
from airflow.decorators import dag, task
from lib import ConnectionBuilder
from dds.dm_users_loader import DMUsersLoader
from dds.dm_restaurants_loader import DMRestaurantsLoader
from dds.dm_timestamps_loader import DMTimestampsLoader
from dds.dm_products_loader import DMProductsLoader
from dds.dm_orders_loader import DMOrdersLoader
from dds.fct_product_sales_loader import FctProductSalesLoader
from dds.dds_couriers_loader import DDMCouriersLoader
from dds.dds_deliveries_loader import DDSDeliveriesLoader
from cdm.courier_ledger_loader import DMCourierLedgerLoader

log = logging.getLogger(__name__)

@dag(
    schedule_interval='0/15 * * * *',
    start_date=pendulum.datetime(2022, 1, 1, tz="UTC"),
    catchup=False,
    tags=['dds', 'example'],
    is_paused_upon_creation=True
)
def dds_pipeline_dag():
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    @task(task_id="users_load")
    def load_dm_users():
        loader = DMUsersLoader(dwh_pg_connect, log)
        loader.load_users()

    @task(task_id="restaurants_load")
    def load_dm_restaurants():
        loader = DMRestaurantsLoader(dwh_pg_connect, log)
        loader.load_restaurants()

    @task(task_id="timestamps_load")
    def load_dm_timestamps():
        loader = DMTimestampsLoader(dwh_pg_connect, log)
        loader.load_timestamps()

    @task(task_id="products_load")
    def load_dm_products():
        loader = DMProductsLoader(dwh_pg_connect, log)
        loader.load_products()

    @task(task_id="orders_load")
    def load_dm_orders():
        loader = DMOrdersLoader(dwh_pg_connect, log)
        loader.load_orders()

    @task(task_id="product_sales_load")
    def load_product_sales():
        loader = FctProductSalesLoader(dwh_pg_connect, log)
        loader.load_sales()

    @task(task_id="couriers_load")
    def load_dm_couriers():
        loader = DDMCouriersLoader(dwh_pg_connect, log)
        loader.load_couriers()

    @task(task_id="deliveries_load")
    def load_dds_deliveries():
        loader = DDSDeliveriesLoader(dwh_pg_connect, log)
        loader.load_deliveries()

    @task(task_id="load_courier_ledger")
    def load_courier_ledger():
        loader = DMCourierLedgerLoader(dwh_pg_connect, log)
        loader.load_ledger()

    users_task = load_dm_users()
    restaurants_task = load_dm_restaurants()
    timestamps_task = load_dm_timestamps()
    products_task = load_dm_products()
    orders_task = load_dm_orders()
    sales_task = load_product_sales()
    couriers_task = load_dm_couriers()
    deliveries_task = load_dds_deliveries()
    courier_ledger_task = load_courier_ledger()

    users_task >> restaurants_task >> timestamps_task >> products_task >> couriers_task >> deliveries_task >> orders_task >> sales_task >> courier_ledger_task

dds_dag = dds_pipeline_dag()