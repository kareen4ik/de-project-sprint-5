import logging
import pendulum
from airflow.decorators import dag, task
from lib import ConnectionBuilder
from examples.stg.couriers_deliveries.courier_pg_saver import CourierPgSaver
from examples.stg.couriers_deliveries.courier_loader import CourierLoader
from examples.stg.couriers_deliveries.courier_reader import CourierReader
from examples.stg.couriers_deliveries.deliveries_pg_saver import DeliveriesPgSaver
from examples.stg.couriers_deliveries.deliveries_loader import DeliveriesLoader
from examples.stg.couriers_deliveries.deliveries_reader import DeliveriesReader
from examples.stg.couriers_deliveries.restaurants_pg_saver import RestaurantsPgSaver
from examples.stg.couriers_deliveries.restaurants_loader import RestaurantsLoader
from examples.stg.couriers_deliveries.restaurants_reader import RestaurantsReader

log = logging.getLogger(__name__)

@dag(
    schedule_interval='*/15 * * * *', 
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=['sprint5', 'project', 'stg', 'origin'],
    is_paused_upon_creation=True
)
def couriers_deliveries_pipeline():
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    API_NICKNAME = "rinchen.helmut"
    API_COHORT = "32"
    API_KEY = "25c27781-8fde-4b30-a22e-524044a7580f"

    @task(task_id="couriers_loading")
    def load_couriers():
        pg_saver = CourierPgSaver()
        reader = CourierReader(nickname=API_NICKNAME, cohort=API_COHORT, api_key=API_KEY)
        loader = CourierLoader(collection_loader=reader, pg_dest=dwh_pg_connect, pg_saver=pg_saver, logger=log)
        loader.run_copy()

    @task(task_id="restaurants_loading")
    def load_restaurants():
        pg_saver = RestaurantsPgSaver()
        reader = RestaurantsReader(nickname=API_NICKNAME, cohort=API_COHORT, api_key=API_KEY)
        loader = RestaurantsLoader(collection_loader=reader, pg_dest=dwh_pg_connect, pg_saver=pg_saver, logger=log)
        loader.run_copy()

    @task(task_id="deliveries_loading")
    def load_deliveries():
        pg_saver = DeliveriesPgSaver()
        reader = DeliveriesReader(nickname=API_NICKNAME, cohort=API_COHORT, api_key=API_KEY)
        loader = DeliveriesLoader(collection_loader=reader, pg_dest=dwh_pg_connect, pg_saver=pg_saver, logger=log)
        loader.run_copy()

    couriers_task = load_couriers()
    restaurants_task = load_restaurants()
    deliveries_task = load_deliveries()

    couriers_task >> restaurants_task >> deliveries_task


couriers_deliveries_dag = couriers_deliveries_pipeline()
