import logging

import pendulum
from airflow.decorators import dag, task
from examples.stg.bonus_system_ranks_dag.ranks_loader import RankLoader
from examples.stg.bonus_system_ranks_dag.ranks_loader import BonusSystemLoader
from lib import ConnectionBuilder

log = logging.getLogger(__name__)


@dag(
    schedule_interval='0/15 * * * *',  
    start_date=pendulum.datetime(2022, 1, 1, tz="UTC"), 
    catchup=False,  
    tags=['sprint5', 'stg', 'origin', 'example'], 
    is_paused_upon_creation=True  
)
def sprint5_example_stg_bonus_system_ranks_dag():
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")
    origin_pg_connect = ConnectionBuilder.pg_conn("PG_ORIGIN_BONUS_SYSTEM_CONNECTION")

    @task(task_id="ranks_load")
    def load_ranks():
        rest_loader = RankLoader(origin_pg_connect, dwh_pg_connect, log)
        rest_loader.load_ranks()  

    ranks_dict = load_ranks()

    @task(task_id="users_load")
    def load_users():
        rest_loader = RankLoader(origin_pg_connect, dwh_pg_connect, log)
        rest_loader.load_users()
    
    users_dict = load_users()

    @task(task_id="events_load")
    def load_events_task():
        loader = BonusSystemLoader(origin_pg_connect, dwh_pg_connect, log)
        loader.load_events()
    

    events_dict = load_events_task()
    ranks_dict >> users_dict  >> events_dict 


stg_bonus_system_ranks_dag = sprint5_example_stg_bonus_system_ranks_dag()
