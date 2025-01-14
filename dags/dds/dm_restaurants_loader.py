import logging
from lib import PgConnect
from lib.dict_util import str2json
from datetime import datetime

class DMRestaurantsLoader:
    def __init__(self, pg_dest: PgConnect, logger: logging.Logger):
        self.pg_dest = pg_dest
        self.log = logger

    def load_restaurants(self):
        with self.pg_dest.connection() as conn:
            with conn.cursor() as cur:
                self.log.info("Extracting data from stg.ordersystem_restaurants...")
                
                # Прочитать данные из stg.ordersystem_users
                cur.execute("SELECT object_id, object_value FROM stg.ordersystem_restaurants;")
                rows = cur.fetchall()
                self.log.info(f"Fetched {len(rows)} rows from stg.ordersystem_restaurants.")

                for row in rows:
                    object_id, object_value = row
                    json_data = str2json(object_value)  

    
                    restaurant_id = json_data.get("_id")
                    restaurant_name = json_data.get("name")
                    active_from = json_data.get("update_ts")
                    

                    if not restaurant_id or not restaurant_name or not active_from:
                        continue  

                    active_to = datetime(2099, 12, 31)

                    cur.execute("""
                        INSERT INTO dds.dm_restaurants (restaurant_id, restaurant_name, active_from, active_to)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (restaurant_id) DO UPDATE
                        SET
                        restaurant_name = EXCLUDED.restaurant_name,
                        active_from = EXCLUDED.active_from,
                        active_to = EXCLUDED.active_to;
                     """, (restaurant_id, restaurant_name, active_from, active_to))
                
                self.log.info("Data successfully loaded into dds.dm_restaurants.")