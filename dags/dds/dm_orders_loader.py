import logging
from lib import PgConnect
from lib.dict_util import str2json

class DMOrdersLoader:
    def __init__(self, pg_dest: PgConnect, logger: logging.Logger):
        self.pg_dest = pg_dest
        self.log = logger

    def load_orders(self):
        with self.pg_dest.connection() as conn:
            with conn.cursor() as cur:
                self.log.info("Extracting data from stg.ordersystem_orders...")
                cur.execute("SELECT object_value FROM stg.ordersystem_orders;")
                rows = cur.fetchall()
                self.log.info(f"Fetched {len(rows)} rows from stg.ordersystem_orders.")

                for row in rows:
                    object_value = row[0]
                    json_data = str2json(object_value)

                    order_key = json_data.get("_id")
                    order_status = json_data.get("final_status")
                    order_date = json_data.get("date")
                    restaurant_source_id = json_data.get("restaurant", {}).get("id")
                    user_source_id = json_data.get("user", {}).get("id")
                    courier_source_id = json_data.get("courier", {}).get("id")  # Assuming courier ID is in the JSON
                    
                    if not all([order_key, order_status, order_date, restaurant_source_id, user_source_id]):
                        self.log.warning(f"Missing data in order {order_key}. Skipping.")
                        continue

                    cur.execute(
                        """
                        SELECT id FROM dds.dm_restaurants
                        WHERE restaurant_id = %s;
                        """,
                        (restaurant_source_id,)
                    )
                    restaurant_id = cur.fetchone()

                    if not restaurant_id:
                        self.log.warning(f"Restaurant not found for {restaurant_source_id}. Skipping.")
                        continue


                    cur.execute(
                        """
                        SELECT id FROM dds.dm_users
                        WHERE user_id = %s;
                        """,
                        (user_source_id,)
                    )
                    user_id = cur.fetchone()

                    if not user_id:
                        self.log.warning(f"User not found for {user_source_id}. Skipping.")
                        continue

                    cur.execute(
                        """
                        SELECT id FROM dds.dm_timestamps
                        WHERE ts = %s;
                        """,
                        (order_date,)
                    )
                    timestamp_id = cur.fetchone()

                    if not timestamp_id:
                        self.log.warning(f"Timestamp not found for {order_date}. Skipping.")
                        continue

                    courier_id = None
                    if courier_source_id:
                        cur.execute(
                            """
                            SELECT id FROM dds.dm_couriers
                            WHERE courier_id = %s;
                            """,
                            (courier_source_id,)
                        )
                        courier_id = cur.fetchone()
                        if not courier_id:
                            self.log.warning(f"Courier not found for {courier_source_id}. Skipping courier information.")
                        else:
                            courier_id = courier_id[0]

                    cur.execute(
                        """
                        INSERT INTO dds.dm_orders (
                            order_key, order_status, restaurant_id, timestamp_id, user_id, courier_id
                        )
                        VALUES (%s, %s, %s, %s, %s, %s)
                        ON CONFLICT (order_key) DO UPDATE
                        SET
                            order_status = EXCLUDED.order_status,
                            restaurant_id = EXCLUDED.restaurant_id,
                            timestamp_id = EXCLUDED.timestamp_id,
                            user_id = EXCLUDED.user_id,
                            courier_id = EXCLUDED.courier_id;
                        """,
                        (order_key, order_status, restaurant_id[0], timestamp_id[0], user_id[0], courier_id)
                    )

                self.log.info("Data successfully loaded into dds.dm_orders.")
