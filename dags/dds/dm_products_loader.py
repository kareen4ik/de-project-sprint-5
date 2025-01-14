import logging
from lib import PgConnect
from lib.dict_util import str2json

class DMProductsLoader:
    def __init__(self, pg_dest: PgConnect, logger: logging.Logger):
        self.pg_dest = pg_dest
        self.log = logger

    def load_products(self):
        with self.pg_dest.connection() as conn:
            with conn.cursor() as cur:
                self.log.info("Extracting data from stg.ordersystem_restaurants...")

                # Получение данных из stg.ordersystem_restaurants
                cur.execute("SELECT object_id, object_value, update_ts FROM stg.ordersystem_restaurants;")
                rows = cur.fetchall()
                self.log.info(f"Fetched {len(rows)} rows from stg.ordersystem_restaurants.")

                for row in rows:
                    object_id, object_value, update_ts = row
                    json_data = str2json(object_value)  # Преобразуем JSON

                    # Получаем ресторан из JSON
                    restaurant_name = json_data.get("name")
                    menu_items = json_data.get("menu", [])

                    # Получаем ID ресторана из dds.dm_restaurants
                    cur.execute("""
                        SELECT id FROM dds.dm_restaurants
                        WHERE restaurant_id = %s;
                    """, (object_id,))
                    restaurant_row = cur.fetchone()
                    if not restaurant_row:
                        self.log.warning(f"Restaurant {object_id} not found in dds.dm_restaurants. Skipping...")
                        continue

                    restaurant_id = restaurant_row[0]

                    # Добавляем продукты в dds.dm_products
                    for item in menu_items:
                        product_id = item.get("_id")
                        product_name = item.get("name")
                        product_price = item.get("price")

                        if not product_id or not product_name or not product_price:
                            self.log.warning(f"Invalid product data: {item}. Skipping...")
                            continue

                        cur.execute("""
                            INSERT INTO dds.dm_products (
                                product_id, product_name, product_price, active_from, active_to, restaurant_id
                            )
                            VALUES (%s, %s, %s, %s, %s, %s)
                            ON CONFLICT (product_id) DO UPDATE
                            SET
                                product_name = EXCLUDED.product_name,
                                product_price = EXCLUDED.product_price,
                                active_from = EXCLUDED.active_from,
                                active_to = EXCLUDED.active_to,
                                restaurant_id = EXCLUDED.restaurant_id;
                        """, (product_id, product_name, product_price, update_ts, "2099-12-31", restaurant_id))

                self.log.info("Data successfully loaded into dds.dm_products.")
