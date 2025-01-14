import logging
from lib import PgConnect
from lib.dict_util import str2json

class FctProductSalesLoader:
    def __init__(self, pg_dest: PgConnect, logger: logging.Logger):
        self.pg_dest = pg_dest
        self.log = logger

    def load_sales(self):
        with self.pg_dest.connection() as conn:
            with conn.cursor() as cur:
                self.log.info("Extracting data from stg.bonussystem_events...")

                # Прочитать данные с фильтром event_type = 'bonus_transaction'
                cur.execute("""
                    SELECT event_value
                    FROM stg.bonussystem_events
                    WHERE event_type = 'bonus_transaction';
                """)
                rows = cur.fetchall()
                self.log.info(f"Fetched {len(rows)} rows from stg.bonussystem_events.")

                for row in rows:
                    event_value = row[0]
                    json_data = str2json(event_value)

                    user_id = json_data.get("user_id")
                    order_id_key = json_data.get("order_id")
                    order_date = json_data.get("order_date")
                    products = json_data.get("product_payments", [])

                    # Найти order_id в dm_orders
                    cur.execute("""
                        SELECT id FROM dds.dm_orders
                        WHERE order_key = %s;
                    """, (order_id_key,))
                    order_id = cur.fetchone()

                    if not order_id:
                        self.log.warning(f"Order not found for order_key: {order_id_key}. Skipping.")
                        continue

                    for product in products:
                        product_source_id = product.get("product_id")
                        count = product.get("quantity")
                        price = product.get("price")
                        total_sum = product.get("product_cost")
                        bonus_payment = product.get("bonus_payment")
                        bonus_grant = product.get("bonus_grant")

                        # Найти product_id в dm_products
                        cur.execute("""
                            SELECT id FROM dds.dm_products
                            WHERE product_id = %s;
                        """, (product_source_id,))
                        product_id = cur.fetchone()

                        if not product_id:
                            self.log.warning(f"Product not found for product_id: {product_source_id}. Skipping.")
                            continue

                        # Вставить запись в fct_product_sales
                        cur.execute("""
                            INSERT INTO dds.fct_product_sales (
                                product_id, order_id, count, price, total_sum, bonus_payment, bonus_grant
                            )
                            VALUES (%s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT (product_id, order_id) DO UPDATE
                            SET
                                count = EXCLUDED.count,
                                price = EXCLUDED.price,
                                total_sum = EXCLUDED.total_sum,
                                bonus_payment = EXCLUDED.bonus_payment,
                                bonus_grant = EXCLUDED.bonus_grant;
                        """, (
                            product_id[0], order_id[0], count, price, total_sum, bonus_payment, bonus_grant
                        ))

                self.log.info("Data successfully loaded into dds.fct_product_sales.")
