import logging
from psycopg import Connection

class DDSDeliveriesLoader:
    def __init__(self, pg_dest: Connection, logger: logging.Logger):
        self.pg_dest = pg_dest
        self.log = logger

    def load_deliveries(self):
        with self.pg_dest.connection() as conn:
            with conn.cursor() as cur:
                self.log.info("Extracting and parsing data from stg.api_deliveries...")

                cur.execute("""
                    SELECT
                        (object_value ->> 'delivery_id')::VARCHAR(50) AS delivery_id,
                        (object_value ->> 'order_ts')::TIMESTAMP AS order_ts,
                        (object_value ->> 'order_id')::VARCHAR(50) AS order_api_id,
                        (object_value ->> 'courier_id')::VARCHAR(50) AS courier_api_id,
                        (object_value ->> 'address')::TEXT AS address,
                        (object_value ->> 'delivery_ts')::TIMESTAMP AS delivery_ts,
                        (object_value ->> 'rate')::INT AS rate,
                        (object_value ->> 'sum')::DECIMAL(10, 2) AS sum,
                        (object_value ->> 'tip_sum')::DECIMAL(10, 2) AS tip_sum,
                        updated_at
                    FROM stg.api_deliveries;
                """)
                rows = cur.fetchall()
                self.log.info(f"Fetched {len(rows)} rows from stg.api_deliveries.")

                for row in rows:
                    delivery_id, order_ts, order_api_id, courier_api_id, address, delivery_ts, rate, sum_, tip_sum, updated_at = row

                    cur.execute(
                        """
                        SELECT id FROM dds.dm_orders
                        WHERE order_key = %s;
                        """,
                        (order_api_id,)
                    )
                    order_id = cur.fetchone()
                    if not order_id:
                        self.log.warning(f"Order not found for order_key {order_api_id}. Skipping.")
                        continue

                    cur.execute(
                        """
                        SELECT id FROM dds.dm_couriers
                        WHERE courier_id = %s;
                        """,
                        (courier_api_id,)
                    )
                    courier_id = cur.fetchone()
                    if not courier_id:
                        self.log.warning(f"Courier not found for courier_id {courier_api_id}. Skipping.")
                        continue

                    cur.execute(
                        """
                        INSERT INTO dds.dm_deliveries (
                            delivery_id,
                            order_ts,
                            order_id,
                            courier_id,
                            address,
                            delivery_ts,
                            rate,
                            sum,
                            tip_sum,
                            updated_at
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (delivery_id) DO UPDATE
                        SET
                            order_ts = EXCLUDED.order_ts,
                            order_id = EXCLUDED.order_id,
                            courier_id = EXCLUDED.courier_id,
                            address = EXCLUDED.address,
                            delivery_ts = EXCLUDED.delivery_ts,
                            rate = EXCLUDED.rate,
                            sum = EXCLUDED.sum,
                            tip_sum = EXCLUDED.tip_sum,
                            updated_at = EXCLUDED.updated_at;
                        """,
                        (delivery_id, order_ts, order_id[0], courier_id[0], address, delivery_ts, rate, sum_, tip_sum, updated_at)
                    )

                self.log.info("Data successfully loaded into dds.dm_deliveries.")
