import logging
from psycopg import Connection

class DMCourierLedgerLoader:
    def __init__(self, pg_dest: Connection, logger: logging.Logger):
        self.pg_dest = pg_dest
        self.log = logger

    def load_ledger(self):
        with self.pg_dest.connection() as conn:
            with conn.cursor() as cur:
                self.log.info("Loading data into cdm.dm_courier_ledger...")
                cur.execute("""
                    INSERT INTO cdm.dm_courier_ledger (
                        courier_id, courier_name, settlement_year, settlement_month,
                        orders_count, orders_total_sum, rate_avg, order_processing_fee,
                        courier_order_sum, courier_tips_sum, courier_reward_sum
                        )
                        SELECT
                            o.courier_id,
                            c.name AS courier_name,
                            t.year AS settlement_year,
                            t.month AS settlement_month,
                            COUNT(*) AS orders_count,
                            SUM(fps.total_sum) AS orders_total_sum,
                            AVG(fps.total_sum / fps.count) AS rate_avg,
                            SUM(fps.total_sum) * 0.25 AS order_processing_fee,
                        CASE
                            WHEN AVG(fps.total_sum / fps.count) < 4 THEN GREATEST(SUM(fps.total_sum) * 0.05, 100)
                            WHEN AVG(fps.total_sum / fps.count) < 4.5 THEN GREATEST(SUM(fps.total_sum) * 0.07, 150)
                            WHEN AVG(fps.total_sum / fps.count) < 4.9 THEN GREATEST(SUM(fps.total_sum) * 0.08, 175)
                            ELSE GREATEST(SUM(fps.total_sum) * 0.1, 200)
                            END AS courier_order_sum,
                        SUM(fps.bonus_payment) AS courier_tips_sum,
                        CASE
                            WHEN AVG(fps.total_sum / fps.count) < 4 THEN GREATEST(SUM(fps.total_sum) * 0.05, 100) + SUM(fps.bonus_payment) * 0.95
                            WHEN AVG(fps.total_sum / fps.count) < 4.5 THEN GREATEST(SUM(fps.total_sum) * 0.07, 150) + SUM(fps.bonus_payment) * 0.95
                            WHEN AVG(fps.total_sum / fps.count) < 4.9 THEN GREATEST(SUM(fps.total_sum) * 0.08, 175) + SUM(fps.bonus_payment) * 0.95
                            ELSE GREATEST(SUM(fps.total_sum) * 0.1, 200) + SUM(fps.bonus_payment) * 0.95
                            END AS courier_reward_sum
                    FROM dds.dm_orders o
                    JOIN dds.dm_couriers c ON c.id = o.courier_id
                    JOIN dds.dm_timestamps t ON t.id = o.timestamp_id
                    JOIN dds.fct_product_sales fps ON fps.order_id = o.id
                    GROUP BY o.courier_id, c.name, t.year, t.month;
                """)
                self.log.info("Data loaded successfully into cdm.dm_courier_ledger.")
