from logging import Logger
from typing import List

from examples.stg import EtlSetting, StgEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel
from datetime import datetime


class RankObj(BaseModel):
    id: int
    name: str
    bonus_percent: float
    min_payment_threshold: float

class UserObj(BaseModel):
    id: int
    order_user_id: str


class EventObj(BaseModel):
    id: int
    event_ts: datetime   
    event_type: str
    event_value: str   


class RanksOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_ranks(self, rank_threshold: int, limit: int) -> List[RankObj]:
        with self._db.client().cursor(row_factory=class_row(RankObj)) as cur:
            cur.execute(
                """
                    SELECT id, name, bonus_percent, min_payment_threshold
                    FROM ranks
                    WHERE id > %(threshold)s --Пропускаем те объекты, которые уже загрузили.
                    ORDER BY id ASC --Обязательна сортировка по id, т.к. id используем в качестве курсора.
                    LIMIT %(limit)s; --Обрабатываем только одну пачку объектов.
                """, {
                    "threshold": rank_threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs



    def list_users(self, threshold: int, limit: int) -> List[UserObj]:
        with self._db.client().cursor(row_factory=class_row(UserObj)) as cur:
            cur.execute(
                """
                SELECT 
                    id, 
                    order_user_id 
                FROM users
                WHERE id > %(threshold)s 
                ORDER BY id ASC
                LIMIT %(limit)s
                """,
                {"threshold": threshold, "limit": limit}
            )
            objs = cur.fetchall()  # забираем все строки
        return objs                # возвращаем результат


    def list_events(self, threshold: int, limit: int) -> List[EventObj]:
        with self._db.client().cursor(row_factory=class_row(EventObj)) as cur:
            cur.execute(
                """
                    SELECT
                        id,
                        event_type,
                        event_ts,
                        event_value
                    FROM outbox
                    WHERE id > %(threshold)s
                    ORDER BY id ASC
                    LIMIT %(limit)s
            """,
            {"threshold": threshold, "limit": limit}
        )
            objs = cur.fetchall()
        return objs

class RankDestRepository:

    def insert_rank(self, conn: Connection, rank: RankObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO stg.bonussystem_ranks(id, name, bonus_percent, min_payment_threshold)
                    VALUES (%(id)s, %(name)s, %(bonus_percent)s, %(min_payment_threshold)s)
                    ON CONFLICT (id) DO UPDATE
                    SET
                        name = EXCLUDED.name,
                        bonus_percent = EXCLUDED.bonus_percent,
                        min_payment_threshold = EXCLUDED.min_payment_threshold;
                """,
                {
                    "id": rank.id,
                    "name": rank.name,
                    "bonus_percent": rank.bonus_percent,
                    "min_payment_threshold": rank.min_payment_threshold
                },
            )

class UserDestRepository:
    """
    Сюда пишем методы для записи (insert/update) в stg.bonussystem_users.
    """
    def insert_user(self, conn: Connection, user: UserObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO stg.bonussystem_users(id, order_user_id)
                    VALUES (%(id)s, %(order_user_id)s)
                    ON CONFLICT (id) DO UPDATE
                       SET order_user_id = EXCLUDED.order_user_id
                """,
                {
                    "id": user.id,
                    "order_user_id": user.order_user_id
                },
            )

class EventsDestRepository:
    def insert_event(self, conn: Connection, event: EventObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO stg.bonussystem_events(
                        id,
                        event_ts,
                        event_type,
                        event_value
                    )
                    VALUES (%(id)s, %(event_ts)s, %(event_type)s, %(event_value)s);
                """,
                {
                    "id": event.id,
                    "event_ts": event.event_ts,
                    "event_type": event.event_type,
                    "event_value": event.event_value
                }
            )

class RankLoader:
    WF_KEY = "example_ranks_origin_to_stg_workflow"
    WF_KEY_USERS = "example_users_origin_to_stg_workflow" 
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 1000  # Рангов мало, но мы хотим продемонстрировать инкрементальную загрузку рангов.

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = RanksOriginRepository(pg_origin)
        self.stg = RankDestRepository()
        self.settings_repository = StgEtlSettingsRepository()
        self.log = log
        self.user_dest = UserDestRepository()

    def load_ranks(self):
        # открываем транзакцию.
        # Транзакция будет закоммичена, если код в блоке with пройдет успешно (т.е. без ошибок).
        # Если возникнет ошибка, произойдет откат изменений (rollback транзакции).
        with self.pg_dest.connection() as conn:

            # Прочитываем состояние загрузки
            # Если настройки еще нет, заводим ее.
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})

            # Вычитываем очередную пачку объектов.
            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.origin.list_ranks(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} ranks to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for rank in load_queue:
                self.stg.insert_rank(conn, rank)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")

    def load_users(self):
        with self.pg_dest.connection() as conn:
            # Используем отдельный ключ WF_KEY_USERS
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY_USERS)
            if not wf_setting:
                wf_setting = EtlSetting(
                    id=0,
                    workflow_key=self.WF_KEY_USERS,
                    workflow_settings={self.LAST_LOADED_ID_KEY: -1}
                )

            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.origin.list_users(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} users to load.")
            if not load_queue:
                self.log.info("No new users. Quitting.")
                return

            for user in load_queue:
                self.user_dest.insert_user(conn, user)

            # Обновляем прогресс
            new_last_id = max([u.id for u in load_queue])
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = new_last_id
            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Users load finished on {new_last_id}")

class BonusSystemLoader:
    WF_KEY_OUTBOX = "example_outbox_origin_to_stg_workflow"  # <-- ключ для events
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 10000  # если нужно брать батчами

    def __init__(self, pg_origin, pg_dest, log):
        self.pg_origin = pg_origin
        self.pg_dest = pg_dest
        self.log = log
        self.origin_repo = RanksOriginRepository(pg_origin)  
        self.events_dest_repo = EventsDestRepository()  

        self.settings_repository = StgEtlSettingsRepository()
    def load_events(self):
        with self.pg_dest.connection() as conn:
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY_OUTBOX)
            if not wf_setting:
                wf_setting = EtlSetting(
                    id=0,
                    workflow_key=self.WF_KEY_OUTBOX,
                    workflow_settings={self.LAST_LOADED_ID_KEY: -1}
                )

            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]

            # 2) прочитать список новых событий
            load_queue = self.origin_repo.list_events(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} new events to load.")
            if not load_queue:
                self.log.info("No new events. Quitting.")
                return

            # 3) вставить в stg.bonussystem_events
            for evt in load_queue:
                self.events_dest_repo.insert_event(conn, evt)

            # 4) обновить last_loaded_id
            new_last_id = max([e.id for e in load_queue])
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = new_last_id
            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Events load finished. Last loaded id = {new_last_id}")