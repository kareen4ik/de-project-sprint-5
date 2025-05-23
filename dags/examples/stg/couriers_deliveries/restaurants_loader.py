from datetime import datetime
from logging import Logger
from lib import PgConnect
from examples.stg.couriers_deliveries.restaurants_pg_saver import RestaurantsPgSaver
from examples.stg.couriers_deliveries.restaurants_reader import RestaurantsReader
from examples.stg.stg_settings_repository import EtlSetting, StgEtlSettingsRepository
from lib.dict_util import json2str

class RestaurantsLoader:
    _LOG_THRESHOLD = 2
    _SESSION_LIMIT = 10000

    WF_KEY = "example_restaurant_data_origin_to_stg_workflow"
    LAST_LOADED_TS_KEY = "last_loaded_ts"

    def __init__(self, collection_loader: RestaurantsReader, pg_dest: PgConnect, pg_saver: RestaurantsPgSaver, logger: Logger) -> None:
        self.collection_loader = collection_loader
        self.pg_saver = pg_saver
        self.pg_dest = pg_dest
        self.settings_repository = StgEtlSettingsRepository()
        self.log = logger

    def run_copy(self) -> int:
        with self.pg_dest.connection() as conn:
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(
                    id=0,
                    workflow_key=self.WF_KEY,
                    workflow_settings={
                        self.LAST_LOADED_TS_KEY: datetime(2022, 1, 1).isoformat()
                    }
                )

            last_loaded_ts_str = wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY]
            last_loaded_ts = datetime.fromisoformat(last_loaded_ts_str)
            self.log.info(f"Starting to load from last checkpoint: {last_loaded_ts}")

            load_queue = self.collection_loader.get_restaurants(limit=self._SESSION_LIMIT)
            self.log.info(f"Found {len(load_queue)} restaurants to sync.")
            if not load_queue:
                self.log.info("No new data. Exiting.")
                return 0

            max_updated_at = datetime.now()
            i = 0

            for restaurant in load_queue:
                try:
                    restaurant_id = restaurant.get("_id") 
                    updated_at = max_updated_at

                    self.pg_saver.save_object(
                        conn=conn,
                        restaurant_id=restaurant_id,
                        updated_at=updated_at,
                        val=restaurant
                    )

                    i += 1
                    if i % self._LOG_THRESHOLD == 0:
                        self.log.info(f"Processed {i} restaurants out of {len(load_queue)}.")
                except Exception as e:
                    self.log.error(f"Failed to process restaurant: {restaurant}. Error: {e}")

            wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY] = max_updated_at.isoformat()
            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.settings_repository.save_setting(conn, self.WF_KEY, wf_setting_json)

            self.log.info(f"Finishing work. Last checkpoint: {wf_setting_json}")

            return len(load_queue)
