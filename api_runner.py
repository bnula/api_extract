import time
import json

import setup_logs
from send_log_email import send_email
from api_class import CallApi


def extract_all_metadata(
        api_realm,
        db,
        logger,
        start_date_override=None,
        end_date_override=None
):
    misc_config_file = "D:/Python/config_files/misc_api_config.json"
    with open(misc_config_file, "r", encoding="utf-8") as f:
        data = json.load(f)
        views = data["views"]
        for view in views:
            run_api = CallApi(
                api_realm=api_realm,
                view_name=view,
                db_connection=db,
                logger=logger,
                start_date_override=start_date_override,
                end_date_override=end_date_override
            )
            run_api.authenticate()
            run_api.submit_job()
            run_api.submit_job()
            run_api.download_zip_files()


def run_single_item_type(
    api_realm,
    view_name,
    db,
    logger,
    start_date_override=None,
    end_date_override=None
):
    run_api = CallApi(
        api_realm=api_realm,
        view_name=view_name,
        db_connection=db,
        logger=logger,
        start_date_override=start_date_override,
        end_date_override=end_date_override
    )
    run_api.authenticate()
    run_api.submit_job()
    run_api.submit_job()
    run_api.download_zip_files()


logger = setup_logs.setup_logger("API_Extract_All", "D:/Logs/API_Extract")
day_stamp = time.strftime("%Y-%m-%d")
try:
    extract_all_metadata(api_realm="prod", db="prod_server", logger=logger)
except Exception as e:
    logger.error(f"Exception - {e}")
    send_email("Ariba_Extract_All", "Failed", day_stamp)