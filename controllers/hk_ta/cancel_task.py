from config.logger import setup_logger
from services.file_services import FileService
from services.queue_service import cancel_task

route_logger = setup_logger("debug")
logger = route_logger

file_service = FileService()

async def hk_ta_cancel_task(task_id: str):
    hk_ta_token = await file_service.read_data_from_csv("hk_ta_token")
    logger.info(f"HK TA token: {hk_ta_token}")
    existing_task_id = hk_ta_token[0].get("task_id") if hk_ta_token else None
    if existing_task_id == task_id:
        await file_service.clear_file_content("hk_ta_token")
        return cancel_task(task_id)
    else:
        return { "task_id": task_id, 'status': "ERROR", "error": "Task ID not found or not match"}