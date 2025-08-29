from config.logger import setup_logger
from services.file_services import FileService
from services.queue_service import queue_check_hk_ta

route_logger = setup_logger("debug")
logger = route_logger

file_service = FileService()

async def hk_ta_initialise():
    hk_ta_token = await file_service.read_data_from_csv("hk_ta_token")
    logger.info(f"HK TA token: {hk_ta_token}")
    existing_task_id = hk_ta_token[0].get("task_id") if hk_ta_token else None
    if existing_task_id:
        return {"task_id": existing_task_id, "status": "ERROR", "message": "HK TA already started"}
    else:
        task = queue_check_hk_ta.delay()
        logger.info(f"HK TA check started, task_id: {task.id}")
        file_service.add_data_to_csv("hk_ta_token", [{"task_id": task.id}], ["task_id"])
        return {"task_id": task.id, "status": "QUEUED", "message": "HK TA check started"}