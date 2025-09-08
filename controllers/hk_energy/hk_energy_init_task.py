from celery import chord

from config.settings import Settings
from config.logger import setup_logger
from services.file_services import FileService
from services.queue_service import prepare_hk_energy_task

route_logger = setup_logger("iniitalise")
logger = route_logger
settings = Settings()

file_service = FileService()

async def hk_energy_initialise(trade_day: str):
    # Create task and save token
    hk_energy_token = await file_service.read_data_from_csv(settings.hk_energy_token_file_name)

    # Clear old data from file
    await file_service.clear_file_content(settings.signal_file_name)

    existing_task_id = hk_energy_token[0].get("task_id") if hk_energy_token else None

    if existing_task_id:
        return {"task_id": existing_task_id, "status": "ERROR", "message": "HK Energy already started"}
    else:
        file_service.add_data_to_csv(settings.hk_energy_token_file_name, [{"task_id": '001'}], ["task_id"])
        
        try:
            task_result = prepare_hk_energy_task.delay(trade_day)
            logger.info(f"Prepare task started with ID: {task_result.id}")
            
        except Exception as e:
            logger.error(f"Error creating tasks: {e}")
            await file_service.clear_file_content(settings.hk_energy_token_file_name)
            raise
            
    return {"task_id": '001', "status": "QUEUED", "message": "HK Energy started"}
