from datetime import datetime, timedelta
from config.settings import settings
from services.file_services import FileService
from services.queue_service import celery_app, prepare_hk_ta
from config.logger import setup_logger

logger = setup_logger("task_scheduler")

class TaskScheduler:
    def __init__(self):
        self.file_service = FileService()
        self.retry_file = settings.hk_ta_retry_schedule 
    
    async def schedule_retry_task(self, delay_hours: int = settings.daily_retry):
        """Add task to retry HK TA after delay_hours hours"""
        try:
            await self.cancel_existing_retry_task()
            
            delay_seconds = delay_hours * 3600 # ! REMOVE
            
            task = retry_hk_ta_task.apply_async(countdown=500)
            
            scheduled_time = datetime.now() + timedelta(hours=delay_hours)
            
            self.file_service.add_data_to_csv(
                self.retry_file, 
                [{"task_id": task.id}],
                ["task_id"]
            )
            
            logger.info(f"Retry task scheduled for: {delay_hours} hours, task_id: {task.id}")
            return {"status": "success", "task_id": task.id, "scheduled_for": scheduled_time}
            
        except Exception as e:
            logger.error(f"Error scheduling retry task: {e}")
            return {"status": "error", "message": str(e)}
    
    async def cancel_existing_retry_task(self):
        """Remove existing retry task"""
        try:
            retry_data = await self.file_service.read_data_from_csv(self.retry_file)
            
            if retry_data:
                task_id = retry_data[0].get("task_id")
                if task_id:
                    celery_app.control.revoke(task_id, terminate=True)
                    logger.info(f"Cancelled existing retry task: {task_id}")
                
                await self.file_service.clear_file_content(self.retry_file)
                logger.info("Cleared retry schedule file")
                
            return {"status": "success", "message": "Existing retry task cancelled"}
            
        except Exception as e:
            logger.error(f"Error cancelling retry task: {e}")
            return {"status": "error", "message": str(e)}

# Celery задача для повторного виклику
@celery_app.task(name='retry_hk_ta')
def retry_hk_ta_task():
    """Task that calls prepare_hk_ta after delay"""
    try:
        logger.info("Starting retry HK TA task")
        
        # Запускаємо prepare_hk_ta напряму
        task = prepare_hk_ta.delay()
        
        logger.info(f"Retry HK TA started, task_id: {task.id}")
        return {"status": "success", "task_id": task.id}
        
    except Exception as e:
        logger.error(f"Error in retry_hk_ta_task: {e}")
        raise e

