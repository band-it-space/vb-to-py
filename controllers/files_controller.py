import os
import zipfile
import io
from fastapi.responses import StreamingResponse
from fastapi import HTTPException

from services.file_services import FileService
from config.settings import settings
from config.logger import setup_logger

logger = setup_logger("files_controller")


async def download_csv_files():
    """
    Download CSV files as ZIP archive from the data directory
    """
    try:
        file_service = FileService()
        filenames = [
            settings.test_db_table_ta1,
            settings.test_db_table_ta2, 
            settings.test_db_table_energy
        ]
        
        # Create ZIP file in memory
        zip_buffer = io.BytesIO()
        
        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
            files_added = 0
            
            for filename in filenames:
                try:
                    file_path = f"{file_service.data_dir}/{filename}.csv"
                    if os.path.exists(file_path):
                        # Read file content
                        with open(file_path, 'r', encoding='utf-8') as file:
                            content = file.read()
                        
                        # Add to ZIP
                        zip_file.writestr(f"{filename}.csv", content)
                        files_added += 1
                        logger.info(f"Successfully added file to ZIP: {filename}.csv")
                    else:
                        logger.warning(f"File not found: {filename}.csv")
                except Exception as e:
                    logger.error(f"Error reading file {filename}.csv: {e}")
        
        if files_added == 0:
            raise HTTPException(
                status_code=404,
                detail="No CSV files found"
            )
        
        # Reset buffer position
        zip_buffer.seek(0)
        
        # Return ZIP file as streaming response
        return StreamingResponse(
            io.BytesIO(zip_buffer.read()),
            media_type="application/zip",
            headers={"Content-Disposition": "attachment; filename=csv_files.zip"}
        )
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error creating ZIP file: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Error creating ZIP file: {str(e)}"
        )
