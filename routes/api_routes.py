from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Dict, Any
from services.hk_algo import HKAlgo
from config.logger import setup_logger


route_logger = setup_logger("api_routes")
logger = route_logger


router = APIRouter()

class SalgoRequest(BaseModel):
    stockname: str
    

class AlgoResponse(BaseModel):
    status: str
    message: str
    data: Dict[str, Any] = {}

@router.get("/ping", response_model=Dict[str, str])
async def health_check():
    logger.info("Health check!")
    return {"status": "pong", "service": "fastapi-app"}

@router.post("/start-hk-algo", response_model=AlgoResponse)
async def process_stock(request: SalgoRequest):
    try:
        logger.info(f"Stockname: {request.stockname}")
        
        if not request.stockname or len(request.stockname.strip()) == 0:
            logger.warning("Stockname required!")
            raise HTTPException(
                status_code=400, 
                detail="Stockname required!"
            )
        
        result = await HKAlgo.start(request.stockname.strip())
        
        if result["status"] == "error":
            logger.error(f"Error: {result['message']}")
            raise HTTPException(
                status_code=500,
                detail=result["message"]
            )
        
        return AlgoResponse(
            status="success",
            message=result["message"],
            data=result.get("data", {})
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error: {request.stockname}: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Server error: {str(e)}"
        )

