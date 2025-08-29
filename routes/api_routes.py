from controllers.get_stocks_codes import get_stocks_codes
from controllers.hk_energy import hk_energy_controller
from controllers.hk_ta.cancel_task import hk_ta_cancel_task
from controllers.hk_ta.init_task import hk_ta_initialise
from fastapi import APIRouter, HTTPException
from models.schemas import (
    AlgoRequest, AlgoResponse,  EnergyAlgoResponse, 
    EnergyAlgoRequestTest,  CodeesResponse, HKTaCancelResponse,
    TaskRequest, TaskResponse,
    CheckHkTaResponse
)

from typing import Dict
from services.hk_ta import HK_TA
from services.queue_service import cancel_task, queue_check_hk_ta, process_hk_ta_task
from config.logger import setup_logger


route_logger = setup_logger("api_routes")
logger = route_logger


router = APIRouter()



@router.get("/ping", response_model=Dict[str, str])
async def health_check():
    logger.info("Health check!")
    return {"status": "pong", "service": "fastapi-app"}

@router.post("/start-hk-ta", response_model=AlgoResponse)
async def process_stock(request: AlgoRequest):
    try:    
        if not request.stock_code or len(request.stock_code.strip()) == 0 or not request.trade_day or len(request.trade_day.strip()) == 0:
            logger.warning("stockname and tradeDay required!")
            raise HTTPException(
                status_code=400, 
                detail="Stockname and tradeDay required!"
            )
        logger.info(f"Stockname: {request.stock_code}, TradeDay: {request.trade_day}")

        result = await HK_TA.start(request.stock_code.strip(), request.trade_day.strip())
        
        if result["status"] == "error":
            logger.error(f"Error: {result['message']}")
            raise HTTPException(
                status_code=500,
                detail=result["message"]
            )
        
        return AlgoResponse(
            status="success",
            stockname=request.stock_code,
            tradeDay=request.trade_day,
            message=result["message"],
            data_from_sergio_ta=result.get("data_from_sergio_ta", []),
            # data_from_api_ta=result.get("data_from_api_ta", [])
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error: {request.stockname}: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Server error: {str(e)}"
        )


@router.post("/start-hk-energy", response_model=EnergyAlgoResponse)
async def energy_hk(request: EnergyAlgoRequestTest):
    try:        
        if not request.stock_code or len(request.stock_code.strip()) == 0 or not request.trade_day or len(request.trade_day.strip()) == 0:
            logger.warning("Missing or invalid required fields!")
            raise HTTPException(
                status_code=400, 
                detail="Missing or invalid required fields!"
            )

        result = await hk_energy_controller(request.stock_code.strip(), request.trade_day.strip())
        
        if result["status"] == "error":
            logger.error(f"Error: {result['message']}")
            raise HTTPException(
                status_code=500,
                detail=result["message"]
            )
    
        return EnergyAlgoResponse(
            status=result["status"] ,
            message=result["message"] ,
            indicators=result["indicators"]
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error: {request.stock_code}: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Server error: {str(e)}"
        )

@router.get("/codes", response_model=CodeesResponse)
async def get_codes():
    codes = await get_stocks_codes()
    if codes:
        return {"date": codes["date"], "codes": codes["codes"]}
    else:
        return {"date": "", "codes": []}


# Queue endpoints
@router.post("/queue/hk-energy", response_model=TaskResponse)
async def queue_hk_energy(request: TaskRequest):
    """Queue HK Energy analysis task"""
    try:
        if not request.stock_code or len(request.stock_code.strip()) == 0 or not request.trade_day or len(request.trade_day.strip()) == 0:
            logger.warning("Missing or invalid required fields!")
            raise HTTPException(
                status_code=400, 
                detail="Missing or invalid required fields!"
            )

        result = queue_hk_energy_analysis(request.stock_code.strip(), request.trade_day.strip())
        
        return TaskResponse(
            task_id=result["task_id"],
            status=result["status"],
            stock_code=result["stock_code"],
            trade_day=result["trade_day"],
            message=f"HK Energy analysis queued for {request.stock_code}"
        )
        
    except Exception as e:
        logger.error(f"Error queueing HK Energy task: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Error queueing task: {str(e)}"
        )




# HK TA
@router.get("/hk-ta", response_model=CheckHkTaResponse)
async def check_hk_ta():
    """
    Start HK TA database check with retry mechanism
    Checks database every 1 minute until data is found or max attempts reached
    """
    try:
        logger.info(f"Starting HK TA !!!")
        
        result = await hk_ta_initialise()
        
        return CheckHkTaResponse(
            task_id=result["task_id"],
            status="QUEUED",
            message=result["message"]
        )   
        
    except Exception as e:
        logger.error(f"Error starting HK TA check: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Error starting HK TA check: {str(e)}"
        )

@router.delete("/queue/hk-ta/cancel/{task_id}", response_model=HKTaCancelResponse)
async def cancel_task_endpoint(task_id: str):
    """Cancel a queued task"""
    try:
        result = await hk_ta_cancel_task(task_id)
        return HKTaCancelResponse(**result)
    except Exception as e:
        logger.error(f"Error cancelling task: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Error cancelling task: {str(e)}"
        )

@router.get("/queue/hk-ta", response_model=TaskResponse)
async def queue_hk_ta():
    """Queue HK TA analysis task"""
    try:
        response_data = await get_stocks_codes()
        for code in response_data["codes"][:10]:
            # process_hk_ta_task.delay(code, response_data["date"])
            process_hk_ta_task.delay(code, '2025-08-29')

        return TaskResponse(
            message=f"HK TA analysis started for {response_data['date']}. Total codes: {len(response_data['codes'])}."
        )
        
    except Exception as e:
        logger.error(f"Error queueing HK TA task: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Error queueing task: {str(e)}"
        )

