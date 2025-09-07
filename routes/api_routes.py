from celery import chord
from controllers.hk_energy.hk_energy_init_task import hk_energy_initialise
from fastapi import APIRouter, HTTPException
from typing import Dict

from controllers.get_stocks_codes import get_stocks_codes
from controllers.hk_energy.hk_energy import hk_energy_controller
from controllers.hk_ta.cancel_task import hk_ta_cancel_task
from controllers.hk_ta.hk_ta_init_task import hk_ta_initialise
from controllers.files_controller import download_csv_files


from models.schemas import (
    AlgoRequest, AlgoResponse,  EnergyAlgoResponse, 
    EnergyAlgoRequestTest,  CodeesResponse, HKEnergyResponse, HKTaCancelResponse, TaskQueueResponse, HKEnergyRequest,
    HKTaCheckResponse
)

from services.hk_ta import HK_TA
from services.queue_service import process_hk_ta_task, clear_hk_ta_token
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



# HK Energy
@router.post("/hk-energy", response_model=HKEnergyResponse)
async def hk_energy(request: HKEnergyRequest):
    try:
        logger.info(f"Starting HK Energy !!!")

        if not request.trade_day or len(request.trade_day.strip()) == 0:
            logger.warning("Missing or invalid required fields!")
            raise HTTPException(
                status_code=400, 
                detail="Missing or invalid required fields!"
            )

        result = await hk_energy_initialise(request.trade_day.strip())
        
        return HKEnergyResponse(
            task_id=result["task_id"],
            status="QUEUED",
            message=result["message"]
        )   
        
    except Exception as e:
        logger.error(f"Error starting HK Energy check: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Error starting HK Energy check: {str(e)}"
        )

# HK TA
@router.get("/hk-ta", response_model=HKTaCheckResponse)
async def check_hk_ta():
    """
    Start HK TA database check with retry mechanism
    Checks database every 1 minute until data is found or max attempts reached
    """
    try:
        logger.info(f"Starting HK TA !!!")
        
        result = await hk_ta_initialise()
        
        return HKTaCheckResponse(
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

@router.get("/hk-ta/queue", response_model=TaskQueueResponse)
async def queue_hk_ta():
    """Queue HK TA analysis task"""
    try:
        response_data = await get_stocks_codes()

        # tasks = [process_hk_ta_task.s(code, response_data["date"]) for code in response_data["codes"][:10]]
        tasks = [process_hk_ta_task.s(code, "2025-09-03") for code in response_data["codes"]]  #! REMOVE

        # chord(tasks)(clear_hk_ta_token.s(response_data["date"]))
        chord(tasks)(clear_hk_ta_token.s("2025-09-03")) #! REMOVE

        return TaskQueueResponse(
            message=f"HK TA analysis started for {response_data['date']}. Total codes: {len(response_data['codes'])}."
        )
        
    except Exception as e:
        logger.error(f"Error queueing HK TA task: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Error queueing task: {str(e)}"
        )

@router.delete("/hk-ta/queue/cancel/{task_id}", response_model=HKTaCancelResponse)
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


@router.get("/files")
async def get_files():
    """
    Download CSV files as ZIP archive from the data directory
    """
    return await download_csv_files()