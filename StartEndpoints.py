from datetime import datetime
from os import name
from PreCalculationFunctions.AuthenticationFunctions import authorize_filemaker_data_api, logout_filemaker_data_api
from AppLogic import calculate_order_date, calculate_planning

from fastapi import FastAPI, HTTPException, Query, Body, BackgroundTasks
from pydantic import BaseModel
import uvicorn

app = FastAPI()
    
@app.post("/calculate_order_date")
async def post_calculate_order_date(background_tasks: BackgroundTasks):
    background_tasks.add_task(calculate_order_date)
    return {"message": "Order date calculation started in background."}

@app.get("/calculate_planning/{duration}")
async def get_calculate_planning(duration: int, background_tasks: BackgroundTasks):
    background_tasks.add_task(calculate_planning, duration)
    return {"message": "Order date calculation started in background."}

#Dev only does not work in container cuz docker start already
#uvicorn.run(app, host="0.0.0.0", port=8000)


