from fastapi import FastAPI, HTTPException
import httpx
import os
from prometheus_fastapi_instrumentator import Instrumentator
from pydantic import BaseModel

app = FastAPI()
Instrumentator().instrument(app).expose(app)

ORDER_SERVICE_URL = os.getenv("ORDER_SERVICE_URL", "http://order-service:5000")

class OrderCreate(BaseModel):
    item: str
    price: float

@app.post("/orders")
async def create_order(order: OrderCreate):
    async with httpx.AsyncClient() as client:
        resp = await client.post(
            f"{ORDER_SERVICE_URL}/order",
            json=order.dict()
        )
    if resp.status_code != 200:
        raise HTTPException(status_code=resp.status_code, detail=resp.text)
    return resp.json()

@app.get("/orders/{order_id}")
async def get_order(order_id: int):
    async with httpx.AsyncClient() as client:
        resp = await client.get(f"{ORDER_SERVICE_URL}/order/{order_id}")
    return resp.json()

@app.get("/health")
async def health():
    return {"status": "ok"}