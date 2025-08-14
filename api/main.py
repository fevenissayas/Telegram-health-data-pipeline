from fastapi import FastAPI, HTTPException, Query, Path
from typing import List
from api import crud, schemas
import logging

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Telegram Health Insights API",
    description="Analytical API to query insights from Ethiopian medical Telegram channels.",
    version="1.0.0"
)

@app.get("/", include_in_schema=False)
async def read_root():
    return {"message": "Welcome to the Telegram Health Insights API. Go to /docs for API documentation."}

@app.get(
    "/api/reports/top-products",
    response_model=schemas.APIResponse[List[schemas.TopProduct]],
    summary="Get top mentioned medical products/keywords",
    description="Returns the top N most frequently mentioned medical products or drugs based on keyword matching in message text."
)
async def get_top_products_report(
    limit: int = Query(10, gt=0, description="Number of top products to return.")
):
    try:
        data = crud.get_top_products(limit=limit)
        return schemas.APIResponse(
            status="success",
            message=f"Successfully retrieved top {limit} products.",
            data=[schemas.TopProduct(**item) for item in data]
        )
    except Exception as e:
        logger.exception("Error retrieving top products report.")
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {e}")

@app.get(
    "/api/channels/{channel_name}/activity",
    response_model=schemas.APIResponse[List[schemas.ChannelActivity]],
    summary="Get posting activity for a specific channel",
    description="Returns the daily message posting activity for a given Telegram channel."
)
async def get_channel_posting_activity(
    channel_name: str = Path(..., description="The exact username of the Telegram channel (e.g., 'CheMed123').")
):
    try:
        data = crud.get_channel_activity(channel_name=channel_name)
        if not data:
            raise HTTPException(status_code=404, detail=f"Channel '{channel_name}' not found or no activity.")
        return schemas.APIResponse(
            status="success",
            message=f"Successfully retrieved activity for channel '{channel_name}'.",
            data=[schemas.ChannelActivity(**item) for item in data]
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Error retrieving activity for channel '{channel_name}'.")
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {e}")

@app.get(
    "/api/search/messages",
    response_model=schemas.APIResponse[List[schemas.MessageSearchResult]],
    summary="Search for messages by keyword",
    description="Searches for Telegram messages containing a specific keyword in their text content."
)
async def search_telegram_messages(
    query: str = Query(..., min_length=3, description="The keyword or phrase to search for in messages.")
):
    try:
        data = crud.search_messages(query_str=query)
        if not data:
            return schemas.APIResponse(
                status="success",
                message=f"No messages found for query '{query}'.",
                data=[]
            )
        return schemas.APIResponse(
            status="success",
            message=f"Successfully found {len(data)} messages for query '{query}'.",
            data=[schemas.MessageSearchResult(**item) for item in data]
        )
    except Exception as e:
        logger.exception(f"Error searching messages for query '{query}'.")
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {e}")