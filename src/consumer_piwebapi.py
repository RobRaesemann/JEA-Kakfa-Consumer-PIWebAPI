import asyncio

from src.services.kafka_service import consume

async def consume_kafka():
    """

    """
    asyncio.Task(consume())