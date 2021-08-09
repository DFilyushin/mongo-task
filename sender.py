import asyncio
import datetime
import json
from typing import List
from uuid import uuid4
from motor.motor_asyncio import AsyncIOMotorClient
import aio_pika


MONGO_DSN = 'mongodb://user:password@host:27017'
MONGO_DB = 'task_test'
MONGO_COLLECTION = 'task'
RABBITMQ_DSN = 'amqp://user:password@host:5672'

TASK_COUNT = 3
DOCUMENTS_IN_TASK = 15


async def create_task(count_documents: int) -> dict:
    connection: AsyncIOMotorClient = AsyncIOMotorClient(MONGO_DSN)
    collection = connection[MONGO_DB][MONGO_COLLECTION]

    documents = []
    for _ in range(count_documents):
        documents.append(
            {
                'id': uuid4().hex,
                'status': 'NEW',
                'started': None,
                'finished': None
            }
        )
    task_id = uuid4().hex
    task = {
        'task_id': task_id,
        'documents': documents,
        'started': datetime.datetime.now().isoformat(),
        'finished': None
    }
    await collection.insert_one(task)
    return task


async def produce_messages(tasks: List[dict]):
    connection = await aio_pika.connect_robust(RABBITMQ_DSN)
    async with connection:
        channel = await connection.channel()
        exchange = await channel.declare_exchange("direct", auto_delete=False, durable=True)
        new_queue = await channel.declare_queue(name='mongo_task', durable=True)
        await new_queue.bind(exchange=exchange, routing_key='mongo_task')
        for task in tasks:
            task_id = task['task_id']
            for document in task['documents']:
                message = {
                    'task_id': task_id,
                    'document_id': document['id']
                }
                await exchange.publish(
                    aio_pika.Message(
                        body=json.dumps(message).encode(),
                    ),
                    routing_key='mongo_task'
                )
            print(task_id)

async def main():
    tasks = []
    for i in range(TASK_COUNT):
        task = await create_task(DOCUMENTS_IN_TASK)
        tasks.append(task)
    await produce_messages(tasks)


asyncio.run(main())
