import asyncio
import aio_pika
import json
from random import choice
from datetime import datetime
from motor.motor_asyncio import AsyncIOMotorClient

RABBITMQ_DSN = 'amqp://user:password@host:5672'
QUEUE_NAME = "mongo_task"
MONGO_DSN = 'mongodb://user:password@host:27017'
MONGO_DB = 'task_test'
MONGO_COLLECTION = 'task'

count_processed_messages = 0
DELAY_IN_SECONDS = [1, 2, 3, 4, 5, 6, 7, 8]


async def update_mongo(task_id: str, document_id: str, field: str, value: str):
    clause = {
        'task_id': task_id,
        'documents.id': document_id
    }
    connection = AsyncIOMotorClient(MONGO_DSN)
    collection = connection[MONGO_DB][MONGO_COLLECTION]
    new_value = {
        f'documents.$.{field}': value
    }
    await collection.update_one(clause, {'$set': new_value})

    if field == 'finished':
        clause = {
            '$and': [
                {'task_id': task_id},
                {'documents.finished': {'$ne': None}}
            ]
        }
        new_value = {
            '$set': {'finished': value}
        }
        result = await collection.update_one(clause, new_value)
        return result.matched_count


async def process_message(message: aio_pika.IncomingMessage):
    """
    Обработка сообщений с автоматическим подтверждением получения
    :param message:
    :return:
    """
    async with message.process():
        global count_processed_messages

        delay = choice(DELAY_IN_SECONDS)
        count_processed_messages += 1
        message_content = message.body.decode('utf-8')
        data = json.loads(message_content)
        task_id = data['task_id']
        document_id = data['document_id']

        await update_mongo(task_id, document_id, 'started', datetime.now().isoformat())

        await asyncio.sleep(delay)

        result = await update_mongo(task_id, document_id, 'finished', datetime.now().isoformat())
        if result:
            print(result)

        print(f'Get result {count_processed_messages}: {data} with time {delay}')


async def main(main_loop, rabbitmq_conn: str, queue_name: str):
    connection: aio_pika.RobustConnection = await aio_pika.connect_robust(rabbitmq_conn, loop=main_loop)

    channel = await connection.channel()
    exchange = await channel.declare_exchange("direct", durable=True, auto_delete=False)
    queue = await channel.declare_queue(queue_name, auto_delete=False, durable=True)

    await queue.bind(exchange, queue_name)
    await queue.consume(process_message)

    print('[x] Start consumer for ', queue_name)
    return connection


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    rabbit_connection = loop.run_until_complete(main(loop, RABBITMQ_DSN, QUEUE_NAME))

    try:
        loop.run_forever()
    finally:
        loop.run_until_complete(rabbit_connection.close())
