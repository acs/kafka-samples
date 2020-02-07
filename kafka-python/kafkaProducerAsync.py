import logging
import time

from aiokafka import AIOKafkaProducer
import asyncio

loop = asyncio.get_event_loop()


async def send_one():
    producer = AIOKafkaProducer(
        loop=loop, bootstrap_servers='localhost:9092')
    await producer.start()

    try:
        while True:
            logging.debug("Sending message ...")
            time.sleep(1)
            # Get cluster layout and initial topic/partition leadership information
                # Produce message
            await producer.send_and_wait("my_topic", b"Super message")
    finally:
        # Wait for all pending messages to be delivered or expire.
        await producer.stop()

if __name__ == '__main__':

    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(message)s')

    loop.run_until_complete(send_one())
