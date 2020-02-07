import asyncio
import time


async def new_hello(wait, greeting):
    await asyncio.sleep(wait)
    print(greeting)


async def main():
    print('Hello ...')
    t1 = asyncio.create_task(new_hello(1, "1s"))
    t2 = asyncio.create_task(new_hello(2, "2s"))
    t3 = asyncio.create_task(new_hello(3, "3s"))
    t4 = asyncio.create_task(new_hello(1, "1s"))
    t5 = asyncio.create_task(new_hello(1, "1s"))
    t6 = asyncio.create_task(new_hello(0.5, "0.5s"))
    print('... World!')
    # The next two lines waits for 10 seconds or until all Tasks have finished
    # The Tasks are being executed in parallel
    await asyncio.sleep(10)
    # await asyncio.gather(t1, t2, t3, t4, t5, t6)


# Python 3.7+
asyncio.run(main())
