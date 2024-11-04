# use what you have learned in this course to optimize this process. Be sure to record the amount of time it takes for each version of your program to complete this task.
# Optimize the program by using threads, so that you benefit from multiple cores in your CPU. Create a multithreaded program, where each thread works on the next chunk of the file.

import asyncio
from itertools import islice
from time import perf_counter
from concurrent.futures import ThreadPoolExecutor
from multiprocessing import Pool, Manager
import time
from icecream import ic
ic.configureOutput(includeContext=True)

# Settings for input reader
INP_1='portfolio_input_1.txt'
INP_2='portfolio_input_2.txt'
BATCH_SIZE=125_000

# Settings for processing
N_PROCS = 16
MAX_Q_SIZE = N_PROCS*3
PROC_HAPPENING = False
custom_executor = ThreadPoolExecutor(max_workers=16)

# Settings for output
OUT_F='portfolio_output_1.txt'

def read_file_in_chunks(file, chunk_size):
    with open(file, "r") as f:
        for chunk in iter(lambda: list(islice(f, chunk_size)), []):
            # ic(file)
            yield chunk

async def generate_chunks(batch_size):
    inp_1_gen = read_file_in_chunks(INP_1, batch_size)
    inp_2_gen = read_file_in_chunks(INP_2, batch_size)
    loop = asyncio.get_running_loop()
    
    count = -1
    while True:
        # Schedule both anext calls concurrently
        i1thr = asyncio.to_thread(lambda: next(inp_1_gen, None))
        i2thr = asyncio.to_thread(lambda: next(inp_2_gen, None))
        i1t = loop.create_task(i1thr)
        i2t = loop.create_task(i2thr)

        inp_1_chunk, inp_2_chunk = await asyncio.gather(
            i1t, i2t
        )
        count += 1
        
        # Stop if either generator is exhausted
        if inp_1_chunk is None or inp_2_chunk is None:
           break
        
        # ic(count)
        yield (count, inp_1_chunk, inp_2_chunk)

async def write_output(queue, out_file):
    global PROC_HAPPENING
    # Wait a moment for data to start coming in to the queue
    await asyncio.sleep(1)
    # ic()
    written_chunks = 0
    while PROC_HAPPENING or not queue.empty():
        item = queue.get()
        out_file.writelines(item[1])
        # Ensure that the chunks being written to are in order matching the ID sent with each chunk
        assert written_chunks == item[0]
        written_chunks += 1
        if written_chunks % 100 == 0:
            ic(written_chunks)
        if not PROC_HAPPENING and queue.empty():
            ic("Processing and writing complete. Shutting down")
            break
        await asyncio.sleep(0.001)
        

def process_chunk(queue, inp_data):
    # First step is converting all the data from strings to integers
    inp_data = [ inp_data[0],
                 [int(item.strip()) for item in inp_data[1]], 
                 [int(item.strip()) for item in inp_data[2]]
               ]
    
    # Then create the output data, keep track of the segment ID
    out_data = [inp_data[0], [f"{inp_data[1][i] + inp_data[2][i]}\n" for i in range(len(inp_data[1]))]]

    # Put the data on the queue to be written out
    queue.put(out_data)
    # ic(out_data[0])


async def main():
    global PROC_HAPPENING
    with Manager() as manager:
        # Set up the processing pool and IPC queue
        pool = Pool(processes=N_PROCS)
        data_queue = manager.Queue(maxsize=MAX_Q_SIZE)

        # Set up the async loop and thread executors
        loop = asyncio.get_running_loop()
        loop.set_default_executor(custom_executor)

        # Start the writing thread
        start = perf_counter()
        ic("Starting writer")
        out_file = open(OUT_F, 'w')
        out_task = loop.create_task(write_output(data_queue, out_file))

        # Start the reading threads, for each chunk that gets returned submit it to the pool to work on
        ic("Starting reader and processing tasks")
        PROC_HAPPENING = True
        submitted_chunks = 0
        async for data_chunk in generate_chunks(BATCH_SIZE):
            # Try to keep the overall queue size down to save on memory
            while data_queue.full():
                ic("Queue full, waiting")
                await asyncio.sleep(0.25)
            pool.apply_async(process_chunk, (data_queue, data_chunk,))
            submitted_chunks += 1
            if submitted_chunks % 100 == 0:
                ic(submitted_chunks)
        
        # Once all the work is submitted wait for it to complete
        pool.close()
        pool.join()
        ic("Processing complete")
        PROC_HAPPENING = False

        # Then close down the writer thread
        await out_task
        ic("Writing complete")
        out_file.close()
        end = perf_counter()
        time_taken = round(end - start,4)
        ic(time_taken)
    

if __name__ == "__main__":
    asyncio.run(main())
    custom_executor.shutdown(wait=True)
