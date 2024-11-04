import asyncio
import optuna
from itertools import islice
from time import perf_counter
from concurrent.futures import ThreadPoolExecutor
import sys
import optuna.visualization as vis

# Settings for input reader
INP_1='test_input_1.txt'
INP_2='test_input_2.txt'
# INP_1='portfolio_input_1.txt'
# INP_2='portfolio_input_2.txt'
INP_BATCH_SIZE=10_000

# For storing the max values of time and memory observed
max_time_taken = 1  # Initialize to 1 to avoid division by zero in the first trial
max_memory = 1
best_batch_memory = -1

custom_executor = ThreadPoolExecutor(max_workers=2)

def read_file_in_chunks(file, chunk_size):
    with open(file, "r") as f:
        for chunk in iter(lambda: list(islice(f, chunk_size)), []):
            #print(f"RFIC: Yielding chunk from {file}")
            # yield [int(item.strip()) for item in chunk]
            yield chunk

async def generate_chunks(batch_size):
    inp_1_gen = read_file_in_chunks(INP_1, batch_size)
    inp_2_gen = read_file_in_chunks(INP_2, batch_size)
    loop = asyncio.get_running_loop()
    
    while True:
        # Schedule both anext calls concurrently
        inp_1_chunk, inp_2_chunk = await asyncio.gather(
            asyncio.to_thread(lambda: next(inp_1_gen, None)),
            asyncio.to_thread(lambda: next(inp_2_gen, None))
        )
        
        # Stop if either generator is exhausted
        if inp_1_chunk is None or inp_2_chunk is None:
           break
        
        #print(f"GC: Yielding data chunk")
        yield (inp_1_chunk, inp_2_chunk)
        

def get_size(obj):
    """Recursively finds the total memory footprint of an object."""
    if isinstance(obj, (list, tuple, set)):
        return sys.getsizeof(obj) + sum(get_size(i) for i in obj)
    elif isinstance(obj, dict):
        return sys.getsizeof(obj) + sum(get_size(k) + get_size(v) for k, v in obj.items())
    else:
        return sys.getsizeof(obj)

async def measure_performance(batch_size):
    start_time = perf_counter()
    line_total = 0
    last_set = None
    async for lines in generate_chunks(batch_size):
        line_total += len(lines[0]) + len(lines[1])
        last_set = lines
    end_time = perf_counter()
    lines_memory = get_size(last_set)  # Measure memory size of lines
    del last_set
    time_taken = end_time - start_time
    # print(f"{batch_size}: {time_taken}s | {lines_memory / 1e6} MB")
    return time_taken, lines_memory

def objective(trial):
    global max_time_taken, max_memory
    batch_size = trial.suggest_int("INP_BATCH_SIZE", 1_000, 500_000, step=1_000)
    
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    # loop.set_default_executor(custom_executor)
    time_taken, peak_memory = loop.run_until_complete(measure_performance(batch_size))
    loop.close()

    return time_taken, (peak_memory/1e6)

async def main():
    loop = asyncio.get_running_loop()
    loop.set_default_executor(custom_executor)

    start1 = perf_counter()
    line_total = 0
    last_set = None
    async for lines in generate_chunks():
        line_total += len(lines[0]) + len(lines[1])
    end1 = perf_counter()
    print(f"Method 1: Read in {line_total} lines in {end1 - start1:.2f} seconds")
    print()
    print()
    print(f"Took {end1 - start1:0.4f}s to fully read input file chunks.")

if __name__ == "__main__":
    # asyncio.run(main())
    # Create a study and optimize the objective function
    study = optuna.create_study(directions=["minimize", "minimize"], study_name="batch_size testing")
    study.optimize(objective, n_trials=100, gc_after_trial=True, show_progress_bar=True)
    custom_executor.shutdown(wait=True)

    optuna.visualization.plot_pareto_front(study, target_names=["time_taken", "peak_memory (MB)"]).show()

    # best_params = study.best_trial.params
    # print("Best batch size:", best_params["INP_BATCH_SIZE"])

    # # Visualize optimization history
    # vis.plot_optimization_history(study).show()

    # # Visualize parameter relationships
    # vis.plot_parallel_coordinate(study).show()
