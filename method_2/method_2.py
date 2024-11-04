# Now, break up hugefile1.txt and hugefile2.txt into 10 files each, and run your process on all 10 sets in parallel. How do the run times compare to the original process?

# use what you have learned in this course to optimize this process. Be sure to record the amount of time it takes for each version of your program to complete this task.
# Optimize the program by using threads, so that you benefit from multiple cores in your CPU. Create a multithreaded program, where each thread works on the next chunk of the file.

from itertools import islice
from time import perf_counter
from multiprocessing import Pool
import time
from icecream import ic
import subprocess
ic.configureOutput(includeContext=True)
import glob

# Settings for input reader
BASE_INP_1='test_input_1.txt'
BASE_INP_2='test_input_2.txt'
# BASE_INP_1='portfolio_input_1.txt'
# BASE_INP_2='portfolio_input_2.txt'
INP1_DIR='inp_1_'
INP2_DIR='inp_2_'
TEMP_IN_PREFIX='processing_input_'
TEMP_OUT_PREFIX='processed_segment'
BATCH_SIZE=125_000

# Settings for processing
N_PROCS = 16

# Settings for output
OUT_F='portfolio_output.txt'

def read_file_in_chunks(file, chunk_size):
    ic(file)
    with open(file, "r") as f:
        for chunk in iter(lambda: list(islice(f, chunk_size)), []):
            yield chunk

def generate_chunks(segment, batch_size):
    inp_1 = f"{INP1_DIR}{N_PROCS}/{TEMP_IN_PREFIX}1{segment:02d}"
    inp_2 = f"{INP2_DIR}{N_PROCS}/{TEMP_IN_PREFIX}2{segment:02d}"
    inp_1_gen = read_file_in_chunks(inp_1, batch_size)
    inp_2_gen = read_file_in_chunks(inp_2, batch_size)

    for inp_1_chunk, inp_2_chunk in zip(inp_1_gen, inp_2_gen):
        yield (inp_1_chunk, inp_2_chunk)

def process_chunk(segment):
    ic(segment)
    segment_output = f"{TEMP_OUT_PREFIX}{segment:02d}"
    with open(segment_output, 'w') as out_f:
        for chunk in generate_chunks(segment, BATCH_SIZE):
            # Create the output data 
            # 1: Take a pair of numbers from the chunk
            # 2: Strip anywhite space as they come in string form
            # 3: Convert string to int
            # 4: Add numbers
            # 5: Convert back to string so it can be written to the .txt file
            chunk = [f"{int(chunk[0][i].strip()) + int(chunk[1][i].strip())}\n" for i in range(len(chunk[0]))]
            out_f.writelines(chunk)

if __name__ == "__main__":
    # Make sure to run the split_files.sh script and populate variables appropriately 
    # to have the correct number / location / prefixes in place to process with

    start = perf_counter()
    with Pool(processes=N_PROCS) as p:
        p.map(process_chunk, list(range(N_PROCS)))
        

    # Combine the outputs
    combine_files = glob.glob(f"{TEMP_OUT_PREFIX}*")
    for f in sorted(combine_files):
        subprocess.run(f"cat {f} >> {OUT_F}", shell=True, capture_output=False)
    
    # Cleanup the extra output files
    cleanup_files = glob.glob(f"{TEMP_OUT_PREFIX}*")
    for f in cleanup_files:
        subprocess.run(["rm", "-f", f])

    end = perf_counter()
    time_taken = round(end - start, 4)
    ic(time_taken)
    