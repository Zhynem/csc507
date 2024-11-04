# use what you have learned in this course to optimize this process. Be sure to record the amount of time it takes for each version of your program to complete this task.
# Optimize the program by using threads, so that you benefit from multiple cores in your CPU. Create a multithreaded program, where each thread works on the next chunk of the file.

from itertools import islice
from time import perf_counter
from icecream import ic
ic.configureOutput(includeContext=True)

# Settings for input reader
INP_1='portfolio_input_1.txt'
INP_2='portfolio_input_2.txt'
BATCH_SIZE=125_000

# Settings for output
OUT_F='portfolio_output_1.txt'

def read_file_in_chunks(file, chunk_size):
    with open(file, "r") as f:
        for chunk in iter(lambda: list(islice(f, chunk_size)), []):
            # ic(file)
            yield chunk

def generate_chunks(batch_size):
    inp_1_gen = read_file_in_chunks(INP_1, batch_size)
    inp_2_gen = read_file_in_chunks(INP_2, batch_size)

    for inp_1_chunk, inp_2_chunk in zip(inp_1_gen, inp_2_gen):
        yield (inp_1_chunk, inp_2_chunk)

def process_chunk(inp_data):
    # First step is converting all the data from strings to integers
    inp_data = [ 
                 [int(item.strip()) for item in inp_data[0]], 
                 [int(item.strip()) for item in inp_data[1]]
               ]
    
    # Then create the output data, keep track of the segment ID
    out_data = [f"{inp_data[0][i] + inp_data[1][i]}\n" for i in range(len(inp_data[0]))]

    # Put the data on the queue to be written out
    return out_data
    

if __name__ == "__main__":
    with open(OUT_F, 'w') as out_f:
        start = perf_counter()
        processed_chunks = 0
        for chunk in generate_chunks(BATCH_SIZE):
            out_f.writelines(process_chunk(chunk))
            processed_chunks += 1
            if processed_chunks % 100 == 0:
                ic(processed_chunks)
        end = perf_counter()
    time_taken = round(end - start, 4)
    ic(time_taken)

