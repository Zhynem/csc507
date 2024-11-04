# My other idea was to keep the same cpu_count worker processes going, but then
# instead of waiting on them all to finish there should be a way to write numbers
# to the file as they are generated in a stream like fashion
# ChatGPT 4o suggested the use of a writer function, queue, and sentinal values
SENTINEL=None

# It worked but turned out to be slow so then I tried batching the operations to reduce
#   overhead on both the queue synchronization between puts and gets, as well as speed up
#   the file writes with writelines (fewer system calls, better buffer usage)

# Batch size of 800 gets performance to roughly the same level as numbers-mp.py (0.19 seconds total)
# Batch size of 25000 makes it run about 0.02 seconds faster than numbers-mp.py (0.17 seconds vs 0.19 seconds)
BATCH_SIZE=25000

# Might as well set this as an extra global variable since I have a couple others now,
# this will be my config section
NUM_LINES=500_000_000

# Trying to keep things slim so separating imports to their respective used locations
def generate_numbers(queue, size):
    # This is the function the worker processes will run
    # Import the only function being used from random
    from random import randint

    # Put each item onto a queue for it to be consumed later.
    # The queue.put function is thread/process safe and will block until
    # it is allowed to put its item into the queue
    # https://docs.python.org/3/library/multiprocessing.html#multiprocessing.Queue
    #
    # It was much slower this way, presumably from all the queue sync stuff happening
    # Trying to batch things a bit more to reduce that overhead?
    num_to_gen = size
    while True:
        if num_to_gen > BATCH_SIZE:
            queue.put_nowait([f"{randint(0, 32767)}\n" for _ in range(BATCH_SIZE)])
            num_to_gen -= BATCH_SIZE
        else:
            queue.put_nowait([f"{randint(0, 32767)}\n" for _ in range(num_to_gen)])
            break
    
    # After all the numbers from this process have bene generated put the sentinel
    # value indicating the process is complete
    queue.put(SENTINEL)

# The writer process will just pull work off the queue and write it to the file as fast as 
# it can, when it's recieved the sentinel value from all workers it will know to quit
def write_numbers(queue, workers):
    closed_workers = 0
    lines_written = 0
    with open("portfolio_input_4.txt", "w") as f:
        while closed_workers < workers:
            if queue.empty():
                continue

            # By default queue.get blocks with infinite timeout, ie. it'll wait forever for work
            next_item = queue.get_nowait()
            
            # If it's the sentinel value we know a worker has finished, so count it and 
            # loop again
            if next_item is SENTINEL:
                closed_workers += 1
                continue
            
            # Otherwise write the item to the file, items are now a batch of strings
            # instead of singular strings
            f.writelines(next_item)
            lines_written += len(next_item)
    # At this point the queue should be completely empty, might as well use it to 
    # return the number of lines written by this process
    queue.put(lines_written)

if __name__ == "__main__":
    print("Main coordinator starting")

    # Imports that only the coordinator needs
    from os import cpu_count
    from multiprocessing import Process, Queue
    from time import perf_counter

    # Print the current date / time in the same format as the linux `date` command
    # including the timezone which requires a bit of extra work
    from datetime import datetime, tzinfo
    from pytz import UTC
    now = datetime.utcnow().replace(tzinfo=UTC).astimezone()
    print(now.strftime('%a %b %d %H:%M:%S %Z %Y'))

    # Get then number of CPU cores on the machine
    cores = cpu_count() - 1

    # Divide the work evenly (or as even as possible, 
    #   one proccess may have a bit more depending on the math)
    sizes = [NUM_LINES // cores for _ in range(cores)]
    remaining = NUM_LINES - ((NUM_LINES // cores) * cores)
    sizes[-1] += remaining

    print("Spawning multiple processes to generate lists of the following sizes:")
    print(f"{sizes} = {sum(sizes):,}")
    print()

    number_queue = Queue()

    # Start the perf_counter
    start = perf_counter()
    # Launch the generation workers
    for i in range(cores):
        Process(target=generate_numbers, args=(number_queue, sizes[i])).start()

    # Start the writer thread
    writer = Process(target=write_numbers, args=(number_queue, cores))
    writer.start()
    
    # Wait for the writer to finish, which also indicates all the worker_procs finished
    writer.join()
    
    # End the perf counter
    end = perf_counter()

    # Get the lines off the queue
    lines_written = number_queue.get()
    print(f"Took {end-start:.2f} seconds")
    print(f"{lines_written:,} lines written")

