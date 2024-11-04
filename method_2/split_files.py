import polars as pl
import sys

def split_file(input_file, num_splits, output_prefix):
    # Read the input file
    df = pl.read_csv(input_file, has_header=False, new_columns=["data"])
    
    # Calculate the number of rows per split
    num_rows = df.height
    rows_per_split = num_rows // num_splits
    remainder = num_rows % num_splits  # To handle extra rows
    
    start = 0
    for i in range(num_splits):
        # Calculate the end row for this split
        end = start + rows_per_split + (1 if i < remainder else 0)
        
        # Slice the dataframe for the current split
        split_df = df.slice(start, end - start)
        
        # Write the split dataframe to a new file
        output_file = f"{output_prefix}{i:02d}"
        split_df.write_csv(output_file, include_header=False)
        
        # Update the start row for the next split
        start = end

# Example usage
F=sys.argv[1]
NUM=int(sys.argv[2])
split_file(f"portfolio_input_{F}.txt", NUM, f"processing_input_{F}")
