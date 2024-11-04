import polars

INP_1='portfolio_input_1.txt'
INP_2='portfolio_input_2.txt'
OUT_F='portfolio_output.txt'

inp_1 = polars.scan_csv(source=INP_1, has_header=False, low_memory=True, cache=False).collect()
inp_2 = polars.scan_csv(source=INP_2, has_header=False, low_memory=True, cache=False).collect()
out_f = polars.scan_csv(source=OUT_F, has_header=False, low_memory=True, cache=False).collect()

assert inp_1.shape == inp_2.shape and inp_2.shape == out_f.shape
print(f"Shapes all match: {out_f.shape}")

assert (inp_1.to_series() + inp_2.to_series() == out_f.to_series()).all()
print(f"Entries all match")