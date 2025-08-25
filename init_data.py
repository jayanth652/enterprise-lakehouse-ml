import subprocess, sys, pathlib

DATA = pathlib.Path("/data")
RAW = DATA / "raw_local"
RAW.mkdir(parents=True, exist_ok=True)

print("==> Generating synthetic data ...")
subprocess.check_call([sys.executable, str(DATA / "make_synthetic_data.py")])
print("Synthetic data created under /data/raw_local/*.csv")