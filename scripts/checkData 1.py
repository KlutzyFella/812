import argparse
import pandas as pd

#============================================================
#COMMAND LINE ARGUMENT
#Check any CSV file without editing this script
#Example:
#python checkData.py --file "C:\...\region2_20000_simulator_input.csv"
# ============================================================

parser = argparse.ArgumentParser()
parser.add_argument("--file", required=True, help="Path to CSV file to check")
args = parser.parse_args()

#============================================================
#LOAD DATASET
#============================================================

print("\nLoading file:")
print(args.file)

df = pd.read_csv(args.file)

#============================================================
#BASIC DATASET CHECKS
#============================================================

#Show first few rows to visually confirm the data loaded correctly
print("\nFirst rows:")
print(df.head())

#Show all column names
print("\nColumns:")
print(df.columns)

#Show number of rows and columns
print("\nShape:")
print(df.shape)

#============================================================
#COLUMN CHECK
#These are the main columns needed by the simulator
# ============================================================

required_cols = [
    "event_time",
    "func_id",
    "trigger_type",
    "exec_time",
    "cold_start_flag"
]

print("\nRequired column check:")
for col in required_cols:
    if col in df.columns:
        print(f"{col}: OK")
    else:
        print(f"{col}: MISSING")

#============================================================
#VALUE CHECKS
#Helps confirm the dataset makes sense
#============================================================

#Count each trigger type
if "trigger_type" in df.columns:
    print("\nTrigger type counts:")
    print(df["trigger_type"].value_counts(dropna=False))

#Count cold start labels
if "cold_start_flag" in df.columns:
    print("\nCold start counts:")
    print(df["cold_start_flag"].value_counts(dropna=False))

#============================================================
#MISSING VALUE CHECK
#Shows if any important columns have missing values
#============================================================

print("\nMissing values per column:")
print(df.isna().sum())

#============================================================
#TIME ORDER CHECK
#Simulator needs events in chronological order
#============================================================

if "event_time" in df.columns:
    print("\nEvent time sorted?")
    print(df["event_time"].is_monotonic_increasing)

#============================================================
#DONE
#============================================================

print("\nCheck complete.")