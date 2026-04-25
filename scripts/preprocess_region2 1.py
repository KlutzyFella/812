import argparse
import os
import pandas as pd

#============================================================
#FILE PATHS (UPDATE IF NEEDED)
#============================================================

#Raw request trace (large file → we sample from this)
REQUESTS_PATH = r"C:\Users\hanon\Documents\CSE 812\data\R2_00000_00019\R2_00000_00019\part-00000-1e3cf064-4a06-4ff0-ae8f-a836612b1b04-c000.csv"

#Function metadata (contains trigger type + runtime info)
METADATA_PATH = r"C:\Users\hanon\Documents\CSE 812\data\df_funcID_runtime_triggerType.csv"

#Cold start dataset (must match same day as requests)
COLDSTART_PATH = r"C:\Users\hanon\Documents\CSE 812\data\day_30.csv"

#Where all processed outputs will be saved
OUTPUT_DIR = r"C:\Users\hanon\Documents\OutputData"


#============================================================
#COMMAND LINE ARGUMENTS
#Allows us to run different dataset sizes (20k, 50k, 100k)
#============================================================

parser = argparse.ArgumentParser()
parser.add_argument("--rows", type=int, default=20000)
args = parser.parse_args()

N_ROWS = args.rows

# Output file names depend on dataset size
FULL_OUTPUT_PATH = os.path.join(
    OUTPUT_DIR, f"region2_{N_ROWS}_preprocessed.csv"
)

SIMPLE_OUTPUT_PATH = os.path.join(
    OUTPUT_DIR, f"region2_{N_ROWS}_simulator_input.csv"
)


#============================================================
#HELPER FUNCTION
#Extract pool name from pod_id (used to build unique func_id)
#============================================================

def extract_pool_name(pod_id):
    if pd.isna(pod_id):
        return None

    parts = str(pod_id).split("-")

    #Keep first 3 parts (represents function pool)
    if len(parts) >= 4:
        return "-".join(parts[:3])

    return None


#============================================================
#LOAD DATA
#============================================================

print(f"Loading request sample: {N_ROWS} rows...")
requests_df = pd.read_csv(REQUESTS_PATH, nrows=N_ROWS)

print("Loading metadata...")
metadata_df = pd.read_csv(METADATA_PATH)

print("Loading cold starts...")
cold_df = pd.read_csv(COLDSTART_PATH)


#============================================================
#DEBUG: CHECK RAW DATA STRUCTURE
#Helps verify column names before processing
#============================================================

print("\nRequest columns:")
print(requests_df.columns.tolist())

print("\nMetadata columns:")
print(metadata_df.columns.tolist())


#============================================================
#RENAME COLUMNS FOR CONSISTENCY
#Makes downstream processing easier and cleaner
#============================================================

requests_df = requests_df.rename(columns={
    "time_worker": "event_time",
    "requestID": "request_id",
    "clusterName": "cluster_name",
    "funcName": "func_name",
    "podID": "pod_id",
    "userID": "user_id",
    "totalCost_worker": "exec_time"
})

metadata_df = metadata_df.rename(columns={
    "funcID": "func_id",
    "triggerType-invocationType": "trigger_invocation"
})

cold_df = cold_df.rename(columns={
    "requestID": "request_id"
})


#============================================================
#FEATURE ENGINEERING
#============================================================

#Extract pool name from pod_id
requests_df["pool_name"] = requests_df["pod_id"].apply(extract_pool_name)

#Create unique function identifier
#Combines function name + user + pool
requests_df["func_id"] = (
    requests_df["func_name"].astype(str)
    + "---"
    + requests_df["user_id"].astype(str)
    + "---"
    + requests_df["pool_name"].astype(str)
)

#Split metadata column into:
#trigger_type
#invocation_type
split_cols = metadata_df["trigger_invocation"].astype(str).str.rsplit("-", n=1, expand=True)
metadata_df["trigger_type"] = split_cols[0]
metadata_df["invocation_type"] = split_cols[1]


#============================================================
#MERGE REQUEST DATA WITH METADATA
#Adds trigger + runtime info to each request
#============================================================

merged_df = requests_df.merge(
    metadata_df[["func_id", "cpu_request", "runtime", "trigger_type", "invocation_type"]],
    on="func_id",
    how="left"
)


#============================================================
#ADD COLD START LABELS
#===========================================================

#Convert IDs to strings for safe matching
request_ids = set(requests_df["request_id"].astype(str).str.strip())
cold_ids = set(cold_df["request_id"].astype(str).str.strip())

#Check overlap (sanity check)
matches = request_ids.intersection(cold_ids)
print("\nNumber of overlapping request IDs:", len(matches))

#Mark cold starts
merged_df["cold_start_flag"] = (
    merged_df["request_id"].astype(str).str.strip().isin(cold_ids)
)

#Check merge quality
print("\nMatched trigger_type rows:", merged_df["trigger_type"].notna().sum())
print("Missing trigger_type rows:", merged_df["trigger_type"].isna().sum())


#============================================================
#FINAL CLEAN DATASET
#============================================================

clean_df = merged_df[[
    "event_time",
    "request_id",
    "cluster_name",
    "func_name",
    "user_id",
    "pod_id",
    "pool_name",
    "func_id",
    "trigger_type",
    "invocation_type",
    "runtime",
    "cpu_request",
    "exec_time",
    "cold_start_flag"
]].copy()

#Convert numeric fields
clean_df["event_time"] = pd.to_numeric(clean_df["event_time"], errors="coerce")
clean_df["exec_time"] = pd.to_numeric(clean_df["exec_time"], errors="coerce")
clean_df["cpu_request"] = pd.to_numeric(clean_df["cpu_request"], errors="coerce")

#Remove invalid rows
clean_df = clean_df.dropna(subset=["event_time", "request_id", "func_id"])

#Sort by time
clean_df = clean_df.sort_values("event_time").reset_index(drop=True)

#Convert True/False
clean_df["cold_start_flag"] = clean_df["cold_start_flag"].astype(int)


#============================================================
#SANITY CHECKS
#============================================================

print("\nFinal dataset shape:", clean_df.shape)

print("\nCold start distribution:")
print(clean_df["cold_start_flag"].value_counts())

print("\nTrigger type distribution:")
print(clean_df["trigger_type"].value_counts())


#============================================================
#SAVE FULL DATASET
#============================================================

os.makedirs(OUTPUT_DIR, exist_ok=True)

clean_df.to_csv(FULL_OUTPUT_PATH, index=False)

print("\nSaved full cleaned dataset:")
print(FULL_OUTPUT_PATH)


#============================================================
#SAVE SIMPLIFIED DATASET
#Only essential columns
#============================================================

simple_df = clean_df[[
    "event_time",
    "func_id",
    "trigger_type",
    "exec_time",
    "cold_start_flag"
]].copy()

simple_df.to_csv(SIMPLE_OUTPUT_PATH, index=False)

print("\nSaved simulator input dataset:")
print(SIMPLE_OUTPUT_PATH)


print("\nDone.")