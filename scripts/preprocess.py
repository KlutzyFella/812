import pandas as pd

# =========================
# FILE PATHS (UPDATE IF NEEDED)
# =========================
# Request trace file (large dataset, we sample from it)
REQUESTS_PATH = "/home/ronnit/projects/812/data/R2_00000_00019/part-00000-1e3cf064-4a06-4ff0-ae8f-a836612b1b04-c000.csv"

# Metadata file (contains runtime + trigger info per function)
METADATA_PATH = "/home/ronnit/projects/812/data/df_funcID_runtime_triggerType.csv"

# Output cleaned dataset
OUTPUT_PATH = "/home/ronnit/projects/812/data/region2_sample_preprocessed.csv"

# Cold-start dataset (must match same day as requests → day 30)
COLDSTART_PATH = "/home/ronnit/projects/812/data/day_30.csv"

# Number of rows to sample (increase later for realism)
N_ROWS = 20000


# =========================
# HELPER FUNCTION
# =========================
# Extract pool name from pod_id (used to help build func_id)
def extract_pool_name(pod_id):
    if pd.isna(pod_id):
        return None

    parts = str(pod_id).split("-")

    # Keep first 3 parts of pod_id as pool identifier
    if len(parts) >= 4:
        return "-".join(parts[:3])

    return None


# =========================
# LOAD DATA
# =========================
print("Loading request sample...")
requests_df = pd.read_csv(REQUESTS_PATH, nrows=N_ROWS)

print("Loading metadata...")
metadata_df = pd.read_csv(METADATA_PATH)

print("Loading cold starts...")
cold_df = pd.read_csv(COLDSTART_PATH)

# Rename cold-start request ID column to match requests
cold_df = cold_df.rename(columns={
    "requestID": "request_id"
})


# =========================
# QUICK COLUMN CHECK (DEBUG)
# =========================
print("\nRequest columns:")
print(requests_df.columns.tolist())

print("\nMetadata columns:")
print(metadata_df.columns.tolist())


# =========================
# RENAME COLUMNS FOR CONSISTENCY
# =========================
# Make column names easier to work with
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


# ============================
# DEBUG: CHECK ID OVERLAP
# ============================
# Ensures request IDs match between request data and cold-start data
print("\nSample request IDs from requests:")
print(requests_df["request_id"].astype(str).head())

print("\nSample request IDs from cold starts:")
print(cold_df["request_id"].astype(str).head())

request_ids = set(requests_df["request_id"].astype(str).str.strip())
cold_ids = set(cold_df["request_id"].astype(str).str.strip())

matches = request_ids.intersection(cold_ids)

print("\nNumber of overlapping request IDs:", len(matches))


# =========================
# FEATURE ENGINEERING
# =========================

# Extract pool name from pod_id
requests_df["pool_name"] = requests_df["pod_id"].apply(extract_pool_name)

# Create unique function identifier (func_id)
# Combines function name + user + pool
requests_df["func_id"] = (
    requests_df["func_name"].astype(str)
    + "---"
    + requests_df["user_id"].astype(str)
    + "---"
    + requests_df["pool_name"].astype(str)
)

# Split metadata column into trigger_type and invocation_type
split_cols = metadata_df["trigger_invocation"].astype(str).str.rsplit("-", n=1, expand=True)
metadata_df["trigger_type"] = split_cols[0]
metadata_df["invocation_type"] = split_cols[1]


# =========================
# MERGE REQUESTS + METADATA
# =========================
merged_df = requests_df.merge(
    metadata_df[["func_id", "cpu_request", "runtime", "trigger_type", "invocation_type"]],
    on="func_id",
    how="left"
)


# =========================
# ADD COLD START LABELS
# =========================
# Mark requests that appear in cold-start dataset
cold_ids = set(cold_df["request_id"].astype(str).str.strip())
merged_df["cold_start_flag"] = merged_df["request_id"].astype(str).str.strip().isin(cold_ids)


# Check merge quality
print("\nMatched trigger_type rows:", merged_df["trigger_type"].notna().sum())
print("Missing trigger_type rows:", merged_df["trigger_type"].isna().sum())


# =========================
# SELECT FINAL COLUMNS
# =========================
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


# =========================
# DATA CLEANING
# =========================
# Convert numeric columns
clean_df["event_time"] = pd.to_numeric(clean_df["event_time"], errors="coerce")
clean_df["exec_time"] = pd.to_numeric(clean_df["exec_time"], errors="coerce")
clean_df["cpu_request"] = pd.to_numeric(clean_df["cpu_request"], errors="coerce")

# Drop invalid rows
clean_df = clean_df.dropna(subset=["event_time", "request_id", "func_id"])

# Sort by time (important for simulation)
clean_df = clean_df.sort_values("event_time").reset_index(drop=True)


# =========================
# SAVE CLEAN DATASET
# =========================
clean_df.to_csv(OUTPUT_PATH, index=False)

print("\nSaved cleaned file to:")
print(OUTPUT_PATH)


# =========================================
# CREATE SIMULATOR INPUT FILE
# =========================================
# Keep only essential columns for simulation
simple_df = clean_df[[
    "event_time",
    "func_id",
    "trigger_type",
    "exec_time",
    "cold_start_flag"
]].copy()

SIMPLE_OUTPUT_PATH = r"C:\Users\hanon\OneDrive\Documents\region2_simulator_input.csv"

simple_df.to_csv(SIMPLE_OUTPUT_PATH, index=False)

print("\nSaved simplified simulator file to:")
print(SIMPLE_OUTPUT_PATH)


print("\nDone.")

