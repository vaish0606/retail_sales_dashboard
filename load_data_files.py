import os
import shutil

INCOMING_DIR = "incoming_data"
RAW_DIR = "raw_data"
MASTER_DIR = "master_data"
LOG_FILE = "processed_files.txt"

def load_new_files():
    # Read log of already processed files
    if os.path.exists(LOG_FILE):
        with open(LOG_FILE, "r") as f:
            processed_files = set(line.strip() for line in f)
    else:
        processed_files = set()

    # Ensure raw_data/ exists and is clean
    os.makedirs(RAW_DIR, exist_ok=True)
    for file in os.listdir(RAW_DIR):
        os.remove(os.path.join(RAW_DIR, file))

    # Copy master files every time
    for file in os.listdir(MASTER_DIR):
        shutil.copy(os.path.join(MASTER_DIR, file), os.path.join(RAW_DIR, file))

    # Copy only new incoming files
    new_files = []
    for file in os.listdir(INCOMING_DIR):
        if file not in processed_files:
            shutil.copy(os.path.join(INCOMING_DIR, file), os.path.join(RAW_DIR, file))
            new_files.append(file)

    # Log new files
    if new_files:
        with open(LOG_FILE, "a") as f:
            for file in new_files:
                f.write(file + "\n")
        print(f"✅ Loaded {len(new_files)} new files into raw_data/")
    else:
        print("⚠️ No new files to load.")

if __name__ == "__main__":
    load_new_files()
