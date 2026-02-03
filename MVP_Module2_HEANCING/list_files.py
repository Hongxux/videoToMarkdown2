import os

path = r"d:\videoToMarkdownTest2\find_alg\video_01"
try:
    files = os.listdir(path)
    for f in files:
        print(f"FOUND: {f}")
except Exception as e:
    print(f"ERROR: {e}")
