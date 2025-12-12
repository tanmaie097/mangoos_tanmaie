# Aggregates the counts.

import sys

current_key = None
current_count = 0

for line in sys.stdin:
    key, val = line.strip().split("\t")
    val = int(val)

    if key == current_key:
        current_count += val
    else:
        if current_key is not None:
            print(f"{current_key}\t{current_count}")
        current_key = key
        current_count = val

if current_key:
    print(f"{current_key}\t{current_count}")