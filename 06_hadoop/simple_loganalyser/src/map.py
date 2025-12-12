# Counts each log level (“INFO”, “WARN”, “ERROR”).

import sys

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    log_level = line.split()[0]
    print(f"{log_level}\t1")
