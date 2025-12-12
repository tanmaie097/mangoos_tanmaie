# ~/Desktop/magnoos/exercises/06_hadoop/simple_loganalyser/output/plot_results.py
import matplotlib.pyplot as plt

labels = []
values = []

with open("/Users/tanmaie/Desktop/magnoos/exercises/06_hadoop/simple_loganalyser/output/results.txt") as f:
    for line in f:
        parts = line.strip().split()
        if len(parts) >= 2:
            key = parts[0]
            val = int(parts[1])
            labels.append(key)
            values.append(val)

plt.figure(figsize=(6,4))
plt.bar(labels, values)
plt.title("Log level counts")
plt.xlabel("Log level")
plt.ylabel("Count")
plt.tight_layout()
plt.show()
