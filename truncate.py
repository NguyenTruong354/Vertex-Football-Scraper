with open("scheduler/live_pool.py", "r", encoding="utf-8") as f:
    lines = f.readlines()

with open("scheduler/live_pool.py", "w", encoding="utf-8") as f:
    f.writelines(lines[:720])
