import glob
from collections import defaultdict
import re
files = glob.glob("files/pg*")

word_dict = defaultdict(int)


for file in files:
    with open(file) as f:
        file_string = f.read()
        file_string = re.sub("[^A-Za-z]", " ", file_string)
        file_arr = file_string.split()
        for word in file_arr:
            word_dict[word] += 1


files = glob.glob("files/reduce*")

result_dict = dict()
for file in files:
    with open(file) as f:
        lines = f.readlines()

        for line in lines:
            if line:
                key, val = line.split(" ")
                result_dict[key] = int(val)


for key, val in word_dict.items():
    result_val = result_dict.get(key)
    if result_val is None or result_val != val:
        print(f"incorrect {key} {val} is not equivalent to {key} {result_val}")
        exit(1)

print("success")
