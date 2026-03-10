import threading
import time

# Define scoring rules
rules = {
    "excellent": 1,
    "amazing": 1,
    "outstanding": 1.5,
    "good": 0.5,
    "nice": 0.5,
    "bad": -0.5,
    "poor": -1,
    "terrible": -1.5
}

# Function to process each file
def process_file(filename):
    with open(filename, "r") as f:
        text = f.read().lower()

    score = 0

    # Apply rules
    for word in rules:
        if word in text:
            score += rules[word]

    print((text.strip(), score))


files = ["file1.txt", "file2.txt", "file3.txt", "file4.txt", "file5.txt"]

start_multi = time.time()

threads = []

for file in files:
    t = threading.Thread(target=process_file, args=(file,))
    threads.append(t)
    t.start()

for t in threads:
    t.join()

end_multi = time.time()

print("Multitasking Time:", round(end_multi - start_multi, 4))



"""
OUTPUT:
PS C:\Users\rlith\OneDrive\Desktop\INFOSYS SPRINGBOARD INTERN\SINGLE VS MULTI TASKING> python Multithreaded_Processing_With_Scoring_Rules.py
('the product is excellent and amazing', 2)
('the service is good but the delivery is bad', 0.0)
('outstanding performance and excellent quality', 2.5)
('poor packaging but nice design', -0.5)
('terrible experience and bad support', -2.0)
Multitasking Time: 0.0024

NOTE: The actual output Order may vary based on the content of the files and the scoring rules applied.
"""