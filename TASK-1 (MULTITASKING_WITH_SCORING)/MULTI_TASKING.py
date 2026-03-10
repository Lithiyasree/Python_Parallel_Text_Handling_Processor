import threading
import time

#Multi Tasking – Using Threading + For Loop

def print_numbers():
    for i in range(1, 4):
        print("Number:", i)
        time.sleep(1)

start = time.time()

t1 = threading.Thread(target=print_numbers)
t2 = threading.Thread(target=print_numbers)

t1.start()
t2.start()

t1.join()
t2.join()

end = time.time()

print("Total Time:", round(end - start, 2))


# Multi Tasking (Functions Parallel)

def task1():
    print("Task 1 Start")
    time.sleep(2)
    print("Task 1 End")

def task2():
    print("Task 2 Start")
    time.sleep(2)
    print("Task 2 End")

t1 = threading.Thread(target=task1)
t2 = threading.Thread(target=task2)

t1.start()
t2.start()

t1.join()
t2.join()

# Multi Tasking – File Processing

def read_file(filename):
    with open(filename, "r") as f:
        print("Reading:", filename)
        time.sleep(2)
        print("Completed:", filename)

files = ["file1.txt", "file2.txt", "file3.txt"]

threads = []
start = time.time()

for file in files:
    t = threading.Thread(target=read_file, args=(file,))
    threads.append(t)
    t.start()

for t in threads:
    t.join()

end = time.time()

print("Total Time:", round(end - start, 2))



'''
OUTPUT:
PS C:\Users\rlith\OneDrive\Desktop\INFOSYS SPRINGBOARD INTERN\SINGLE VS MULTI TASKING> python MULTI_TASKING.py   
Number: 1
Number: 1
Number: 2
Number: 2
Number: 3
Number: 3
Total Time: 3.0
Task 1 Start
Task 2 Start
Task 1 End
Task 2 End
Reading: file1.txt
Reading: file2.txt
Reading: file3.txt
Completed: file1.txt
Completed: file3.txt
Completed: file2.txt
Total Time: 2.0
'''