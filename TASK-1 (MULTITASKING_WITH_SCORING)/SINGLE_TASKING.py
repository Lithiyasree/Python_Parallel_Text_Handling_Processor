import time

#Single Tasking – Using For Loop

def print_numbers():
    for i in range(1, 4):
        print("Number:", i)
        time.sleep(1)

start = time.time()

print_numbers()
print_numbers()

end = time.time()

print("Total Time:", round(end - start, 2))


# Single Tasking (Functions Sequential)

def task1():
    print("Task 1 Start")
    time.sleep(2)
    print("Task 1 End")

def task2():
    print("Task 2 Start")
    time.sleep(2)
    print("Task 2 End")

task1()
task2()

# Single Tasking – File Processing

def read_file(filename):
    with open(filename, "r") as f:
        print("Reading:", filename)
        time.sleep(2)
        print("Completed:", filename)

files = ["file1.txt", "file2.txt", "file3.txt"]

start = time.time()

for file in files:
    read_file(file)

end = time.time()

print("Total Time:", round(end - start, 2))



'''
OUTPUT:
PS C:\Users\rlith\OneDrive\Desktop\INFOSYS SPRINGBOARD INTERN\SINGLE VS MULTI TASKING> python SINGLE_TASKING.py
Number: 1
Number: 2
Number: 3
Number: 1
Number: 2
Number: 3
Total Time: 6.01
Task 1 Start
Task 1 End
Task 2 Start
Task 2 End
Reading: file1.txt
Completed: file1.txt
Reading: file2.txt
Completed: file2.txt
Reading: file3.txt
Completed: file3.txt
Total Time: 6.01
'''