# Python program for weighted job scheduling using Dynamic
# Programming and Binary Search

# Class to represent a job

VALUE=9999
class Job:
    def __init__(self, start, finish, profit,compulsory):
        self.start  = start
        self.finish = finish
        self.profit  = profit
        self.actual=profit
        self.compulsory=compulsory
        if(self.compulsory=="I"):
            self.actual=self.profit
            self.profit=VALUE+self.actual


# A Binary Search based function to find the latest job
# (before current job) that doesn't conflict with current
# job.  "index" is index of the current job.  This function
# returns -1 if all jobs before index conflict with it.
# The array jobs[] is sorted in increasing order of finish
# time.
def binarySearch(job, start_index):

    # Initialize 'lo' and 'hi' for Binary Search
    lo = 0
    hi = start_index - 1

    # Perform binary Search iteratively
    while lo <= hi:
        mid = (lo + hi) // 2
        if job[mid].finish <= job[start_index].start:
            if job[mid + 1].finish <= job[start_index].start:
                lo = mid + 1
            else:
                return mid
        else:
            hi = mid - 1
    return -1

# The main function that returns the maximum possible
# profit from given array of jobs
def schedule(job):

    # Sort jobs according to finish time
    job = sorted(job, key = lambda j: j.finish)

    # Create an array to store solutions of subproblems.  table[i]
    # stores the profit for jobs till arr[i] (including arr[i])
    n = len(job)
    table = [0 for _ in range(n)]

    table[0] = job[0].profit

    # Fill entries in table[] using recursive property
    for i in range(1, n):

        # Find profit including the current job
        inclProf = job[i].profit
        l = binarySearch(job, i)
        if (l != -1):
            inclProf += table[l]

        # Store maximum of including and excluding
        table[i] = max(inclProf, table[i - 1])

    return table[n-1]


cases=int(input())
for _ in range(cases):
    shops=int(input())
    job=[]
    factor=0
    for j in range(shops):
        params=input().split()
        if(params[-1]=="I"):
            factor+=VALUE#-int(params[-2])
        jbb=Job(int(params[0]),int(params[0])+int(params[1]),int(params[2]),params[3])
        job.append(jbb)
 #Driver code to test above function
#job = [Job(1, 2, 50,"N"), Job(2, 5, 20,"N")]
     #Job(6, 19, 100,"N"), Job(2, 100, 200,"N")]
    print("Optimal profit is"),
    print(schedule(job)-factor)