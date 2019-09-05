The following program aggregates csv data based on the column the column headers you specify.
The data proccessing has been implemented in such a way the the run time of the program is O(nlogn).
This is done by using the stable sorting property of javas sorting algorthim. All data is first sorted according to which columns where specified.
Then the program can just use iterative loops to count, add and remove duplicates. 
