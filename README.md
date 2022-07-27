# SQL_Pandas
Problem: There was a need to batch rows in a SQL table such that the sum of values in a specific column per batch remain the same. 

Example: The requirement is to divide the rows in the table below into 3 batches so that sum of colB values per batch remain the same, 
that is, sum/count = 40/3 ~ 13. The assigned batch is in the third column.
colA | colB | BatchID
-------------------
A1 | 4 | 1
B1 | 5 | 1
C1 | 8 | 2
D1 | 4 | 2
E1 | 19 | 3
