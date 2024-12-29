# [570. Managers with at Least 5 Direct Reports](https://leetcode.com/problems/managers-with-at-least-5-direct-reports?envType=study-plan-v2&envId=top-sql-50)

**Type:** Easy <br>
**Topics:** Database <br>
**Companies:** Google, Amazon, Bloomberg, Microsoft, Meta, Uber, Yahoo, Adobe, Apple, Cognizant
<hr>

Table: `Employee`
```
+-------------+---------+
| Column Name | Type    |
+-------------+---------+
| id          | int     |
| name        | varchar |
| department  | varchar |
| managerId   | int     |
+-------------+---------+
id is the primary key (column with unique values) for this table.
Each row of this table indicates the name of an employee, their department, and the id of their manager.
If managerId is null, then the employee does not have a manager.
No employee will be the manager of themself.
```

Write a solution to find managers with at least **five direct reports**.

Return the result table in **any order**.

The result format is in the following example.
<hr> 

### Examples
- **Example 1:**
    - **Input:**
        ```
        Employee table:
        +-----+-------+------------+-----------+
        | id  | name  | department | managerId |
        +-----+-------+------------+-----------+
        | 101 | John  | A          | null      |
        | 102 | Dan   | A          | 101       |
        | 103 | James | A          | 101       |
        | 104 | Amy   | A          | 101       |
        | 105 | Anne  | A          | 101       |
        | 106 | Ron   | B          | 101       |
        +-----+-------+------------+-----------+
        ```
    - **Output:** 
        ```
        +------+
        | name |
        +------+
        | John |
        +------+
        ```
<hr>

### Hints
- Try to get all the mangerIDs that have count bigger than 5
- Use the last hint's result as a table and do join with origin table at id equals to managerId
- This is a very good example to show the performance of SQL code. Try to work out other solutions and you may be surprised by running time difference.
- If your solution uses 'IN' function and runs more than 5 seconds, try to optimize it by using 'JOIN' instead.