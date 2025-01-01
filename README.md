# 570. Managers with at Least 5 Direct Reports

- ### General Approach:
    The goal is to find managers who have at least 5 direct reports. This can be done by:
    1. **Self-Join**: The core of the solution is to join the employee table with itself. This allows you to associate each employee with their manager, creating a connection between employees and the people who manage them.
    - In the first part of the join, the `id` column of the table represents the employee, while in the second part, the `managerId` column represents the manager of that employee.
    2. **Count Direct Reports**: Once you have the self-join, you need to count how many employees are reporting to each manager. This is done by grouping the data by the `managerId` (or `id` of the manager) and counting how many employees fall under each manager.
        - This helps us determine how many direct reports each manager has.
    3. **Filter**: After counting the direct reports, the next step is to **filter out managers who have fewer than 5 direct reports**. We are only interested in managers with 5 or more direct reports, so this filtering step is crucial.
    4. **Return Manager Names**: Finally, the manager's name is returned for those who have 5 or more direct reports. This gives the final list of qualifying managers.

- ### SQL Approach:
    - **Join**: In SQL, a **self-join** is performed on the `id` and `managerId` columns. The goal is to link the employees with their managers.
    - **Group and Count**: After the join, the data is grouped by `id` (manager’s ID), and `COUNT` is used to find how many direct reports each manager has.
    - **Filter**: The `HAVING` clause is used to filter out managers who have fewer than 5 direct reports.
    - **Return**: The final result selects the `name` of managers who have 5 or more direct reports.

    **Explanation**: SQL handles the task with declarative queries. You specify the result you want (`SELECT`), how to join (`INNER JOIN`), how to group (`GROUP BY`), and apply conditions after grouping (`HAVING`).

- ### PySpark Approach:
    - **Join**: PySpark performs the self-join using the `join` method, linking the `id` (employee) and `managerId` (manager). This is similar to SQL's `INNER JOIN`.
    - **Group and Count**: After the join, the data is grouped by `managerId` (manager) and the number of direct reports is counted using the `count` function.
    - **Filter**: The `filter` method is used to exclude managers who have fewer than 5 direct reports.
    - **Return**: Finally, you select the manager names who meet the condition using the `select` method.

    **Explanation**: PySpark leverages the DataFrame API for distributed data processing. The operations of join, group, and filter are similar to SQL, but executed on distributed datasets, which makes PySpark suitable for large-scale datasets.

- ### Pandas Approach:
    - **Merge**: In Pandas, you first create two DataFrames:
        - `e1` is the DataFrame of employees without the `managerId` column.
        - `e2` is the DataFrame where `managerId` is renamed to `id` so it can be used for the join.
    After that, a **merge** is performed on these two DataFrames to link employees with their managers.
    - **Group and Count**: After merging, you group by `managerId` and use the `size()` function to count the number of direct reports for each manager.
    - **Filter**: You filter out managers who have fewer than 5 direct reports by using boolean indexing.
    - **Return**: The result is the names of the managers who have 5 or more direct reports, extracted from the original employee DataFrame.

    **Explanation**: Pandas is ideal for handling data in memory and works well with local datasets. The operations are performed imperatively with easy-to-understand functions like `merge`, `groupby`, and `size`. It's a simpler approach suited for smaller datasets or when data is already loaded in memory.

- ### Code Implementation
    - **SQL:**
        ```sql []
        -- Select the names of employees who manage at least 5 others
        SELECT e1.name
        FROM Employee e1
        -- Join the Employee table with itself to link managers (e1) to their direct reports (e2)
        INNER JOIN Employee e2
        ON e1.id = e2.managerId
        -- Group the results by manager (e1.name) and manager's ID (e2.managerId) to count their direct reports
        GROUP BY e1.name, e2.managerId
        -- Filter the managers who have 5 or more direct reports
        HAVING COUNT(*) >= 5
        ```
    - **PySpark:**
        ```python3 []
        def find_managers(employee: spark_DataFrame) -> spark_DataFrame:
            # Step 1: Perform a self-join on the Employee DataFrame to link employees with their managers
            # 'e1' represents the employee (manager in this case)
            # 'e2' represents the employee's direct reports
            join_result = employee.alias('e1').join(
                employee.alias('e2'), 
                on = (col('e1.id') == col('e2.managerId')),  # Join condition: e1.id = e2.managerId
                how = 'inner'  # Inner join, as we want only employees with managers
            ) \
            .select(
                col('e1.name').alias('manager_name'),  # Selecting manager's name from 'e1'
                col('e2.managerId')  # Selecting the managerId from 'e2' (direct reports)
            )

            # Step 2: Group the results by manager's name and managerId
            # We need to count the number of direct reports for each manager
            grouped_result = join_result.groupby(
                'manager_name', 'managerId'  # Group by manager's name and managerId
            ).agg(
                count('managerId').alias('direct_reports_count')  # Count the number of direct reports
            )

            # Step 3: Filter to keep only the managers who have 5 or more direct reports
            filtered_result = grouped_result.filter(
                col('direct_reports_count') >= 5  # Filter condition: direct_reports_count >= 5
            )

            # Step 4: Select only the manager names (those who have 5 or more direct reports)
            final_result = filtered_result.select('manager_name')

            # Return the final DataFrame containing the names of the managers
            return final_result
        ```
    - **Pandas**
        ```python3 []
        def find_managers(employee: pd.DataFrame) -> pd.DataFrame:
            # Step 1: Create a DataFrame `e1` which is the original employee DataFrame with the 'managerId' column dropped
            # 'e1' will represent employees (not managers) in the merge
            e1 = employee.drop(columns = ['managerId'])

            # Step 2: Create a DataFrame `e2` where the 'id' column is dropped, and 'managerId' is renamed to 'id'
            # 'e2' will represent the direct reports of the managers
            e2 = employee.drop(columns = ['id']).rename(columns = {'managerId': 'id'})

            # Step 3: Perform an inner merge (join) of `e1` and `e2` on 'id' column (manager's 'id' and employee's 'managerId')
            # The merge links employees (e1) to their direct reports (e2) based on matching 'id' (manager's ID)
            join = e1.merge(e2, on = 'id', how = 'inner', suffixes = ['_e1', '_e2'])

            # Step 4: Group the merged DataFrame by manager's 'id' (from the 'e1' part of the DataFrame)
            # Count the number of direct reports (i.e., size of each group) for each manager and reset the index
            manager = join.groupby(['id']).size().reset_index(name = 'managerCount')

            # Step 5: Filter out the managers who have fewer than 5 direct reports
            # `managerCount` is the number of direct reports, so we select managers with at least 5 direct reports
            managerIds = manager.id[manager['managerCount'] >= 5]

            # Step 6: Create a list `names` to store the names of the managers who have at least 5 direct reports
            # Loop through the `managerIds` and fetch the unique names for each qualifying manager
            names = []
            for managerId in managerIds:
                # Get the names of employees whose 'id' matches the current managerId
                names += list(np.unique(employee.name[employee['id'] == managerId]))

            # Step 7: Return the result as a new DataFrame with the column 'name' for the managers
            return pd.DataFrame(names, columns = ['name'])
        ```