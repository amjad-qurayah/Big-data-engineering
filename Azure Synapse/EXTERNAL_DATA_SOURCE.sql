
------ Create External Data Source----
CREATE EXTERNAL DATA SOURCE datasourcer
WITH (
    LOCATION = 'https://capstone2storageacc.dfs.core.windows.net/bd-project/BI/ml_result.csv);
