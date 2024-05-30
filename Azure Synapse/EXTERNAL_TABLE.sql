
------ Create External Table----
CREATE EXTERNAL TABLE ml_result (
    topic NVARCHAR(100),
    qty INT
)
WITH (
    LOCATION = 'https://capstone2storageacc.dfs.core.windows.net/bd-project/BI/ml_result.csv',
    DATA_SOURCE = datasourcer,
    FILE_FORMAT = CSV
);
