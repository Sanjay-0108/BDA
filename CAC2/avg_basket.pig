-- Load the data
retail_data = LOAD '/home/hadoop/Downloads/Dataset.csv' USING PigStorage('\t') AS (
    BillNo:chararray,
    ItemName:chararray,
    Quantity:int,
    Date:chararray,
    Price:float,
    CustomerID:chararray,
    Country:chararray
);

-- Step 1: Count items per transaction (BillNo)
grouped_by_transaction = GROUP retail_data BY (BillNo, Country);

count_items_per_transaction = FOREACH grouped_by_transaction GENERATE 
    FLATTEN(group) AS (BillNo, Country), 
    COUNT(retail_data.ItemName) AS CountOfItemName;

-- Step 2: Group by Country and calculate the average
grouped_by_country = GROUP count_items_per_transaction BY Country;

average_item_count_per_country = FOREACH grouped_by_country GENERATE 
    group AS Country, 
    AVG(count_items_per_transaction.CountOfItemName) AS AverageBasketSize;

-- Store the results
STORE average_item_count_per_country INTO '/home/hadoop/Downloads/average_item_count_by_country' USING PigStorage(',');

