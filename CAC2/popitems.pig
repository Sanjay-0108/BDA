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

-- Step 1: Group by Country and ItemName
grouped_by_country_item = GROUP retail_data BY (Country, ItemName);

-- Step 2: Count the occurrences of each item per country
count_items_by_country = FOREACH grouped_by_country_item GENERATE 
    group.Country AS Country, 
    group.ItemName AS ItemName,
    SUM(retail_data.Quantity) AS TotalQuantity;

-- Step 3: Sort by TotalQuantity in descending order to find the most popular items
sorted_by_popularity = ORDER count_items_by_country BY TotalQuantity DESC;

-- Step 4: Group by Country to apply limit per country
grouped_by_country = GROUP sorted_by_popularity BY Country;

-- Step 5: Limit to top 5 items per country
top_5_items_by_country = FOREACH grouped_by_country {
    sorted_data = LIMIT sorted_by_popularity 5;
    GENERATE FLATTEN(sorted_data);
}

-- Step 6: Store the results (Top 5 popular items by country)
STORE top_5_items_by_country INTO '/home/hadoop/Downloads/top_5_popular_items_by_country' USING PigStorage(',');

