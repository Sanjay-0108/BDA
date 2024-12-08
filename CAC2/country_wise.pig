-- Load the data
raw_data = LOAD '/home/hadoop/Downloads/Dataset(1).csv' USING PigStorage(';') AS (
    BillNo:chararray,
    Items:chararray,
    Date:chararray,
    Price:float,
    Quantity:int,
    CustomerID:chararray,
    Country:chararray
);

-- Filter out header if present
filtered_data = FILTER raw_data BY BillNo != 'BillNo';

-- Group by country and bill number to get unique transactions
country_bills = GROUP filtered_data BY (Country, BillNo);

-- Get unique transactions per country
country_unique_trans = FOREACH country_bills GENERATE group.Country as Country;

-- Group by country to count transactions
country_counts = GROUP country_unique_trans BY Country;

-- Count transactions per country
final_count = FOREACH country_counts GENERATE 
    group as Country,
    COUNT(country_unique_trans) as TransactionCount;

-- Order by transaction count descending
ordered_result = ORDER final_count BY TransactionCount DESC;

-- Store the results
STORE ordered_result INTO '/home/hadoop/Downloads/country_wise' USING PigStorage(',');

