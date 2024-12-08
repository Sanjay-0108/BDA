-- Load the retail data
retail_data = LOAD '/home/hadoop/Downloads/Dataset.csv' USING PigStorage('\t') AS (
    BillNo:chararray,
    ItemName:chararray,
    Quantity:int,
    Date:chararray,
    Price:float,
    CustomerID:chararray,
    Country:chararray
);

-- Calculate revenue for each row (Quantity * Price)
revenue_data = FOREACH retail_data GENERATE
    Country,
    (Quantity * Price) as revenue;

-- Group the data by country
grouped_by_country = GROUP revenue_data BY Country;

-- Calculate total revenue for each country
country_revenue = FOREACH grouped_by_country GENERATE
    group as Country,
    SUM(revenue_data.revenue) as TotalRevenue;

-- Order results by revenue in descending order
ordered_results = ORDER country_revenue BY TotalRevenue DESC;

-- Store the results
STORE ordered_results INTO '/home/hadoop/Downloads/revenue_by_country' USING PigStorage(',');




