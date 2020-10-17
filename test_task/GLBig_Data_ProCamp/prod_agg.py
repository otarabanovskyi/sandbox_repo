"""
Task
Find the attached file test-task_dataset_summer_products.cvs with the clothing products dataset. For each product, there are the following fields of interests:
•	origin_country - country of origin for the products
•	price - price of the products
•	rating_count - how many times the product has been rated by user/consumer
•	rating_five_count - how many times the product has been rated by user/consumer with five stars

Using Programming Language of your choice (Java/Python/Scala), calculate the following metrics for each Country of Origins:

1.	Average price of product
2.	Share of five-star products
Timeline
Please send back a link to your public repository with your source code within the next 3 business days.

SQL Representation
select avg(price), (sum(rating_five_count) / sum(rating_count)) * 100 as five_percentage, origin_country
from summer_products
group by origin_country
order by origin_country
"""

import pandas as pd

# Load dataset
col_list = ['origin_country', 'price', 'rating_count', 'rating_five_count']
df = pd.read_csv('test-task_dataset_summer_products.csv', usecols=col_list, header=0)

# Define aggregates
aggregations = {
    'price': 'mean',
    'rating_five_count': 'sum',
    'rating_count': 'sum'
}
# Group by 'origin_country' (including NUll values)
agg_result = df.groupby('origin_country', dropna=False).agg(aggregations)

# Add calculated column 'five_percentage'
agg_result['five_percentage'] = agg_result['rating_five_count'] / agg_result['rating_count'] * 100

# Drop extra columns
agg_result = agg_result.drop(['rating_five_count', 'rating_count'], axis=1)

# Rename 'price' to 'avg_price'
agg_result = agg_result.rename(columns={'price': 'avg_price'}, inplace=False)

# Reorder columns according to the SQL Representation
column_names = ['avg_price', 'five_percentage', 'origin_country']
agg_result = agg_result.reset_index()
agg_result = agg_result.reindex(columns=column_names)

# Print result
print(agg_result)
