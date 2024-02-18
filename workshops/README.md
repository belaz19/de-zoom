# **Homework**: Data talks club data engineering zoomcamp Data loading workshop

Hello folks, let's practice what we learned - Loading data with the best practices of data engineering.

Here are the exercises we will do



# 1. Use a generator

Remember the concept of generator? Let's practice using them to futher our understanding of how they work.

Let's define a generator and then run it as practice.

**Answer the following questions:**

- **Question 1: What is the sum of the outputs of the generator for limit = 5?**
- **Question 2: What is the 13th number yielded**

I suggest practicing these questions without GPT as the purpose is to further your learning.
def square_root_generator(limit):
    n = 1
    while n <= limit:
        yield n ** 0.5
        n += 1

# Example usage:
limit = 13
generator = square_root_generator(limit)
s = 0

for sqrt_value in generator:
    print("sqrt is ", sqrt_value)
    s += sqrt_value
    print("sum is ", s)


# 2. Append a generator to a table with existing data


Below you have 2 generators. You will be tasked to load them to duckdb and answer some questions from the data

1. Load the first generator and calculate the sum of ages of all people. Make sure to only load it once.
2. Append the second generator to the same table as the first.
3. **After correctly appending the data, calculate the sum of all ages of people.**



def people_1():
    for i in range(1, 6):
        yield {"ID": i, "Name": f"Person_{i}", "Age": 25 + i, "City": "City_A"}

for person in people_1():
    print(person)


def people_2():
    for i in range(3, 9):
        yield {"ID": i, "Name": f"Person_{i}", "Age": 30 + i, "City": "City_B", "Occupation": f"Job_{i}"}


for person in people_2():
    print(person)

#pip install dlt[duckdb]
import dlt
import duckdb
# define the connection to load data
pipeline=dlt.pipeline(destination='duckdb', dataset_name='people_1_2')
def people_1():
    for i in range(1, 6):
        yield {"ID": i, "Name": f"Person_{i}", "Age": 25 + i, "City": "City_A"}

for person in people_1():
    print(person)
    # I have done this ingestion below one time!
    #data = [person]
    #info=pipeline.run(data, table_name="people_table", write_disposition="append", primary_key='ID')
# show the outcome
conn = duckdb.connect(f"{pipeline.pipeline_name}.duckdb")
# let's see the tables
conn.sql(f"SET search_path = '{pipeline.dataset_name}'")
print('Loaded tables: ')
display(conn.sql("show tables"))
#pip install numpy
#pip install pandas
import numpy as np
import pandas as pd
# print all
people_sql = conn.sql("SELECT * FROM people_table").df()
display(people_sql)
def people_2():
    for i in range(3, 9):
        yield {"ID": i, "Name": f"Person_{i}", "Age": 30 + i, "City": "City_B", "Occupation": f"Job_{i}"}


for person in people_2():
    print(person)

    # I have done this ingestion below one time!
    #data = [person]
    #info=pipeline.run(data, table_name="people_table", write_disposition="append", primary_key='ID')
# print sum age
people_sql = conn.sql("SELECT sum(age) FROM people_table").df()
display(people_sql)

# 3. Merge a generator

Re-use the generators from Exercise 2.

A table's primary key needs to be created from the start, so load your data to a new table with primary key ID.

Load your first generator first, and then load the second one with merge. Since they have overlapping IDs, some of the records from the first load should be replaced by the ones from the second load.

After loading, you should have a total of 8 records, and ID 3 should have age 33.

Question: **Calculate the sum of ages of all the people loaded as described above.**

def people_1():
    for i in range(1, 6):
        yield {"ID": i, "Name": f"Person_{i}", "Age": 25 + i, "City": "City_A"}

for person in people_1():
    print(person)
    # I have done this ingestion below one time!
    #data = [person]
    #info=pipeline.run(data, table_name="people_merge", write_disposition="merge", primary_key='ID')
def people_2():
    for i in range(3, 9):
        yield {"ID": i, "Name": f"Person_{i}", "Age": 30 + i, "City": "City_B", "Occupation": f"Job_{i}"}


for person in people_2():
    print(person)

    # I have done this ingestion below one time!
    #data = [person]
    #info=pipeline.run(data, table_name="people_merge", write_disposition="merge", primary_key='ID')
# print all
people_sql = conn.sql("SELECT * FROM people_merge").df()
display(people_sql)
# print sum age
people_sql = conn.sql("SELECT sum(age) FROM people_merge").df()
display(people_sql)
