# Checkpoint Question 1  
## What is the main goal of the Spark stage in the London Transport Data Engineering Project?

**Your answer:**  
It plays central role because makes all: extractions, transformations(cleaning ang joining), loading(producing outputs file)  using its engine. 

---

# Checkpoint Question 2  
## How does Day 2 connect to the work you completed on Day 1?

**Your answer:**  
We used the same raw data and received at the very end the same output data but in different form. In Day1 the final data was saved in database, for Day2 - in csv-files.

---

# Checkpoint Question 3  
## Which raw files were the most important for the Day 2 Spark workflow, and why?

**Your answer:**  
Like for Day1 the journeys.json file is the most important. It contains fact data on which the whole analysis has been build. Other files are also important but they have lookup data.

---

# Checkpoint Question 4  
## What are the main differences between the Day 1 local pipeline approach and the Day 2 Spark approach?

**Your answer:**  
In Day1 we the different parts of pipeline were made by different instruments: python scripts, SQL queries.
In Day2 we used pyspark for all parts of pipeline.

---

# Checkpoint Question 5  
## Why is this Spark stage important for your future portfolio and professional growth?

**Your answer:**  
It's widely used, especially by big companies. Maybe for some startups, where the amount of data is small, it's not reasonable to set up a spark cluster. But anyway it's a good knowledge to show on interview.

---

# Final reflection

## What was the most important thing you learned from Day 2?

**Your answer:**  
That pyspark is powerful instrument for data engineering.