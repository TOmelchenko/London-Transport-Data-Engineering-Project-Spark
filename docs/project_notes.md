You should write short notes about:

## which files were used in the Spark pipeline
For the main Spark reporting pipeline the following files have been used:

- stations.csv
- lines.csv
- journeys.json
- boroughs.csv
- zones.csv

## what transformations were done
For the all lookups (stations, lines, boroughs, zones) we made:
- removes rows with missing IDs
- removes duplicate IDs
- trims extra spaces
- standardizes text formatting

For fact data (journeys):
- keeps only rows with valid IDs
- keeps only rows with valid numeric passenger counts
- keeps only rows with valid numeric delay values
- keeps only correctly formatted dates
- casts values into proper data types
- cleans text columns

## what outputs were created
We created 4 reports:
1. main - "transport_report" - that contains all data:
- journey activity
- station details
- borough context
- zone context
- line information

2. and 3 reports, that answer special questions:
- top_stations - by number of passengers;
- line_delay - top lines by avg_delay_minutes;
- borough_passengers - top borough by number of passengers;

## what you noticed about Spark compared to Day 1
With spark we use ETL approach. Spark makes all: extractions, transformations(cleaning ang joining), loading(producing outputs file)  using its engine. 
We also got the final output in files, not in DB.

