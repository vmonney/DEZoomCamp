# Clustering and Partitioning

When it comes to partitioning and clustering in BigQuery, you need to consider how you're going to be querying your data most often.

Partitioning is a way of dividing a table into segments, or partitions, based on the values in a particular column, and is often used for improving query performance. BigQuery can scan only the partitions needed for a particular query, rather than scanning the entire table. Partitioning is most effective on timestamp or date columns and when you frequently use filters in WHERE clauses based on those columns.

In your case, the Level 1tpep_pickup_datetime and Level 1tpep_dropoff_datetime columns seem to be the most likely candidates for partitioning, particularly if you often run queries where you're looking at data from specific time periods.

Clustering is a way of organizing data in a table based on the contents of specific columns. BigQuery sorts the data based on the clustered columns and stores it in a way that can significantly improve the performance of certain types of queries. Clustering is particularly useful for large tables and for columns where the number of distinct values is small compared to the number of rows, and when those columns are frequently used in WHERE, ORDER BY, and GROUP BY clauses.

Looking at your data, you might want to consider clustering based on the Level 1VendorID, Level 1PULocationID, and Level 1DOLocationID columns. These columns seem to be identifiers for vendors and locations, and are likely to be used frequently in queries for filtering, sorting, or grouping data.

Remember that you can only use one column for partitioning but you can cluster by multiple columns in BigQuery. Also, these choices will highly depend on the queries you are frequently running and the specific distribution of your data.

You should also consider that both partitioning and clustering will increase the cost of data loading, as BigQuery will have to sort and organize the data when it's loaded. However, this is typically offset by the decrease in query costs and increase in query speed.

## Best Practices
  - Avoid SELECT *
  - Place the table with the largest number of rows first, followed by the table with the fewest rows, and then place the remaining tables by decreasing size.

# External table
External tables in BigQuery are tables that are backed by data stored in an external source, such as Google Cloud Storage, rather than being stored in BigQuery itself. This can be useful when you have large amounts of data stored in GCS and you want to be able to query that data without having to load it into BigQuery first.

-- Creating external table referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE `taxi-rides-ny.nytaxi.external_yellow_tripdata`
OPTIONS (
  format = 'CSV',
  uris = ['gs://nyc-tl-data/trip data/yellow_tripdata_2019-*.csv', 'gs://nyc-tl-data/trip data/yellow_tripdata_2020-*.csv']
);
