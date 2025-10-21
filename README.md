# SpotiETL
Fetching data from Spotify REST API Transform and Load

## Overview
An educational project aimed at practicing and enhancing my skills for self-development.  
The main focus areas are:
1. Data integration  
2. Working with APIs  
3. ETL processes  

## Workflow
1. Downloading data from a REST API using Python  
2. Loading the data into MS SQL Server (data warehouse)  

## Technologies
1. Python  
2. MS SQL Server  

## Design Decisions

### A single ETL pipeline instead of three

Initially, I considered creating separate ETL processes for each table (`artists`, `albums`, `tracks`).  
This approach would have been simpler to implement, but it came with several drawbacks:

- **Duplicated logic** – each ETL path would repeat some parts of the code (e.g., fetching the artist ID).
- **Lack of data consistency** – running separate pipelines could lead to a situation where data in the `albums` and `tracks` tables comes from a different point in time than the data in `artists`.

For these reasons, I decided to build **one integrated pipeline** that:
1. Fetches the artist data.
2. Uses it to retrieve related albums.
3. For each album, fetches its tracks.
4. Splits the results into three datasets (for the `artists`, `albums`, and `tracks` tables) and loads them into the database within a single transaction.

With this solution:
- **the number of API requests is minimized**,  
- **the entire process runs faster**,  
- **data consistency across tables is guaranteed**
