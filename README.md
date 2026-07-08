# Carlos' very cool Lakehouse


## Introduction
Lakehouse, a data architecture that integrates the flexibility, cost-efficiency, and scalability of Data Lakes with the robust data management features and ACID Transactions typical of Data Warehouses.

As a Data Engineer working in Databricks and Snowflake on a daily basis at work, I find interesting how these systems are built across organizations.

On a personal note, I enjoy open-source initiatives and discovering new tools which I can integrate to my daily work, or just discover its functionality for fun.

### Data Content

This Lakehouse won't carry any specific kind of data, I'm planning on simply integrating any data source I find interesting and test any releases of Software I like.

### Notes on Apache Ozone

Who even uses Ozone? [Apache Ozone Best Practices at DiDi (PDF)](https://ozone.apache.org/assets/files/ApacheOzoneBestPracticesAtDidi-10e92be1e017fa7c6c0ba1979732808d.pdf)


## Tech Stack

| Process | Tool |
| ----------- | ----------- |
| Data Orchestration | Dagster |
| Object Storage | Apache Ozone |
| Table Format(s)| Apache Iceberg|
| Query Engine | pySpark |
| Package Manager | UV |
| Data Catalog | Iceberg Hadoop Catalog |


## Data Sources

| Source | Info |
| ----------- | ----------- |
| Open Brewery DB | [Open Brewery DB Docs](https://www.openbrewerydb.org/documentation) |
