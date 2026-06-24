# Take-Home Task: PySpark ETL Pipeline

**Time budget:** 4 hours  
**Deliverable:** A git repository (GitHub/GitLab). Send us the link when done.

---

## Background

Our analytics platform ingests user events from a web application. Events land in S3 as JSON files — one file per hour, 24 files per day, roughly 10 GB total per day in production. The data science team queries this data constantly, but querying raw JSON directly is slow and expensive.

Your job is to build an ETL job that transforms this raw event data into a clean, analytics-ready table.

---

## What You're Given

- A folder of sample event JSON files (representative of what lands in S3 each hour)
- A `schema.json` describing the event structure

In production, input paths follow the pattern:
```
s3://analytics-events/raw/year=2024/month=01/day=15/events_2024-01-15_HH.json
```

---

## What We Need

A PySpark job that reads the event files and produces a table called `fact_user_daily` with one row per user per day, containing:

- `user_id`
- `date`
- `event_count`
- `distinct_event_types`
- `first_event_ts`
- `last_event_ts`
- `total_revenue`

Output should be written as Parquet, partitioned by date. The job should be runnable locally with `spark-submit`.

The exact implementation decisions are yours to make. Use your judgment.

---

## Requirements

- PySpark (Spark 3.x)
- Your README must explain:
  - How to install dependencies and run the job
  - Any assumptions you made and why
  - How you'd run this in production at scale

---

## Submission

Push to a GitHub or GitLab repo and send us the link. Include sample Parquet output generated from the provided files.
