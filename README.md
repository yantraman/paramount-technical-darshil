## Paramount Technical Assessment

### Architecture

- Maintained the three separate datasets as different tables in postgres
- Used PySpark to ingest the various datasources using a single spark client. Pandas or any other python dataframes framework requires more connection configurations
- Used PostgreSQL over MySQL due to better support for larger datasets, scalable ingestions and (weirdly) better emoji support

### Runbook

In order to properly run this project

1. Install pyspark=3.4.0 using pip or conda
2. Use `docker-compose up` to run the postgres database
3. Run the `main.py` to create a postgres database with `post_meta`, `comment_meta` and `comment_info` tables
4. Run the `queries.py` file to run queries for the second part. the results should be in the terminal output
5. Check the `discussion.md` to find the discussion answers