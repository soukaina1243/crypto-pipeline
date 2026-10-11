# Checkpoint Questions

Answer these (to me, in conversation) before moving to the next day.
No looking up definitions — if you can't answer from what you just built,
that's the signal to review, not to skip ahead.

## Day 1 — Kafka Producer
1. Why do we use `coin` as the Kafka message key instead of not setting a key at all?
2. What happens if the CoinGecko API call fails mid-loop — does your producer crash or recover? Why?
3. In NASA logs, the producer read from a CSV. Here it polls a live API. What's the practical difference in how you'd test each one?

## Day 2 — Spark Streaming
1. Explain watermarking in your own words — what specific problem does it solve?
2. If a price tick arrives 12 minutes late (watermark = 10 min), what happens to it?
3. Why do we use `foreachBatch` instead of a simple `.format("jdbc")` sink directly on the streaming DataFrame?

## Day 3 — Cloud
1. What's the difference between the AWS Free plan and Paid plan you signed up for?
2. Where exactly did you set your budget alert, and what threshold did you choose?
3. If your RDS instance is in a different region than expected, what could that affect (cost, latency)?

## Day 4 — dbt models
1. What's the difference between a staging model and a mart model?
2. Why does dbt use `ref()` and `source()` instead of hardcoding table names?
3. What does `dbt run` actually do under the hood — what SQL does it generate?

## Day 5 — dbt tests
1. What's the difference between a generic test (not_null) and a singular test?
2. If `dbt test` fails in CI, should the deploy proceed anyway? Why or why not?
3. What's one data quality issue from your NASA project that a dbt test would have caught early?

## Day 6 — Airflow
1. Why set `retries=3` with a 5-minute delay instead of retries=0 or retries=100?
2. What's the difference between a task retry and a DAG-level rerun?
3. Your alert callback currently just prints. What would change to wire it to Slack for real?

## Day 7 — CI/CD
1. Why do we spin up a fresh Postgres container in CI instead of pointing CI at your real dev database?
2. What would you add to this workflow to also run before merging a PR (not just after push to main)?
3. What's one thing that could pass locally but fail in CI, and why?

## Day 8 — Dashboard & wrap-up
1. Walk through the full data path from API call to chart, in one breath, no notes.
2. If a recruiter asks "what would break first at 100x scale," what's your honest answer?
3. Which single line item from this project are you most confident explaining in an interview?
