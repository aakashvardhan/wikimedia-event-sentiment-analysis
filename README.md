# Real-time English Sentiment Pipeline on Wikimedia RecentChanges (Kafka + Python)

This pipeline ingests Wikimedia's recentChanges Server-Sent Events and performs real-time English sentiment analysis on edit comments.

## Data Flow and Topics

- Source -> `wm_rc_raw`. Python SSE client reads `recentchange`, filters to `type as {edit, new}`, `namespace in {0, 1}` (articles and talk), `wiki == "enwiki"`, `bot == false`, and requires a valid `rev_id`. The edit summary is normalized (urls/tags/wiki markup removed) and produces JSON records.

- Processing -> `wm_sentiment_scored`. Python consumer reads `wm_rc_raw`, cleans the comment again and then scores sentiment. Using HF English RoBERTa model, each message is enriched with `label in {positive, neutral, negative}` and numeric `polarity in {+1, 0, -1}`, then written to `wc_sentiment_scored` with the same key and schema field.


- The topics are created with 3-6 partitions. Keying by page title preserves the per-page order and enables title aggregations.


## Real World Application

- News & Trend Monitoring: Surface pages tied to breaking events via rising negative/positive polarity over short windows
- Moderator Triage: flag pages with spikes in negative sentiment in edit summaries to prioritize reviews.