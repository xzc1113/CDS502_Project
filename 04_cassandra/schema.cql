CREATE KEYSPACE IF NOT EXISTS cds502_wikirank
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE cds502_wikirank;

CREATE TABLE IF NOT EXISTS lang_summary (
  lang text PRIMARY KEY,
  article_count bigint,
  avg_quality double,
  high_quality_ratio double
);

CREATE TABLE IF NOT EXISTS lang_quality_bin_summary (
  lang text,
  quality_bin int,
  bin_count bigint,
  PRIMARY KEY ((lang), quality_bin)
) WITH CLUSTERING ORDER BY (quality_bin ASC);

CREATE TABLE IF NOT EXISTS lang_topk (
  lang text,
  rank int,
  quality double,
  page_id bigint,
  title text,
  PRIMARY KEY ((lang), rank)
) WITH CLUSTERING ORDER BY (rank ASC);

CREATE TABLE IF NOT EXISTS lang_titlelen_bin_summary (
  lang text,
  title_len_bin int,
  bin_count bigint,
  avg_quality double,
  high_quality_ratio double,
  PRIMARY KEY ((lang), title_len_bin)
) WITH CLUSTERING ORDER BY (title_len_bin ASC);
