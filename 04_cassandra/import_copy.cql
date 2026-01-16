USE cds502_wikirank;

COPY lang_summary FROM '/import/lang_summary.csv' WITH HEADER = true;
COPY lang_quality_bin_summary FROM '/import/lang_quality_bin_summary.csv' WITH HEADER = true;
COPY lang_titlelen_bin_summary FROM '/import/lang_titlelen_bin_summary.csv' WITH HEADER = true;

COPY lang_topk FROM '/import/lang_topk.tsv'
WITH HEADER = true AND DELIMITER = '\t';
