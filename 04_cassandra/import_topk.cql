USE cds502_wikirank;
COPY lang_topk (lang, rank, quality, page_id, title) FROM '/import/lang_topk.tsv' WITH HEADER = true AND DELIMITER = '	';
