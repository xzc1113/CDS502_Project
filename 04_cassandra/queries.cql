USE cds502_wikirank;

-- Q1 Language overview
SELECT * FROM lang_summary;

-- Q2 Quality distribution for en
SELECT quality_bin, bin_count FROM lang_quality_bin_summary WHERE lang='en';

-- Q3 Top 50 for en
SELECT rank, title, quality FROM lang_topk WHERE lang='en';

-- Q4 Title-length bins for en
SELECT title_len_bin, bin_count, avg_quality, high_quality_ratio
FROM lang_titlelen_bin_summary
WHERE lang='en';
