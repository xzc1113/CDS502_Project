use cds502_wikirank;

// Q1 Language overview
print("Q1 Language overview (top 30 by count)");
db.articles.aggregate([
  {$group:{_id:"$lang", article_count:{$sum:1}, avg_quality:{$avg:"$quality"}, high_quality_ratio:{$avg:"$is_high_quality"}}},
  {$sort:{article_count:-1}},
  {$limit:30}
]).toArray().forEach(x => printjson(x));

// Q2 Quality distribution for en
print("Q2 Quality distribution (lang=en)");
db.articles.aggregate([
  {$match:{lang:"en"}},
  {$group:{_id:"$quality_bin", bin_count:{$sum:1}}},
  {$sort:{_id:1}}
]).toArray().forEach(x => printjson(x));

// Q3 Top 50 for en
print("Q3 Top 50 (lang=en)");
db.articles.aggregate([
  {$match:{lang:"en"}},
  {$sort:{quality:-1}},
  {$limit:50},
  {$project:{_id:0, page_id:1, title:1, quality:1}}
]).toArray().forEach(x => printjson(x));

// Q4 Title-length bins for en
print("Q4 Title length bins (lang=en)");
db.articles.aggregate([
  {$match:{lang:"en"}},
  {$group:{_id:"$title_len_bin", bin_count:{$sum:1}, avg_quality:{$avg:"$quality"}, high_quality_ratio:{$avg:"$is_high_quality"}}},
  {$sort:{_id:1}}
]).toArray().forEach(x => printjson(x));
