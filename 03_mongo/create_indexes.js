use cds502_wikirank;
db.articles.createIndex({ lang: 1 });
db.articles.createIndex({ lang: 1, quality: -1 });
db.articles.createIndex({ lang: 1, quality_bin: 1 });
db.articles.createIndex({ lang: 1, title_len_bin: 1 });
printjson(db.articles.getIndexes());
