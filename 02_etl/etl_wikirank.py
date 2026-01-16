import os, math, time
import pandas as pd

INPUT_CSV = "/home/ubuntu/cds502/01_raw/20240801-wikirank.csv"
OUT_DIR   = "/home/ubuntu/cds502/05_outputs"

# 固定规则（写进 report）
HQ_THRESHOLD = 80
QUALITY_BIN_SIZE = 10
TITLELEN_BIN_SIZE = 20
TOPK = 50
CHUNKSIZE = 200_000

# 真实列名（你已确认）
LANG_COL  = "Language"
TITLE_COL = "Title"
ID_COL    = "Page_ID"
QUAL_COL  = "WikiRank_score"

os.makedirs(OUT_DIR, exist_ok=True)

def qbin(x):
    if pd.isna(x): return -1
    try: v=float(x)
    except: return -1
    b=int(math.floor(v/QUALITY_BIN_SIZE)*QUALITY_BIN_SIZE)
    return max(0, min(100, b))

def tbin(x):
    if pd.isna(x): return -1
    try: v=int(x)
    except: return -1
    b=int(math.floor(v/TITLELEN_BIN_SIZE)*TITLELEN_BIN_SIZE)
    return max(0, b)

def clean_title(s):
    s = "" if pd.isna(s) else str(s)
    return s.replace("\t"," ").replace("\r"," ").replace("\n"," ").strip()

# header check
sample = pd.read_csv(INPUT_CSV, nrows=5)
cols = list(sample.columns)
missing = [c for c in [LANG_COL, TITLE_COL, ID_COL, QUAL_COL] if c not in cols]
print("Detected columns:", cols, flush=True)
if missing:
    print("ERROR missing columns:", missing, flush=True)
    raise SystemExit(2)

print("Column mapping fixed:", flush=True)
print(" lang  <-", LANG_COL, flush=True)
print(" title <-", TITLE_COL, flush=True)
print(" page_id <-", ID_COL, flush=True)
print(" quality <-", QUAL_COL, flush=True)

# clean old outputs
mongo_out = os.path.join(OUT_DIR, "articles_clean.jsonl")
for f in [
    mongo_out,
    os.path.join(OUT_DIR, "lang_summary.csv"),
    os.path.join(OUT_DIR, "lang_quality_bin_summary.csv"),
    os.path.join(OUT_DIR, "lang_titlelen_bin_summary.csv"),
    os.path.join(OUT_DIR, "lang_topk.tsv"),
]:
    if os.path.exists(f):
        os.remove(f)

# aggregators
lang_count, lang_qsum, lang_hqsum = {}, {}, {}
lang_qbin_count = {}
lang_tbin_count, lang_tbin_qsum, lang_tbin_hqsum = {}, {}, {}
topk = {}

def push_topk(lang, q, pid, title):
    if pd.isna(q): return
    if pd.isna(pid): return
    try:
        pid_i = int(pid)
    except:
        return
    lst = topk.setdefault(lang, [])
    lst.append((float(q), pid_i, title))
    lst.sort(key=lambda x:(-x[0], x[1]))
    if len(lst) > TOPK:
        del lst[TOPK:]

usecols = [LANG_COL, TITLE_COL, ID_COL, QUAL_COL]

t0 = time.time()
rows_total = 0
chunk_idx = 0

for chunk in pd.read_csv(INPUT_CSV, usecols=usecols, chunksize=CHUNKSIZE):
    chunk_idx += 1
    rows_total += len(chunk)

    chunk = chunk.rename(columns={
        LANG_COL:"lang",
        TITLE_COL:"title",
        ID_COL:"page_id",
        QUAL_COL:"quality"
    })

    chunk["title"] = chunk["title"].apply(clean_title)
    chunk["title_len"] = chunk["title"].str.len()

    chunk["page_id"] = pd.to_numeric(chunk["page_id"], errors="coerce")
    chunk["quality"] = pd.to_numeric(chunk["quality"], errors="coerce")

    chunk["quality_bin"] = chunk["quality"].apply(qbin)
    chunk["is_high_quality"] = (chunk["quality"] >= HQ_THRESHOLD).astype(int)
    chunk["title_len_bin"] = chunk["title_len"].apply(tbin)

    # write Mongo JSONL
    chunk.to_json(mongo_out, orient="records", lines=True, mode="a", force_ascii=False)

    # Q1 + TopK per lang
    for lang, g in chunk.groupby("lang"):
        cnt = int(len(g))
        qsum = float(g["quality"].sum(skipna=True))
        hsum = int(g["is_high_quality"].sum())

        lang_count[lang] = lang_count.get(lang, 0) + cnt
        lang_qsum[lang] = lang_qsum.get(lang, 0.0) + qsum
        lang_hqsum[lang] = lang_hqsum.get(lang, 0) + hsum

        for _, r in g[["quality","page_id","title"]].iterrows():
            push_topk(lang, r["quality"], r["page_id"], r["title"])

    # Q2 bins
    for (lang, qb), g in chunk.groupby(["lang","quality_bin"]):
        key = (lang, int(qb))
        lang_qbin_count[key] = lang_qbin_count.get(key, 0) + int(len(g))

    # Q4 title_len bins
    for (lang, tb), g in chunk.groupby(["lang","title_len_bin"]):
        key = (lang, int(tb))
        c = int(len(g))
        qs = float(g["quality"].sum(skipna=True))
        hs = int(g["is_high_quality"].sum())

        lang_tbin_count[key] = lang_tbin_count.get(key, 0) + c
        lang_tbin_qsum[key] = lang_tbin_qsum.get(key, 0.0) + qs
        lang_tbin_hqsum[key] = lang_tbin_hqsum.get(key, 0) + hs

    # progress
    if chunk_idx == 1 or chunk_idx % 10 == 0:
        elapsed = int(time.time() - t0)
        jsonl_bytes = os.path.getsize(mongo_out) if os.path.exists(mongo_out) else 0
        print(f"[progress] chunk={chunk_idx} rows={rows_total} elapsed_s={elapsed} jsonl_mb={jsonl_bytes/1024/1024:.1f}", flush=True)

# write Q1 summary
rows=[]
for lang in sorted(lang_count.keys()):
    cnt=lang_count[lang]
    avgq=lang_qsum[lang]/cnt if cnt else 0.0
    hqr=lang_hqsum[lang]/cnt if cnt else 0.0
    rows.append([lang,cnt,round(avgq,6),round(hqr,6)])
pd.DataFrame(rows, columns=["lang","article_count","avg_quality","high_quality_ratio"]) \
  .to_csv(os.path.join(OUT_DIR,"lang_summary.csv"), index=False)

# write Q2 summary
rows=[]
for (lang,qb),cnt in lang_qbin_count.items():
    rows.append([lang,qb,cnt])
pd.DataFrame(rows, columns=["lang","quality_bin","bin_count"]) \
  .to_csv(os.path.join(OUT_DIR,"lang_quality_bin_summary.csv"), index=False)

# write Q4 summary
rows=[]
for (lang,tb),cnt in lang_tbin_count.items():
    avgq=lang_tbin_qsum[(lang,tb)]/cnt if cnt else 0.0
    hqr=lang_tbin_hqsum[(lang,tb)]/cnt if cnt else 0.0
    rows.append([lang,tb,cnt,round(avgq,6),round(hqr,6)])
pd.DataFrame(rows, columns=["lang","title_len_bin","bin_count","avg_quality","high_quality_ratio"]) \
  .to_csv(os.path.join(OUT_DIR,"lang_titlelen_bin_summary.csv"), index=False)

# write Q3 topk as TSV (safer for Cassandra COPY)
rows=[]
for lang,lst in topk.items():
    for rank,(q,pid,title) in enumerate(lst, start=1):
        rows.append([lang,rank,q,pid,title])
pd.DataFrame(rows, columns=["lang","rank","quality","page_id","title"]) \
  .to_csv(os.path.join(OUT_DIR,"lang_topk.tsv"), index=False, sep="\t")

elapsed = int(time.time() - t0)
print(f"ETL done. elapsed_s={elapsed} output_dir={OUT_DIR}", flush=True)
