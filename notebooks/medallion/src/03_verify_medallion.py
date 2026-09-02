# %% [markdown]
# # Verify · proving the estate is real
#
# ![Medallion architecture: bronze, silver, gold]({{IMG:medallion_flow}})
#
# A short, **read-only** pass over all three layers. Run it after 00 / 01 / 01b / 02 as
# the closing act of a demo — it re-derives every claim the other notebooks made, from
# the tables themselves rather than from scrollback.
#
# Four questions, in order:
#
# 1. **What was actually built?** Every table in every layer, with row counts.
# 2. **What did silver catch?** The data-quality ledger and the quarantine reasons.
# 3. **Does gold answer business questions?** Four of them, without touching silver.
# 4. **Is the lineage intact?** One batch id, traced from bronze through to gold.

# %%
# =====================================================================
# MEDALLION / RETAIL  ·  VERIFY
# ---------------------------------------------------------------------
# A short read-only pass over all three layers. Run it after 00/01/02 to
# prove the estate is actually there: what was built, how big it is, what
# the data-quality ledger says, and whether gold answers real questions.
# =====================================================================
from pyspark.sql import SparkSession, functions as F

# ---------------------------------------------------------------------
# DEMO CONSOLE — live, readable progress
# ---------------------------------------------------------------------
# Every helper below flushes. Spark steps here take tens of seconds, and
# without an explicit flush a notebook's output can arrive in one burst at the
# very end — which reads as "nothing is happening" to an audience watching.
# Each step also announces itself BEFORE the work starts, so the pause is
# narrated rather than silent, and reports rows plus throughput when it lands.
import builtins as _bi
import functools as _ft
import time
print = _ft.partial(_bi.print, flush=True)

_W = 78


def fmt(n):
    return "{:,}".format(int(n))


def rule(ch="─"):
    print(ch * _W)


def banner(title, subtitle=""):
    print()
    print("╔" + "═" * (_W - 2) + "╗")
    print("║ " + title.ljust(_W - 3) + "║")
    if subtitle:
        print("║ " + subtitle.ljust(_W - 3) + "║")
    print("╚" + "═" * (_W - 2) + "╝")


def section(num, title):
    print()
    print("▸ %s  %s" % (num, title))
    rule()


def working(msg):
    print("   … %s" % msg)


def ok(name, rows, secs, note=""):
    rate = "%12s rows/s" % fmt(rows / secs) if secs > 0.05 else " " * 18
    print("   ✓ %-28s %14s rows %7.1fs %s%s" % (name, fmt(rows), secs, rate, note))


def skipped(name, rows, what="already built"):
    print("   ~ %-28s %14s rows       —   %s" % (name, fmt(rows), what))


def metric(table, rule_name, value):
    print("       · %-20s %-30s %12s" % (table, rule_name, fmt(value)))


def finished(t0, what):
    print()
    rule("═")
    print("  %s COMPLETE in %.1fs" % (what.upper(), time.time() - t0))
    rule("═")

BRONZE_DB, SILVER_DB, GOLD_DB = "bronze_retail", "silver_retail", "gold_retail"
spark = SparkSession.builder.appName("medallion-verify").getOrCreate()

def layer(db):
    try:
        tabs = [r.tableName for r in spark.sql(f"SHOW TABLES IN {db}").collect()
                if not r.tableName.startswith("_stg")]
    except Exception as e:
        print(f"  {db}: NOT PRESENT ({type(e).__name__})")
        return 0, 0
    total = 0
    print(f"\n  {db}  —  {len(tabs)} tables")
    print("  " + "-" * 58)
    for t in sorted(tabs):
        n = spark.table(f"{db}.{t}").count()
        total += n
        print("    %-34s %16s rows" % (t, "{:,}".format(n)))
    print("  " + "-" * 58)
    print("    %-34s %16s rows" % ("TOTAL", "{:,}".format(total)))
    return len(tabs), total

T_RUN = time.time()
banner("VERIFY  ·  THE MEDALLION ESTATE",
       "what was built, what it proves, and that it hangs together")
section("3.1", "TABLE AND ROW CENSUS BY LAYER")
grand_t = grand_r = 0
for db in (BRONZE_DB, SILVER_DB, GOLD_DB):
    t, r = layer(db)
    grand_t += t
    grand_r += r
print("\n  %d tables, %s rows across all three layers" % (grand_t, "{:,}".format(grand_r)))

# %% [markdown]
# ### The data-quality ledger — what silver caught and fixed
#
# `dq_metrics` read back as a table, plus the quarantined sales lines grouped by reject
# reason. This is the answer to *"how do you know the data is good?"* — the defects
# bronze landed with, and what happened to each of them.

# %%
# ---------------------------------------------------------------------
# The data-quality ledger: what silver actually caught and fixed.
# ---------------------------------------------------------------------
print("\nSILVER DATA-QUALITY LEDGER")
spark.sql(f"""SELECT table_name, rule, scope, metric_value
              FROM {SILVER_DB}.dq_metrics
              ORDER BY table_name, scope, rule""").show(200, False)

print("QUARANTINED SALES LINES BY REASON")
spark.sql(f"""SELECT reject_reason, count(*) AS lines
              FROM {SILVER_DB}.dq_rejects_sales
              GROUP BY reject_reason ORDER BY lines DESC""").show(truncate=False)

# %% [markdown]
# ### Four business questions, answered from gold alone
#
# Each query below touches **only** `gold_retail`. No joins back to silver, no cleaning,
# no window functions in the query itself — the modelling work was done upstream, and
# that is exactly the property the gold layer is supposed to have.
#
# > **A note on the numbers:** margins read negative and 2003 is a partial year. Both are
# > characteristics of the TPC-DS generator's synthetic price/cost distribution, not
# > pipeline defects — the arithmetic is correct on the data as generated.

# %%
# ---------------------------------------------------------------------
# Does gold answer business questions without touching silver? Yes.
# ---------------------------------------------------------------------
print("\nQ1. Which channel grew year on year?")
spark.sql(f"""SELECT channel, sold_year,
                     round(sum(net_revenue)/1e6, 1)   AS net_revenue_musd,
                     round(avg(margin_pct), 2)        AS avg_margin_pct,
                     round(avg(yoy_growth_pct), 1)    AS avg_yoy_growth_pct
              FROM {GOLD_DB}.agg_sales_by_channel_month
              GROUP BY channel, sold_year
              ORDER BY channel, sold_year""").show(40, False)

print("Q2. Top 10 customers by lifetime value, and their segment")
spark.sql(f"""SELECT full_name, customer_segment, order_count,
                     round(lifetime_revenue, 0) AS lifetime_revenue
              FROM {GOLD_DB}.dim_customer_rfm
              ORDER BY lifetime_revenue DESC LIMIT 10""").show(truncate=False)

print("Q3. Which promotions destroyed value?")
spark.sql(f"""SELECT promo_name, promoted_units,
                     round(revenue_lift_pct, 1) AS lift_pct, roi_pct, verdict
              FROM {GOLD_DB}.agg_promotion_effectiveness
              WHERE verdict <> 'INSUFFICIENT_DATA'
              ORDER BY roi_pct LIMIT 10""").show(truncate=False)

print("Q4. What should we reorder right now?")
spark.sql(f"""SELECT category, count(*) AS item_weeks,
                     round(avg(weeks_of_cover), 1) AS avg_cover
              FROM {GOLD_DB}.agg_inventory_health_weekly
              WHERE stock_status = 'REORDER_NOW'
              GROUP BY category ORDER BY item_weeks DESC LIMIT 10""").show(truncate=False)

# %% [markdown]
# ### Storage locations and the lineage stamp
#
# Where each layer's bytes physically live in Hive, and the payoff check: the **same
# batch id** present in bronze, silver and gold. If that matches, a row in a gold mart is
# traceable all the way back to the source extract it came from.

# %%
# ---------------------------------------------------------------------
# Where the bytes actually live, and the lineage stamp that ties a row in
# gold back to the bronze batch it came from.
# ---------------------------------------------------------------------
for db, t in ((BRONZE_DB, "store_sales"), (SILVER_DB, "fct_sales"), (GOLD_DB, "kpi_daily_executive")):
    loc = [r for r in spark.sql(f"DESCRIBE FORMATTED {db}.{t}").collect()
           if r.col_name.strip() == "Location"]
    print("  %-22s %s" % (f"{db}.{t}", loc[0].data_type if loc else "?"))

print("\nLINEAGE — one batch id threaded through all three layers")
b = spark.table(f"{BRONZE_DB}.store_sales").select("_batch_id").limit(1).collect()[0][0]
s = spark.table(f"{SILVER_DB}.fct_sales").select("_silver_batch_id").limit(1).collect()[0][0]
g = spark.table(f"{GOLD_DB}.kpi_daily_executive").select("_gold_batch_id").limit(1).collect()[0][0]
print("  bronze _batch_id        :", b)
print("  silver _silver_batch_id :", s)
print("  gold   _gold_batch_id   :", g)
print("  consistent              :", b == s == g)
finished(T_RUN, "VERIFICATION")
