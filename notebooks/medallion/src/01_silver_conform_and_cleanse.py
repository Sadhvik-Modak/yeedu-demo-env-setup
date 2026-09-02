# %% [markdown]
# # Step 1 · Bronze ➜ **Silver** — conform and cleanse
#
# ![Medallion architecture: bronze, silver, gold]({{IMG:medallion_flow}})
#
# Silver is the **trusted, modelled layer**. Bronze answered *"what arrived?"*; silver
# answers *"what is true?"*. Every table below is deliberate, and every transformation
# is one a real pipeline has to do:
#
# | what silver does | why it matters |
# |---|---|
# | **Conforms** three channels onto one schema | store, catalog and web name the same column three different ways |
# | **Cleanses** — trims, upper-cases, normalises countries | `USA` / `United States` / `us` are one country |
# | **Deduplicates** CDC replays | the same order line arrived twice; last write wins |
# | **Quarantines** rather than deletes | a rejected row goes to `dq_rejects_sales` with a reason, not to `/dev/null` |
# | **Records** a data-quality ledger | `dq_metrics` is the audit evidence that any of this happened |
#
# ### The star schema this builds
#
# ![Silver star schema around fct_sales]({{IMG:silver_star}})
#
# The dimensions are built **whole** — all 500,000 customers, all 102,000 products. Only
# the sales facts are sliced for the demo (see the note in the next cell), so every join
# and every business question is still answered against complete reference data.

# %%
# =====================================================================
# MEDALLION / RETAIL  ·  STEP 1 — BRONZE  ➜  SILVER
# ---------------------------------------------------------------------
# Silver is the trusted, modelled layer. Everything here is deliberate:
#
#   * TYPE      — explicit decimals/dates/ints, never inferred
#   * DEDUP     — CDC replays and MDM re-sends collapsed on natural keys
#   * CONFORM   — three sales channels (store / catalog / web) unified into
#                 ONE fact grain; three return feeds into another
#   * STANDARD  — trimmed names, upper-cased states, one spelling per country
#   * ENRICH    — customer + demographics + household + income band -> one
#                 dimension; business-readable column names throughout
#   * QUARANTINE— rows that fail the contract go to a reject table, they do
#                 not silently vanish and they do not pollute the facts
#   * MEASURE   — every rule writes a row into silver_retail.dq_metrics
#
# Produces 20 tables: 15 conformed dimensions, 3 facts, 2 quality tables.
# =====================================================================
import time
from pyspark.sql import SparkSession, functions as F, Window as W
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType

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

BRONZE_DB = "bronze_retail"
SILVER_DB = "silver_retail"

spark = SparkSession.builder.appName("medallion-silver").getOrCreate()
# 8 cores, and at demo scale every shuffle is a few hundred thousand rows. The
# default 200 partitions turns a 2-second write into 20 seconds of pure task
# scheduling; 16 keeps every core busy with none of that overhead.
spark.conf.set("spark.sql.shuffle.partitions", "16")
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")

# ---------------------------------------------------------------------
# DEMO SCALE
# ---------------------------------------------------------------------
# Bronze is landed at the full TPC-DS SF10 — 24 tables, ~192M rows — because a
# raw layer should look like a real raw layer. Silver deliberately processes
# only a slice of the *facts*: this cluster is 8 cores sharing a 2.85 GB heap
# with no local disk, so a full-volume shuffle spills to network storage and
# does not finish. Dimensions are always processed whole, so every join, every
# mart and every business question below is still answered against complete
# reference data.
#
# The slice is taken on a hash of the natural key, not with .sample(). That
# matters: sampling rows at random would split a CDC replay from its original
# and quietly destroy the duplicate-detection story this layer exists to show.
# Hashing the key keeps every replay of a kept order together.
#
# Set to 100 for the full dataset on a cluster that can take it.
KEEP_PCT = 5

def demo_slice(df, *key_cols):
    """Keep a deterministic ~KEEP_PCT% of natural keys, replays included."""
    if KEEP_PCT >= 100:
        return df
    return df.filter(F.abs(F.hash(*[F.col(c) for c in key_cols])) % 100 < KEEP_PCT)
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", str(64 * 1024 * 1024))
# A Parquet writer buffers a whole row group in the heap. The default 128 MB,
# times one writer per concurrent task, does not fit a 2.85 GB driver, so
# trade a little compression for headroom.
spark.sparkContext._jsc.hadoopConfiguration().setInt("parquet.block.size", 32 * 1024 * 1024)

spark.sql(f"CREATE DATABASE IF NOT EXISTS {SILVER_DB} "
          f"COMMENT 'Conformed, cleansed, deduplicated retail model — the trusted layer'")

BATCH_ID = (spark.table(f"{BRONZE_DB}.store_sales")
                 .select("_batch_id").limit(1).collect()[0]["_batch_id"])
T_RUN = time.time()
banner("SILVER  ·  CONFORM & CLEANSE",
       "typed, deduplicated, quarantined — the trusted layer")
print("   silver database   : %s" % SILVER_DB)
print("   processing batch  : %s" % BATCH_ID)
print("   shuffle partitions: %s" % spark.conf.get("spark.sql.shuffle.partitions"))
rule()

DQ = []            # (table, rule, scope, value) — flushed to dq_metrics at the end
STAGE_TIMES = {}


def dq(table, rule, scope, value):
    DQ.append((table, rule, scope, int(value)))
    metric(table, rule, value)


def bronze(name):
    """Read a bronze table and drop the landing-zone audit columns."""
    df = spark.table(f"{BRONZE_DB}.{name}")
    return df.drop("_source_system", "_source_file", "_batch_id", "_ingest_ts")


def publish(df, name, partition_by=None, files=None, comment=""):
    t0 = time.time()
    if already_done(name):
        n = spark.table(f"{SILVER_DB}.{name}").count()
        skipped(name, n, "already written for this batch")
        return n
    working("building %s …" % name)
    if files:
        df = df.repartition(files)
    if partition_by:
        # Sorted input means each task fills one Hive partition at a time and
        # keeps a single Parquet writer open instead of one per year.
        df = df.sortWithinPartitions(partition_by)
    writer = (df.withColumn("_silver_batch_id", F.lit(BATCH_ID))
                .withColumn("_silver_ts", F.current_timestamp())
                .write.mode("overwrite").format("parquet").option("compression", "snappy"))
    if partition_by:
        writer = writer.partitionBy(partition_by)
    writer.saveAsTable(f"{SILVER_DB}.{name}")
    if comment:
        spark.sql(f"ALTER TABLE {SILVER_DB}.{name} SET TBLPROPERTIES ('comment' = '{comment}')")
    n = spark.table(f"{SILVER_DB}.{name}").count()
    STAGE_TIMES[name] = time.time() - t0
    ok(name, n, STAGE_TIMES[name])
    return n


# Runs are capped at roughly 22 minutes on this platform, so every step is
# idempotent: a table already written for THIS bronze batch is left alone.
# Re-running a notebook therefore resumes instead of starting over.
# Set False to force a clean rebuild — needed after changing KEEP_PCT, since the
# staged tables from a previous scale would otherwise be skipped and silently
# reused. A full rebuild at demo scale takes about two minutes.
SKIP_EXISTING = True


def already_done(name, stamped=True):
    if not SKIP_EXISTING:
        return False
    try:
        t = spark.table(f"{SILVER_DB}.{name}")
        if stamped and "_silver_batch_id" in t.columns:
            got = t.select("_silver_batch_id").limit(1).collect()
            return bool(got) and got[0][0] == BATCH_ID
        return bool(t.limit(1).collect())
    except Exception:
        return False


def stage(df, name):
    """Materialise an intermediate result to disk. On a small driver heap this
    beats .cache() — the heap cannot hold tens of millions of rows, and a
    spilled cache is slower to re-read than plain Parquet."""
    if already_done(name, stamped=False):
        print("   ~ staged %-26s              —   already present" % name)
        return
    t0 = time.time()
    working("staging %s to disk …" % name)
    df.write.mode("overwrite").format("parquet").saveAsTable(f"{SILVER_DB}.{name}")
    print("   · staged %-26s %26.1fs" % (name, time.time() - t0))


def dedup(df, keys, order_col=None):
    """Keep one row per natural key — last one wins, as a CDC merge would.

    A row_number() window, deliberately. The obvious alternative — max() over a
    struct of every column — was tried and is far worse here: the natural key is
    close to unique, so the aggregate builds one wide buffer per key for tens of
    millions of keys and collapses into a spilling sort-aggregate. The window
    only has to sort inside each shuffle partition, which is bounded."""
    order = F.col(order_col).desc() if order_col else F.lit(1)
    w = W.partitionBy(*[F.col(k) for k in keys]).orderBy(order)
    return (df.withColumn("_rn", F.row_number().over(w))
              .filter(F.col("_rn") == 1).drop("_rn"))

print("helpers ready")

# %% [markdown]
# ### 1.1 · Calendar and clock
#
# `dim_date` and `dim_time` first, because everything else joins to them. These come
# through nearly unchanged — a conformed date dimension is mostly about *typing* the
# columns correctly and giving them names a business user recognises.

# %%
# ---------------------------------------------------------------------
# 1.1  CALENDAR & CLOCK — dim_date, dim_time
#      Every other silver table hangs off these, so they go first.
# ---------------------------------------------------------------------
section("1.1", "CALENDAR AND CLOCK")
d = bronze("date_dim")
dim_date = (d
    .select(
        F.col("d_date_sk").cast("int").alias("date_sk"),
        F.col("d_date_id").alias("date_id"),
        F.col("d_date").cast("date").alias("calendar_date"),
        F.col("d_year").cast("int").alias("calendar_year"),
        F.col("d_moy").cast("int").alias("month_of_year"),
        F.col("d_dom").cast("int").alias("day_of_month"),
        F.col("d_dow").cast("int").alias("day_of_week"),
        F.col("d_qoy").cast("int").alias("quarter_of_year"),
        F.col("d_day_name").alias("day_name"),
        F.col("d_quarter_name").alias("quarter_name"),
        F.col("d_month_seq").cast("int").alias("month_seq"),
        F.col("d_week_seq").cast("int").alias("week_seq"),
        F.col("d_fy_year").cast("int").alias("fiscal_year"),
        (F.col("d_holiday") == "Y").alias("is_holiday"),
        (F.col("d_weekend") == "Y").alias("is_weekend"))
    .withColumn("year_month", F.date_format("calendar_date", "yyyy-MM"))
    .withColumn("month_start", F.trunc("calendar_date", "month"))
    .withColumn("is_business_day", ~F.col("is_weekend") & ~F.col("is_holiday"))
    .filter(F.col("date_sk").isNotNull()))
publish(dedup(dim_date, ["date_sk"]), "dim_date", files=1,
        comment="Conformed calendar with fiscal and business-day attributes")

dim_time = (bronze("time_dim").select(
        F.col("t_time_sk").cast("int").alias("time_sk"),
        F.col("t_hour").cast("int").alias("hour_of_day"),
        F.col("t_minute").cast("int").alias("minute_of_hour"),
        F.col("t_am_pm").alias("am_pm"),
        F.col("t_shift").alias("shift_name"),
        F.col("t_meal_time").alias("meal_time"))
    .withColumn("day_part",
        F.when(F.col("hour_of_day").between(5, 11), "MORNING")
         .when(F.col("hour_of_day").between(12, 16), "AFTERNOON")
         .when(F.col("hour_of_day").between(17, 21), "EVENING")
         .otherwise("OVERNIGHT")))
publish(dedup(dim_time, ["time_sk"]), "dim_time", files=1,
        comment="Time-of-day dimension with retail day-parts")

# %% [markdown]
# ### 1.2 · Address — the first real cleansing job
#
# The first table where bronze is visibly dirty. Watch the `dq ·` lines in the output:
# **5,005 duplicate keys, 61,840 lower-case state codes, 12,590 null ZIPs and four
# different spellings of the same two countries** go in — and two clean country
# spellings come out.

# %%
# ---------------------------------------------------------------------
# 1.2  ADDRESS — the first real cleansing job
#      Bronze holds 'ca', 'CA', 'United States', 'USA', 'us', null zips
#      and duplicate address rows from the MDM re-send. Fix all of it,
#      and count what we fixed.
# ---------------------------------------------------------------------
section("1.2", "ADDRESS — THE FIRST REAL CLEANSING JOB")
a = bronze("customer_address")
dq("dim_address", "bronze rows", "in", a.count())
dq("dim_address", "duplicate address keys", "in",
   a.groupBy("ca_address_sk").count().filter("count > 1").count())
dq("dim_address", "lower-case state codes", "in",
   a.filter(F.col("ca_state") != F.upper(F.col("ca_state"))).count())
dq("dim_address", "null zip", "in", a.filter(F.col("ca_zip").isNull()).count())
dq("dim_address", "distinct country spellings", "in",
   a.select("ca_country").distinct().count())

dim_address = (dedup(a, ["ca_address_sk"], "ca_address_id")
    .select(
        F.col("ca_address_sk").cast("int").alias("address_sk"),
        F.col("ca_address_id").alias("address_id"),
        F.trim(F.concat_ws(" ", F.col("ca_street_number"), F.col("ca_street_name"),
                           F.col("ca_street_type"), F.col("ca_suite_number"))).alias("street_address"),
        F.initcap(F.trim(F.col("ca_city"))).alias("city"),
        F.initcap(F.trim(F.col("ca_county"))).alias("county"),
        # standardise: state codes are always upper case, two characters
        F.upper(F.trim(F.col("ca_state"))).alias("state_code"),
        # standardise: one spelling of the country, unknown flagged not blanked
        F.when(F.upper(F.trim(F.col("ca_country"))).isin("USA", "US", "UNITED STATES"),
               F.lit("United States"))
         .when(F.col("ca_country").isNull(), F.lit("Unknown"))
         .otherwise(F.initcap(F.trim(F.col("ca_country")))).alias("country"),
        F.lpad(F.coalesce(F.trim(F.col("ca_zip")), F.lit("")), 5, "0").alias("postal_code"),
        F.col("ca_gmt_offset").cast("decimal(5,2)").alias("gmt_offset"),
        F.upper(F.trim(F.col("ca_location_type"))).alias("location_type"))
    .withColumn("is_zip_missing", F.col("postal_code") == "00000")
    .withColumn("region",
        F.when(F.col("state_code").isin("CT","ME","MA","NH","RI","VT","NJ","NY","PA"), "NORTHEAST")
         .when(F.col("state_code").isin("IL","IN","MI","OH","WI","IA","KS","MN","MO","NE","ND","SD"), "MIDWEST")
         .when(F.col("state_code").isin("DE","FL","GA","MD","NC","SC","VA","DC","WV","AL","KY","MS","TN","AR","LA","OK","TX"), "SOUTH")
         .when(F.col("state_code").isin("AZ","CO","ID","MT","NV","NM","UT","WY","AK","CA","HI","OR","WA"), "WEST")
         .otherwise("UNKNOWN")))
n = publish(dim_address, "dim_address", files=1,
            comment="Standardised postal address with US census region")
dq("dim_address", "rows published", "out", n)
dq("dim_address", "country spellings after", "out",
   spark.table(f"{SILVER_DB}.dim_address").select("country").distinct().count())

# %% [markdown]
# ### 1.3 · Customer — the flagship conforming job
#
# Customer is where a lakehouse earns its keep. Three problems at once: duplicate keys
# from a CDC replay, untrimmed and inconsistently-cased names, and demographics that
# live in two *separate* source tables and have to be joined back on.
#
# The result is one row per customer with a validated email flag — the record every
# downstream team was previously rebuilding by hand.

# %%
# ---------------------------------------------------------------------
# 1.3  CUSTOMER — the flagship conforming job
#      Four bronze tables (customer, customer_demographics,
#      household_demographics, income_band) plus the standardised address
#      collapse into one dimension a business user can actually query.
# ---------------------------------------------------------------------
section("1.3", "CUSTOMER — THE FLAGSHIP CONFORMING JOB")
c   = bronze("customer")
cd  = bronze("customer_demographics")
hd  = bronze("household_demographics")
ib  = bronze("income_band")
addr = spark.table(f"{SILVER_DB}.dim_address")

dq("dim_customer", "bronze rows", "in", c.count())
dq("dim_customer", "duplicate customer keys", "in",
   c.groupBy("c_customer_sk").count().filter("count > 1").count())
dq("dim_customer", "missing email", "in", c.filter(F.col("c_email_address").isNull()).count())
dq("dim_customer", "untrimmed first name", "in",
   c.filter(F.col("c_first_name") != F.trim(F.col("c_first_name"))).count())

c1 = dedup(c, ["c_customer_sk"], "c_customer_id")

dim_customer = (c1.alias("c")
    .join(cd.alias("cd"), F.col("c.c_current_cdemo_sk") == F.col("cd.cd_demo_sk"), "left")
    .join(hd.alias("hd"), F.col("c.c_current_hdemo_sk") == F.col("hd.hd_demo_sk"), "left")
    .join(ib.alias("ib"), F.col("hd.hd_income_band_sk") == F.col("ib.ib_income_band_sk"), "left")
    .join(addr.alias("a"), F.col("c.c_current_addr_sk") == F.col("a.address_sk"), "left")
    .select(
        F.col("c.c_customer_sk").cast("int").alias("customer_sk"),
        F.col("c.c_customer_id").alias("customer_id"),
        # name hygiene: trim, then proper-case whatever the CRM shouted or whispered
        F.initcap(F.trim(F.col("c.c_first_name"))).alias("first_name"),
        F.initcap(F.trim(F.col("c.c_last_name"))).alias("last_name"),
        F.trim(F.col("c.c_salutation")).alias("salutation"),
        F.lower(F.trim(F.col("c.c_email_address"))).alias("email_address"),
        (F.col("c.c_preferred_cust_flag") == "Y").alias("is_preferred"),
        F.col("c.c_birth_year").cast("int").alias("birth_year"),
        F.col("c.c_birth_country").alias("birth_country"),
        F.col("c.c_first_sales_date_sk").cast("int").alias("first_sales_date_sk"),
        F.col("c.c_current_addr_sk").cast("int").alias("address_sk"),
        F.col("cd.cd_gender").alias("gender"),
        F.col("cd.cd_marital_status").alias("marital_status"),
        F.col("cd.cd_education_status").alias("education_status"),
        F.col("cd.cd_credit_rating").alias("credit_rating"),
        F.col("cd.cd_purchase_estimate").cast("int").alias("purchase_estimate"),
        F.col("cd.cd_dep_count").cast("int").alias("dependent_count"),
        F.col("hd.hd_buy_potential").alias("buy_potential"),
        F.col("hd.hd_vehicle_count").cast("int").alias("vehicle_count"),
        F.col("ib.ib_lower_bound").cast("int").alias("income_lower_bound"),
        F.col("ib.ib_upper_bound").cast("int").alias("income_upper_bound"),
        F.col("a.city").alias("city"), F.col("a.state_code").alias("state_code"),
        F.col("a.postal_code").alias("postal_code"), F.col("a.country").alias("country"),
        F.col("a.region").alias("region"))
    # derived business attributes — the reason a dimension exists
    .withColumn("full_name", F.concat_ws(" ", F.col("first_name"), F.col("last_name")))
    .withColumn("has_valid_email",
                F.col("email_address").isNotNull() & F.col("email_address").contains("@"))
    .withColumn("age_years",
                F.when(F.col("birth_year").isNotNull(), F.lit(2003) - F.col("birth_year")))
    .withColumn("age_band",
        F.when(F.col("age_years") < 25, "18-24")
         .when(F.col("age_years") < 35, "25-34")
         .when(F.col("age_years") < 45, "35-44")
         .when(F.col("age_years") < 55, "45-54")
         .when(F.col("age_years") < 65, "55-64")
         .when(F.col("age_years") >= 65, "65+")
         .otherwise("UNKNOWN"))
    .withColumn("income_band_label",
        F.when(F.col("income_lower_bound").isNull(), "UNKNOWN")
         .otherwise(F.concat(F.lit("$"), (F.col("income_lower_bound") / 1000).cast("int"),
                             F.lit("k-$"), (F.col("income_upper_bound") / 1000).cast("int"), F.lit("k")))))
n = publish(dim_customer, "dim_customer", files=2,
            comment="Customer 360 dimension: CRM + demographics + household + income + address")
dq("dim_customer", "rows published", "out", n)
dq("dim_customer", "valid email", "out",
   spark.table(f"{SILVER_DB}.dim_customer").filter("has_valid_email").count())
dq("dim_customer", "demographics matched", "out",
   spark.table(f"{SILVER_DB}.dim_customer").filter("gender is not null").count())

# %% [markdown]
# ### 1.4 · Product, with SCD-2 history
#
# `dim_item` is a **slowly changing dimension, type 2**: the source carries
# `rec_start_date` / `rec_end_date`, so a product that changed price or category has
# *multiple* rows, only one of which is current.
#
# That is why 102,000 rows contain only **51,000 current products**. Getting this wrong
# is the classic lakehouse bug — join to a SCD-2 dimension without filtering
# `is_current`, and every sale is counted once per historical version of its product.

# %%
# ---------------------------------------------------------------------
# 1.4  PRODUCT — dim_item, with the SCD-2 history the source already carries
# ---------------------------------------------------------------------
section("1.4", "PRODUCT, WITH SCD-2 HISTORY")
i = bronze("item")
dq("dim_item", "bronze rows", "in", i.count())
dq("dim_item", "duplicate item keys", "in",
   i.groupBy("i_item_sk").count().filter("count > 1").count())
dq("dim_item", "missing price", "in", i.filter(F.col("i_current_price").isNull()).count())
dq("dim_item", "untrimmed category", "in",
   i.filter(F.col("i_category") != F.trim(F.col("i_category"))).count())

dim_item = (dedup(i, ["i_item_sk"], "i_rec_start_date")
    .select(
        F.col("i_item_sk").cast("int").alias("item_sk"),
        F.col("i_item_id").alias("item_id"),
        F.trim(F.col("i_product_name")).alias("product_name"),
        F.trim(F.col("i_item_desc")).alias("product_description"),
        # trim the leading space the merch ERP feed injects into category
        F.upper(F.trim(F.col("i_category"))).alias("category"),
        F.col("i_category_id").cast("int").alias("category_id"),
        F.upper(F.trim(F.col("i_class"))).alias("product_class"),
        F.col("i_class_id").cast("int").alias("class_id"),
        F.trim(F.col("i_brand")).alias("brand"),
        F.col("i_brand_id").cast("int").alias("brand_id"),
        F.trim(F.col("i_manufact")).alias("manufacturer"),
        F.col("i_manager_id").cast("int").alias("manager_id"),
        F.trim(F.col("i_size")).alias("size"),
        F.trim(F.col("i_color")).alias("color"),
        F.trim(F.col("i_units")).alias("unit_of_measure"),
        F.trim(F.col("i_container")).alias("container"),
        F.col("i_current_price").cast("decimal(7,2)").alias("list_price"),
        F.col("i_wholesale_cost").cast("decimal(7,2)").alias("wholesale_cost"),
        F.col("i_rec_start_date").cast("date").alias("valid_from"),
        F.col("i_rec_end_date").cast("date").alias("valid_to"))
    .withColumn("is_current", F.col("valid_to").isNull())
    .withColumn("is_price_missing", F.col("list_price").isNull())
    .withColumn("unit_margin",
                F.when(F.col("list_price").isNotNull(),
                       (F.col("list_price") - F.col("wholesale_cost")).cast("decimal(9,2)")))
    .withColumn("margin_pct",
                F.when(F.col("list_price") > 0,
                       F.round((F.col("list_price") - F.col("wholesale_cost")) / F.col("list_price") * 100, 2)))
    .withColumn("price_tier",
        F.when(F.col("list_price").isNull(), "UNPRICED")
         .when(F.col("list_price") < 10, "VALUE")
         .when(F.col("list_price") < 50, "STANDARD")
         .when(F.col("list_price") < 150, "PREMIUM")
         .otherwise("LUXURY")))
n = publish(dim_item, "dim_item", files=1,
            comment="Product master with margin, price tier and SCD-2 validity window")
dq("dim_item", "rows published", "out", n)
dq("dim_item", "current records", "out",
   spark.table(f"{SILVER_DB}.dim_item").filter("is_current").count())

# %% [markdown]
# ### 1.5 · Selling locations and logistics
#
# Nine smaller dimensions in one pass — stores, web sites, web pages, call centres,
# catalogue pages, warehouses, promotions, ship modes and return reasons. Small tables,
# but they are what turn a fact table of surrogate keys into a readable business answer.

# %%
# ---------------------------------------------------------------------
# 1.5  SELLING LOCATIONS & LOGISTICS — 9 smaller conformed dimensions
# ---------------------------------------------------------------------
section("1.5", "SELLING LOCATIONS AND LOGISTICS")
publish(dedup(bronze("store"), ["s_store_sk"], "s_rec_start_date").select(
        F.col("s_store_sk").cast("int").alias("store_sk"),
        F.col("s_store_id").alias("store_id"),
        F.trim(F.col("s_store_name")).alias("store_name"),          # trailing space from POS feed
        F.col("s_number_employees").cast("int").alias("employee_count"),
        F.col("s_floor_space").cast("int").alias("floor_space_sqft"),
        F.trim(F.col("s_manager")).alias("store_manager"),
        F.col("s_market_id").cast("int").alias("market_id"),
        F.trim(F.col("s_market_desc")).alias("market_description"),
        F.trim(F.col("s_geography_class")).alias("geography_class"),
        F.trim(F.col("s_division_name")).alias("division_name"),
        F.trim(F.col("s_company_name")).alias("company_name"),
        F.initcap(F.trim(F.col("s_city"))).alias("city"),
        F.upper(F.trim(F.col("s_state"))).alias("state_code"),
        F.trim(F.col("s_zip")).alias("postal_code"),
        F.col("s_tax_percentage").cast("decimal(5,2)").alias("tax_percentage"),
        F.col("s_rec_start_date").cast("date").alias("valid_from"),
        F.col("s_rec_end_date").cast("date").alias("valid_to"))
    .withColumn("is_current", F.col("valid_to").isNull())
    .withColumn("size_band", F.when(F.col("floor_space_sqft") > 8000, "LARGE")
                              .when(F.col("floor_space_sqft") > 4000, "MEDIUM").otherwise("SMALL")),
    "dim_store", files=1, comment="Physical store master")

publish(dedup(bronze("web_site"), ["web_site_sk"], "web_rec_start_date").select(
        F.col("web_site_sk").cast("int").alias("web_site_sk"),
        F.col("web_site_id").alias("web_site_id"),
        F.trim(F.col("web_name")).alias("site_name"),
        F.trim(F.col("web_class")).alias("site_class"),
        F.trim(F.col("web_manager")).alias("site_manager"),
        F.trim(F.col("web_mkt_class")).alias("market_class"),
        F.trim(F.col("web_company_name")).alias("company_name"),
        F.upper(F.trim(F.col("web_state"))).alias("state_code"),
        F.col("web_tax_percentage").cast("decimal(5,2)").alias("tax_percentage"),
        F.col("web_rec_start_date").cast("date").alias("valid_from"),
        F.col("web_rec_end_date").cast("date").alias("valid_to"))
    .withColumn("is_current", F.col("valid_to").isNull()),
    "dim_web_site", files=1, comment="E-commerce storefront master")

publish(dedup(bronze("web_page"), ["wp_web_page_sk"], "wp_rec_start_date").select(
        F.col("wp_web_page_sk").cast("int").alias("web_page_sk"),
        F.col("wp_web_page_id").alias("web_page_id"),
        F.trim(F.col("wp_url")).alias("page_url"),
        F.upper(F.trim(F.col("wp_type"))).alias("page_type"),
        (F.col("wp_autogen_flag") == "Y").alias("is_autogenerated"),
        F.col("wp_char_count").cast("int").alias("char_count"),
        F.col("wp_link_count").cast("int").alias("link_count"),
        F.col("wp_image_count").cast("int").alias("image_count")),
    "dim_web_page", files=1, comment="Web page catalogue")

publish(dedup(bronze("call_center"), ["cc_call_center_sk"], "cc_rec_start_date").select(
        F.col("cc_call_center_sk").cast("int").alias("call_center_sk"),
        F.col("cc_call_center_id").alias("call_center_id"),
        F.trim(F.col("cc_name")).alias("call_center_name"),
        F.trim(F.col("cc_class")).alias("call_center_class"),
        F.col("cc_employees").cast("int").alias("employee_count"),
        F.trim(F.col("cc_manager")).alias("manager"),
        F.trim(F.col("cc_division_name")).alias("division_name"),
        F.upper(F.trim(F.col("cc_state"))).alias("state_code"),
        F.col("cc_tax_percentage").cast("decimal(5,2)").alias("tax_percentage")),
    "dim_call_center", files=1, comment="Catalog channel call centres")

publish(dedup(bronze("catalog_page"), ["cp_catalog_page_sk"]).select(
        F.col("cp_catalog_page_sk").cast("int").alias("catalog_page_sk"),
        F.col("cp_catalog_page_id").alias("catalog_page_id"),
        F.trim(F.col("cp_department")).alias("department"),
        F.col("cp_catalog_number").cast("int").alias("catalog_number"),
        F.col("cp_catalog_page_number").cast("int").alias("page_number"),
        F.upper(F.trim(F.col("cp_type"))).alias("catalog_type"),
        F.col("cp_start_date_sk").cast("int").alias("start_date_sk"),
        F.col("cp_end_date_sk").cast("int").alias("end_date_sk")),
    "dim_catalog_page", files=1, comment="Printed catalogue pages")

publish(dedup(bronze("warehouse"), ["w_warehouse_sk"]).select(
        F.col("w_warehouse_sk").cast("int").alias("warehouse_sk"),
        F.col("w_warehouse_id").alias("warehouse_id"),
        F.trim(F.col("w_warehouse_name")).alias("warehouse_name"),
        F.col("w_warehouse_sq_ft").cast("int").alias("warehouse_sqft"),
        F.initcap(F.trim(F.col("w_city"))).alias("city"),
        F.upper(F.trim(F.col("w_state"))).alias("state_code"),
        F.trim(F.col("w_zip")).alias("postal_code")),
    "dim_warehouse", files=1, comment="Distribution centres")

publish(dedup(bronze("promotion"), ["p_promo_sk"]).select(
        F.col("p_promo_sk").cast("int").alias("promo_sk"),
        F.col("p_promo_id").alias("promo_id"),
        F.trim(F.col("p_promo_name")).alias("promo_name"),
        F.col("p_cost").cast("decimal(15,2)").alias("promo_cost"),
        F.col("p_item_sk").cast("int").alias("promoted_item_sk"),
        F.col("p_start_date_sk").cast("int").alias("start_date_sk"),
        F.col("p_end_date_sk").cast("int").alias("end_date_sk"),
        F.upper(F.trim(F.col("p_purpose"))).alias("promo_purpose"),
        (F.col("p_discount_active") == "Y").alias("is_discount_active"),
        (F.col("p_channel_email") == "Y").alias("channel_email"),
        (F.col("p_channel_dmail") == "Y").alias("channel_direct_mail"),
        (F.col("p_channel_catalog") == "Y").alias("channel_catalog"),
        (F.col("p_channel_tv") == "Y").alias("channel_tv"),
        (F.col("p_channel_radio") == "Y").alias("channel_radio"),
        (F.col("p_channel_press") == "Y").alias("channel_press"),
        (F.col("p_channel_event") == "Y").alias("channel_event"))
    .withColumn("media_channel_count",
        sum([F.col(x).cast("int") for x in
             ["channel_email","channel_direct_mail","channel_catalog","channel_tv",
              "channel_radio","channel_press","channel_event"]]))
    .withColumn("is_multichannel", F.col("media_channel_count") > 1),
    "dim_promotion", files=1, comment="Marketing promotions with media-channel mix")

publish(dedup(bronze("ship_mode"), ["sm_ship_mode_sk"]).select(
        F.col("sm_ship_mode_sk").cast("int").alias("ship_mode_sk"),
        F.col("sm_ship_mode_id").alias("ship_mode_id"),
        F.upper(F.trim(F.col("sm_type"))).alias("ship_type"),
        F.trim(F.col("sm_code")).alias("ship_code"),
        F.trim(F.col("sm_carrier")).alias("carrier")),
    "dim_ship_mode", files=1, comment="Shipping methods and carriers")

publish(dedup(bronze("reason"), ["r_reason_sk"]).select(
        F.col("r_reason_sk").cast("int").alias("reason_sk"),
        F.col("r_reason_id").alias("reason_id"),
        F.trim(F.col("r_reason_desc")).alias("reason_description")),
    "dim_return_reason", files=1, comment="Return reason codes")

publish(dedup(bronze("household_demographics"), ["hd_demo_sk"]).select(
        F.col("hd_demo_sk").cast("int").alias("household_demo_sk"),
        F.col("hd_income_band_sk").cast("int").alias("income_band_sk"),
        F.trim(F.col("hd_buy_potential")).alias("buy_potential"),
        F.col("hd_dep_count").cast("int").alias("dependent_count"),
        F.col("hd_vehicle_count").cast("int").alias("vehicle_count")),
    "dim_household", files=1, comment="Household demographic profile")

# %% [markdown]
# ### 1.6 · The conforming step — one fact from three channels
#
# The heart of the medallion pattern.
#
# `store_sales`, `catalog_sales` and `web_sales` are three different tables, with three
# different column vocabularies, at three different grains of detail. `conform()` projects
# each onto **one canonical schema**, filling in a typed `NULL` where a channel genuinely
# has no equivalent column — a catalogue order has no web page, a store sale has no
# shipping address.
#
# The result is a single `fct_sales` where *"revenue by channel"* is a `GROUP BY`, not a
# three-way union written by hand in every query.

# %%
# ---------------------------------------------------------------------
# 1.6  THE CONFORMING STEP — one sales fact from three channels
#
#      This is the single most valuable thing silver does. Store, catalog
#      and web arrive with different column names, different keys and
#      different measures. Downstream nobody should have to know that.
# ---------------------------------------------------------------------
section("1.6", "THE CONFORMING STEP — ONE FACT FROM THREE CHANNELS")
CANON_SALES = [
    ("sold_date_sk", "int"), ("sold_time_sk", "int"), ("ship_date_sk", "int"),
    ("item_sk", "int"), ("customer_sk", "int"), ("ship_customer_sk", "int"),
    ("address_sk", "int"), ("cdemo_sk", "int"), ("hdemo_sk", "int"),
    ("store_sk", "int"), ("call_center_sk", "int"), ("catalog_page_sk", "int"),
    ("web_site_sk", "int"), ("web_page_sk", "int"),
    ("ship_mode_sk", "int"), ("warehouse_sk", "int"), ("promo_sk", "int"),
    ("order_number", "bigint"), ("quantity", "int"),
    ("wholesale_cost", "decimal(7,2)"), ("list_price", "decimal(7,2)"),
    ("sales_price", "decimal(7,2)"), ("ext_discount_amt", "decimal(7,2)"),
    ("ext_sales_price", "decimal(7,2)"), ("ext_wholesale_cost", "decimal(7,2)"),
    ("ext_list_price", "decimal(7,2)"), ("ext_tax", "decimal(7,2)"),
    ("coupon_amt", "decimal(7,2)"), ("ext_ship_cost", "decimal(7,2)"),
    ("net_paid", "decimal(7,2)"), ("net_paid_inc_tax", "decimal(7,2)"),
    ("net_profit", "decimal(7,2)"),
]

SALES_MAP = {
 "STORE": ("store_sales", {
    "sold_date_sk":"ss_sold_date_sk", "sold_time_sk":"ss_sold_time_sk",
    "item_sk":"ss_item_sk", "customer_sk":"ss_customer_sk", "address_sk":"ss_addr_sk",
    "cdemo_sk":"ss_cdemo_sk", "hdemo_sk":"ss_hdemo_sk", "store_sk":"ss_store_sk",
    "promo_sk":"ss_promo_sk", "order_number":"ss_ticket_number", "quantity":"ss_quantity",
    "wholesale_cost":"ss_wholesale_cost", "list_price":"ss_list_price",
    "sales_price":"ss_sales_price", "ext_discount_amt":"ss_ext_discount_amt",
    "ext_sales_price":"ss_ext_sales_price", "ext_wholesale_cost":"ss_ext_wholesale_cost",
    "ext_list_price":"ss_ext_list_price", "ext_tax":"ss_ext_tax",
    "coupon_amt":"ss_coupon_amt", "net_paid":"ss_net_paid",
    "net_paid_inc_tax":"ss_net_paid_inc_tax", "net_profit":"ss_net_profit"}),
 "CATALOG": ("catalog_sales", {
    "sold_date_sk":"cs_sold_date_sk", "sold_time_sk":"cs_sold_time_sk",
    "ship_date_sk":"cs_ship_date_sk", "item_sk":"cs_item_sk",
    "customer_sk":"cs_bill_customer_sk", "ship_customer_sk":"cs_ship_customer_sk",
    "address_sk":"cs_bill_addr_sk", "cdemo_sk":"cs_bill_cdemo_sk", "hdemo_sk":"cs_bill_hdemo_sk",
    "call_center_sk":"cs_call_center_sk", "catalog_page_sk":"cs_catalog_page_sk",
    "ship_mode_sk":"cs_ship_mode_sk", "warehouse_sk":"cs_warehouse_sk",
    "promo_sk":"cs_promo_sk", "order_number":"cs_order_number", "quantity":"cs_quantity",
    "wholesale_cost":"cs_wholesale_cost", "list_price":"cs_list_price",
    "sales_price":"cs_sales_price", "ext_discount_amt":"cs_ext_discount_amt",
    "ext_sales_price":"cs_ext_sales_price", "ext_wholesale_cost":"cs_ext_wholesale_cost",
    "ext_list_price":"cs_ext_list_price", "ext_tax":"cs_ext_tax",
    "coupon_amt":"cs_coupon_amt", "ext_ship_cost":"cs_ext_ship_cost",
    "net_paid":"cs_net_paid", "net_paid_inc_tax":"cs_net_paid_inc_tax",
    "net_profit":"cs_net_profit"}),
 "WEB": ("web_sales", {
    "sold_date_sk":"ws_sold_date_sk", "sold_time_sk":"ws_sold_time_sk",
    "ship_date_sk":"ws_ship_date_sk", "item_sk":"ws_item_sk",
    "customer_sk":"ws_bill_customer_sk", "ship_customer_sk":"ws_ship_customer_sk",
    "address_sk":"ws_bill_addr_sk", "cdemo_sk":"ws_bill_cdemo_sk", "hdemo_sk":"ws_bill_hdemo_sk",
    "web_site_sk":"ws_web_site_sk", "web_page_sk":"ws_web_page_sk",
    "ship_mode_sk":"ws_ship_mode_sk", "warehouse_sk":"ws_warehouse_sk",
    "promo_sk":"ws_promo_sk", "order_number":"ws_order_number", "quantity":"ws_quantity",
    "wholesale_cost":"ws_wholesale_cost", "list_price":"ws_list_price",
    "sales_price":"ws_sales_price", "ext_discount_amt":"ws_ext_discount_amt",
    "ext_sales_price":"ws_ext_sales_price", "ext_wholesale_cost":"ws_ext_wholesale_cost",
    "ext_list_price":"ws_ext_list_price", "ext_tax":"ws_ext_tax",
    "coupon_amt":"ws_coupon_amt", "ext_ship_cost":"ws_ext_ship_cost",
    "net_paid":"ws_net_paid", "net_paid_inc_tax":"ws_net_paid_inc_tax",
    "net_profit":"ws_net_profit"}),
}


def conform(df, canon, mapping, channel):
    """Project a channel-specific table onto the canonical schema.
    A column the channel does not have becomes a typed NULL, not an error."""
    cols, missing = [], []
    for name, dtype in canon:
        src = mapping.get(name)
        if src and src in df.columns:
            cols.append(F.col(src).cast(dtype).alias(name))
        else:
            missing.append(name)
            cols.append(F.lit(None).cast(dtype).alias(name))
    if missing:
        print("      %-8s has no: %s" % (channel, ", ".join(missing)))
    return df.select(*cols).withColumn("channel", F.lit(channel))


raw_sales = None
for channel, (tbl, mapping) in SALES_MAP.items():
    part = conform(bronze(tbl), CANON_SALES, mapping, channel)
    raw_sales = part if raw_sales is None else raw_sales.unionByName(part)

# The union is read three times below (count, dedup, publish). Caching 51M
# rows would not fit the driver heap, so stage it to disk once instead —
# every later read is then a cheap columnar scan of one narrow table.
raw_sales = demo_slice(raw_sales, "channel", "order_number", "item_sk")
stage(raw_sales, "_stg_sales")
raw_sales = spark.table(f"{SILVER_DB}._stg_sales")
n_raw = raw_sales.count()
dq("fct_sales", "bronze rows (3 channels)", "in", n_raw)

# %% [markdown]
# ### 1.7 · Deduplicate, quarantine, publish — then record the evidence
#
# Three things happen here, in order:
#
# 1. **Dedup the CDC replays.** The natural key of a sales line is
#    `(channel, order_number, item_sk)`. Bronze has duplicates on it because the change
#    feed was replayed. A `row_number()` window keeps the last version — exactly what a
#    MERGE would do.
# 2. **Quarantine, don't delete.** Rows failing a data contract — missing sold date,
#    non-positive quantity, unsettled amount — are written to `dq_rejects_sales` *with
#    the reason attached*. They are recoverable and auditable; a `WHERE` clause that
#    dropped them would be neither.
# 3. **Publish** the clean fact, partitioned by year.
#
# Finally, **§1.10 persists the data-quality ledger**. `dq_metrics` is the deliverable
# most demos skip: every `dq ·` line printed above is also written to a table, so the
# question *"how do you know the data is good?"* has a SQL answer rather than a
# scrollback answer.

# %%
# ---------------------------------------------------------------------
# 1.7  Dedup the CDC replays, quarantine the bad rows, publish fct_sales
# ---------------------------------------------------------------------
# The natural key of a sales line is (channel, order_number, item_sk).
# Bronze has duplicates on it because the CDC batch was replayed.
# Read three times below (the count, the quarantine and the fact), so stage it
# to disk once rather than recomputing the shuffle each time. On this heap a
# .cache() of even the sliced set is slower to re-read than plain Parquet.
section("1.7", "DEDUPLICATE, QUARANTINE, PUBLISH — THEN RECORD THE EVIDENCE")
stage(dedup(raw_sales, ["channel", "order_number", "item_sk"], "sold_date_sk"),
      "_stg_sales_dedup")
deduped = spark.table(f"{SILVER_DB}._stg_sales_dedup")
n_dedup = deduped.count()
dq("fct_sales", "duplicate lines removed", "clean", n_raw - n_dedup)

# The data contract: a sales line must have a date, an item, a positive
# quantity and a settled amount. Anything else is quarantined, with the
# reason attached, so operations can chase the source system.
reject_reason = (F.when(F.col("sold_date_sk").isNull(), "MISSING_SOLD_DATE")
                  .when(F.col("item_sk").isNull(), "MISSING_ITEM")
                  .when(F.col("net_paid").isNull(), "UNSETTLED_AMOUNT")
                  .when(F.col("quantity") <= 0, "NON_POSITIVE_QUANTITY"))

flagged = deduped.withColumn("reject_reason", reject_reason)
rejects = flagged.filter(F.col("reject_reason").isNotNull())
clean = flagged.filter(F.col("reject_reason").isNull()).drop("reject_reason")

publish(rejects.withColumn("rejected_at", F.current_timestamp()),
        "dq_rejects_sales", files=2,
        comment="Quarantined sales lines that failed the silver data contract")
for row in (spark.table(f"{SILVER_DB}.dq_rejects_sales")
                 .groupBy("reject_reason").count().collect()):
    dq("fct_sales", "rejected: " + row["reject_reason"], "reject", row["count"])

dim_date_lkp = spark.table(f"{SILVER_DB}.dim_date").select(
    F.col("date_sk").alias("_dsk"), F.col("calendar_date").alias("sold_date"),
    F.col("calendar_year").alias("sold_year"), F.col("month_of_year").alias("sold_month"),
    F.col("year_month").alias("sold_year_month"))

fct_sales = (clean
    .join(F.broadcast(dim_date_lkp), F.col("sold_date_sk") == F.col("_dsk"), "left").drop("_dsk")
    # one stable surrogate key for the line, independent of source system
    .withColumn("sales_line_sk",
                F.sha2(F.concat_ws("|", F.col("channel"), F.col("order_number"),
                                   F.col("item_sk")), 256).substr(1, 32))
    # business measures every downstream mart would otherwise recompute
    .withColumn("gross_revenue", F.col("ext_sales_price"))
    .withColumn("discount_amount",
                (F.coalesce(F.col("ext_discount_amt"), F.lit(0)) +
                 F.coalesce(F.col("coupon_amt"), F.lit(0))).cast("decimal(9,2)"))
    .withColumn("net_revenue", F.col("net_paid"))
    .withColumn("cost_of_goods", F.col("ext_wholesale_cost"))
    .withColumn("gross_margin",
                (F.col("net_paid") - F.coalesce(F.col("ext_wholesale_cost"), F.lit(0))).cast("decimal(9,2)"))
    .withColumn("margin_pct",
                F.when(F.col("net_paid") > 0,
                       F.round(F.col("gross_margin") / F.col("net_paid") * 100, 2)))
    .withColumn("discount_pct",
                F.when(F.col("ext_list_price") > 0,
                       F.round(F.col("discount_amount") / F.col("ext_list_price") * 100, 2)))
    .withColumn("is_promotional",
                F.col("promo_sk").isNotNull() & (F.col("discount_amount") > 0))
    .withColumn("is_shipped", F.col("ship_date_sk").isNotNull())
    .withColumn("days_to_ship",
                F.when(F.col("ship_date_sk").isNotNull(),
                       F.col("ship_date_sk") - F.col("sold_date_sk")))
    .filter(F.col("sold_year").isNotNull()))

n = publish(fct_sales, "fct_sales", partition_by="sold_year", files=8,
            comment="Conformed sales fact — store, catalog and web at one grain")
dq("fct_sales", "rows published", "out", n)
for _t in ("_stg_sales", "_stg_sales_dedup"):
    spark.sql(f"DROP TABLE IF EXISTS {SILVER_DB}.{_t}")

print()
spark.sql(f"""SELECT channel, sold_year, count(*) AS lines,
                     round(sum(net_revenue)/1e6, 1) AS net_revenue_musd
              FROM {SILVER_DB}.fct_sales GROUP BY channel, sold_year
              ORDER BY sold_year, channel""").show(30, False)

# %
# ---------------------------------------------------------------------
# 1.10  Persist this part's slice of the data-quality ledger
# ---------------------------------------------------------------------
schema = StructType([
    StructField("table_name", StringType()), StructField("rule", StringType()),
    StructField("scope", StringType()), StructField("metric_value", LongType()),
])
(spark.createDataFrame(DQ, schema)
      .withColumn("batch_id", F.lit(BATCH_ID))
      .withColumn("measured_at", F.current_timestamp())
      .repartition(1)
      .write.mode("overwrite").format("parquet")
      .saveAsTable(f"{SILVER_DB}.dq_metrics"))
print("dq_metrics rows written (overwrite):", len(DQ))

section("1.9", "SILVER TABLES SO FAR")
spark.sql(f"SHOW TABLES IN {SILVER_DB}").show(30, False)
finished(T_RUN, "SILVER PART A")
print("   next: 01b_silver_facts_returns_inventory")
