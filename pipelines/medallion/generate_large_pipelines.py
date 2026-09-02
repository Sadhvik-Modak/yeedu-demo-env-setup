#!/usr/bin/env python3
"""Generate the two large medallion demo pipelines.

These are 100-task DAGs, which is far past what is sensible to hand-author, so
they are described here as source systems / dimensions / marts / domains and
expanded into the JSON that ``provision.py`` uploads.

Run ``python3 generate_large_pipelines.py`` to rewrite the two definitions in
``definitions/``.  Nothing here talks to the API.
"""

import json
import os

HERE = os.path.dirname(os.path.abspath(__file__))
DEFINITIONS = os.path.join(HERE, "definitions")

# Cluster tiers.  Only these three are attached to demo_workspace; 980
# (onprem_large) is attached to no workspace and is rejected with RFA-000129.
LIGHT = 979   # onprem_small  - counts, ratios, metadata, DDL
MEDIUM = 977  # onprem_medium - ingestion feeds, exception handling
HEAVY = 978   # aws_medium    - joins, window functions, wide aggregations


def task(key, notebook=None, deps=None, cluster=None, run_if=None,
         params=None, retries=None, retry_delay=None, fallback=None, **extra):
    """Build one task dict.  ``deps`` is a list of task keys or (key, outcome)."""
    t = {"task_key": key}
    if notebook:
        t["__notebook__"] = notebook
    if deps:
        t["depends_on"] = [
            {"task_key": d[0], "outcome": d[1]} if isinstance(d, tuple)
            else {"task_key": d}
            for d in deps
        ]
    if run_if:
        t["run_if"] = run_if
    if cluster:
        t["task_cluster_id"] = cluster
    if fallback:
        t["cluster_ids"] = fallback
    if retries is not None:
        t["retries"] = retries
        t["retry_delay"] = retry_delay if retry_delay is not None else 30000
    if params:
        t["parameters"] = params
    t.update(extra)
    return t


RUN_DATE = {"RUN_DATE": "{{ job.parameters.run_date }}"}


# --------------------------------------------------------------------------
# Pipeline 3: enterprise_banking_medallion_platform  (100 tasks)
# --------------------------------------------------------------------------

SOURCES = [
    ("core_banking",        "core-banking-cbs"),
    ("payments_switch",     "payments-switch"),
    ("cards_authorisation", "cards-auth-host"),
    ("retail_loans",        "loan-origination"),
    ("term_deposits",       "deposits-ledger"),
    ("forex_treasury",      "treasury-fx"),
    ("atm_network",         "atm-switch"),
    ("digital_channel",     "mobile-internet-banking"),
    ("wealth_management",   "wealth-portfolio"),
    ("trade_finance",       "trade-finance-hub"),
]

DIMS = ["account", "customer", "merchant", "branch"]

MARTS = [
    "account_balance_daily",
    "fraud_signal_summary",
    "channel_mix_daily",
    "merchant_spend_monthly",
    "loan_exposure_snapshot",
    "deposit_growth_trend",
    "fx_position_daily",
    "atm_utilisation_daily",
    "customer_360_profile",
    "regulatory_capital_input",
    "card_spend_category",
    "trade_finance_exposure",
]

HOUSEKEEPING = [
    "archive_bronze_partitions",
    "compact_silver_tables",
    "optimize_gold_layout",
    "refresh_bi_extracts",
    "capture_lineage_graph",
    "publish_cost_report",
    "evaluate_sla_targets",
]


def enterprise_platform():
    t = []

    # --- stage 1: platform setup -----------------------------------------
    t.append(task("platform_setup", "00_setup_environment", cluster=LIGHT,
                  retries=1, params=dict(RUN_DATE,
                                         DQ_THRESHOLD="{{ job.parameters.dq_threshold }}")))
    t.append(task("metastore_preflight", "00_setup_environment",
                  deps=["platform_setup"], cluster=LIGHT, retries=1, params=RUN_DATE))
    t.append(task("landing_zone_scan", "11_table_stats",
                  deps=["platform_setup"], cluster=LIGHT, retries=1,
                  params=dict(RUN_DATE, TARGET_TABLE="banking_bronze.raw_accounts")))

    # --- stage 2: land, profile and check every source system -------------
    # Ten feeds land in parallel; each one is profiled and DQ-checked on its
    # own before anything is allowed to fan in.
    for i, (src, system) in enumerate(SOURCES):
        ingest_nb = "01_ingest_bronze_accounts" if i % 2 == 0 else "02_ingest_bronze_transactions"
        t.append(task(f"land_{src}", ingest_nb,
                      deps=["metastore_preflight", "landing_zone_scan"],
                      cluster=MEDIUM, fallback=[HEAVY], retries=2, retry_delay=60000,
                      params=dict(RUN_DATE, SOURCE_SYSTEM=system)))
        t.append(task(f"profile_{src}", "11_table_stats", deps=[f"land_{src}"],
                      cluster=LIGHT, retries=1,
                      params=dict(RUN_DATE, TARGET_TABLE=f"banking_bronze.raw_{src}")))
        t.append(task(f"dq_bronze_{src}", "03_validate_bronze", deps=[f"profile_{src}"],
                      cluster=LIGHT, retries=1,
                      params=dict(RUN_DATE, DQ_THRESHOLD="{{ job.parameters.dq_threshold }}")))

    # --- stage 3: the bronze gate ----------------------------------------
    t.append(task("bronze_dq_rollup", "03_validate_bronze",
                  deps=[f"dq_bronze_{s}" for s, _ in SOURCES],
                  run_if="All Done", cluster=LIGHT, retries=1,
                  params=dict(RUN_DATE, DQ_THRESHOLD="{{ job.parameters.dq_threshold }}")))
    t.append(task("bronze_gate", deps=["bronze_dq_rollup"],
                  condition_task={"op": "EQUAL_TO",
                                  "left": "{{ tasks.bronze_dq_rollup.values.dq_status }}",
                                  "right": "PASS"}))
    t.append(task("quarantine_bronze", "10_quarantine_bad_records",
                  deps=[("bronze_gate", "false")], cluster=MEDIUM, retries=1, params=RUN_DATE))
    t.append(task("bronze_incident_report", "09_publish_and_audit",
                  deps=["quarantine_bronze"], cluster=LIGHT, retries=0,
                  params=dict(RUN_DATE, NOTIFY_ONLY="true")))

    # --- stage 4: conformed silver ---------------------------------------
    # Dimensions first, then every source conforms against them.
    for d in DIMS:
        t.append(task(f"silver_dim_{d}", "04_build_silver_dim_account",
                      deps=[("bronze_gate", "true")], cluster=HEAVY,
                      retries=1, retry_delay=60000,
                      params=dict(RUN_DATE, DIMENSION=d)))
    for src, _ in SOURCES:
        t.append(task(f"silver_conform_{src}", "05_build_silver_txn_clean",
                      deps=[f"silver_dim_{d}" for d in DIMS],
                      cluster=HEAVY, fallback=[MEDIUM], retries=2, retry_delay=120000,
                      params=dict(RUN_DATE, SOURCE_DOMAIN=src)))
    for src, _ in SOURCES:
        t.append(task(f"dq_silver_{src}", "06_validate_silver",
                      deps=[f"silver_conform_{src}"], cluster=LIGHT, retries=1,
                      params=dict(RUN_DATE, SOURCE_DOMAIN=src)))

    # --- stage 5: the silver gate ----------------------------------------
    t.append(task("silver_dq_rollup", "06_validate_silver",
                  deps=[f"dq_silver_{s}" for s, _ in SOURCES],
                  run_if="None Failed", cluster=LIGHT, retries=1, params=RUN_DATE))
    t.append(task("silver_gate", deps=["silver_dq_rollup"],
                  condition_task={"op": "EQUAL_TO",
                                  "left": "{{ tasks.silver_dq_rollup.values.dq_status }}",
                                  "right": "PASS"}))
    t.append(task("silver_repair", "10_quarantine_bad_records",
                  deps=[("silver_gate", "false")], cluster=MEDIUM, retries=1, params=RUN_DATE))

    # --- stage 6: the gold marts -----------------------------------------
    for i, mart in enumerate(MARTS):
        nb = "07_gold_account_balance_daily" if i % 2 == 0 else "08_gold_fraud_signal_summary"
        # Alternate clouds so the fan-out visibly spans on-prem and AWS.
        t.append(task(f"gold_{mart}", nb, deps=[("silver_gate", "true")],
                      cluster=HEAVY if i % 2 == 0 else MEDIUM,
                      retries=1, retry_delay=60000,
                      params=dict(RUN_DATE, TARGET_TABLE=f"banking_gold.{mart}")))
    for mart in MARTS:
        t.append(task(f"dq_gold_{mart}", "06_validate_silver", deps=[f"gold_{mart}"],
                      cluster=LIGHT, retries=1,
                      params=dict(RUN_DATE, TARGET_TABLE=f"banking_gold.{mart}")))

    # --- stage 7: settle, profile, housekeep, publish ---------------------
    t.append(task("settle_delay", deps=[f"dq_gold_{m}" for m in MARTS],
                  run_if="All Done", sleep_task={"seconds": 30}))
    t.append(task("collect_gold_stats", deps=["settle_delay"], for_each_task={
        "inputs": "{{ job.parameters.gold_tables }}", "concurrency": 4,
        "run_if": "All Done",
        "task": task("profile_gold_table", "11_table_stats", cluster=LIGHT, retries=1,
                     params=dict(RUN_DATE, TARGET_TABLE="{{ loop_input }}"))}))
    t.append(task("collect_silver_stats", deps=["settle_delay"], for_each_task={
        "inputs": "{{ job.parameters.silver_tables }}", "concurrency": 3,
        "run_if": "All Done",
        "task": task("profile_silver_table", "11_table_stats", cluster=LIGHT, retries=1,
                     params=dict(RUN_DATE, TARGET_TABLE="{{ loop_input }}"))}))
    for job in HOUSEKEEPING:
        t.append(task(job, "09_publish_and_audit",
                      deps=["collect_gold_stats", "collect_silver_stats"],
                      run_if="All Done", cluster=LIGHT, retries=1,
                      params=dict(RUN_DATE, MAINTENANCE_JOB=job)))
    t.append(task("publish_and_audit", "09_publish_and_audit",
                  deps=HOUSEKEEPING + ["quarantine_bronze", "silver_repair"],
                  run_if="All Done", cluster=LIGHT, retries=1, params=RUN_DATE))
    t.append(task("platform_notify", "09_publish_and_audit", deps=["publish_and_audit"],
                  run_if="All Done", cluster=LIGHT, retries=0,
                  params=dict(RUN_DATE, NOTIFY_ONLY="true")))

    return {
        "name": "enterprise_banking_medallion_platform",
        "description": (
            "Enterprise-scale medallion platform for retail banking: 10 source "
            "systems landed and DQ-checked in parallel, a bronze gate, four "
            "conformed dimensions, per-domain silver conformance, a silver gate, "
            "12 gold marts fanned out across on-prem and AWS, two for-each "
            "profiling loops, housekeeping and an always-runs audit tail. "
            "100 tasks."),
        "schedule_cron": "0 3 * * *",
        "max_active_runs": 1,
        "tags": ["medallion", "banking", "demo", "enterprise", "large-dag"],
        "params": {
            "run_date": "2026-09-03",
            "dq_threshold": "0.98",
            "gold_tables": json.dumps([f"banking_gold.{m}" for m in MARTS]),
            "silver_tables": json.dumps(
                [f"banking_silver.dim_{d}" for d in DIMS]
                + [f"banking_silver.{s}_clean" for s, _ in SOURCES[:4]]),
        },
        "tasks": t,
    }


# --------------------------------------------------------------------------
# Pipeline 4: medallion_backfill_and_reconciliation  (100 tasks)
# --------------------------------------------------------------------------

SLICES = ["2026_08_25", "2026_08_26", "2026_08_27",
          "2026_08_28", "2026_08_29", "2026_08_30"]

DOMAINS = ["accounts", "transactions", "cards", "loans",
           "deposits", "forex", "atm", "digital"]

REGIONS = ["apac", "emea", "amer", "latam"]

CONTROLS = [
    "snapshot_control_totals",
    "compare_to_source_of_record",
    "detect_late_arriving_facts",
    "reprocess_late_facts",
    "refresh_recon_dashboard",
    "update_recon_sla",
    "close_backfill_window",
]


def backfill_reconciliation():
    t = []

    # --- stage 1: decide how to backfill ---------------------------------
    t.append(task("backfill_setup", "00_setup_environment", cluster=LIGHT,
                  retries=1, params=RUN_DATE))
    t.append(task("resolve_backfill_window", "11_table_stats", deps=["backfill_setup"],
                  cluster=LIGHT, retries=1,
                  params=dict(RUN_DATE,
                              WINDOW_START="{{ job.parameters.backfill_start }}",
                              WINDOW_END="{{ job.parameters.backfill_end }}")))
    t.append(task("choose_backfill_mode", deps=["resolve_backfill_window"],
                  switch_case_task={"expr": "{{ job.parameters.backfill_mode }}",
                                    "cases": ["full_reload", "delta_replay", "repair_only"]}))

    # --- stage 2a: full reload -------------------------------------------
    t.append(task("full_truncate_bronze", "10_quarantine_bad_records",
                  deps=[("choose_backfill_mode", "full_reload")],
                  cluster=MEDIUM, retries=1, params=RUN_DATE))
    t.append(task("full_reload_bronze", "02_ingest_bronze_transactions",
                  deps=["full_truncate_bronze"], cluster=MEDIUM, fallback=[HEAVY],
                  retries=2, retry_delay=60000,
                  params=dict(RUN_DATE, LOAD_STRATEGY="FULL_REFRESH")))
    t.append(task("full_rebuild_silver", "05_build_silver_txn_clean",
                  deps=["full_reload_bronze"], cluster=HEAVY, retries=2, retry_delay=120000,
                  params=dict(RUN_DATE, LOAD_STRATEGY="FULL_REFRESH")))
    t.append(task("full_rebuild_gold", "07_gold_account_balance_daily",
                  deps=["full_rebuild_silver"], cluster=HEAVY, retries=1, retry_delay=60000,
                  params=dict(RUN_DATE, LOAD_STRATEGY="FULL_REFRESH")))

    # --- stage 2b: delta replay, one slice at a time ----------------------
    for s in SLICES:
        t.append(task(f"replay_{s}", "02_ingest_bronze_transactions",
                      deps=[("choose_backfill_mode", "delta_replay")],
                      cluster=MEDIUM, fallback=[HEAVY], retries=2, retry_delay=60000,
                      params=dict(RUN_DATE, SLICE_DATE=s.replace("_", "-"))))
        t.append(task(f"verify_{s}", "03_validate_bronze", deps=[f"replay_{s}"],
                      cluster=LIGHT, retries=1,
                      params=dict(RUN_DATE, SLICE_DATE=s.replace("_", "-"))))
    t.append(task("merge_delta_batches", "05_build_silver_txn_clean",
                  deps=[f"verify_{s}" for s in SLICES], run_if="None Failed",
                  cluster=HEAVY, retries=2, retry_delay=120000, params=RUN_DATE))

    # --- stage 2c: repair only -------------------------------------------
    t.append(task("repair_scan", "03_validate_bronze",
                  deps=[("choose_backfill_mode", "repair_only")],
                  cluster=LIGHT, retries=1, params=RUN_DATE))
    t.append(task("repair_apply", "10_quarantine_bad_records", deps=["repair_scan"],
                  cluster=MEDIUM, retries=1, params=RUN_DATE))
    t.append(task("repair_verify", "06_validate_silver", deps=["repair_apply"],
                  cluster=LIGHT, retries=1, params=RUN_DATE))

    # --- stage 3: converge and re-run the core pipeline -------------------
    t.append(task("backfill_converge", "09_publish_and_audit",
                  deps=["full_rebuild_gold", "merge_delta_batches", "repair_verify"],
                  run_if="At Least One Succeeded", cluster=LIGHT, retries=1,
                  params=RUN_DATE))
    t.append(task("rerun_core_medallion", deps=["backfill_converge"],
                  run_job_task={"__pipeline__": "medallion_banking_pipeline"},
                  params={"run_date": "{{ job.parameters.run_date }}",
                          "load_strategy": "BACKFILL"}))
    t.append(task("post_backfill_settle", deps=["rerun_core_medallion"],
                  run_if="All Done", sleep_task={"seconds": 20}))

    # --- stage 4: the reconciliation matrix -------------------------------
    # Three independent controls per domain, then a per-domain gate that
    # either repairs or signs off.
    for d in DOMAINS:
        t.append(task(f"recon_rowcount_{d}", "03_validate_bronze",
                      deps=["post_backfill_settle"], cluster=LIGHT, retries=1,
                      params=dict(RUN_DATE, DOMAIN=d, CONTROL="ROW_COUNT")))
        t.append(task(f"recon_sum_{d}", "06_validate_silver",
                      deps=["post_backfill_settle"], cluster=LIGHT, retries=1,
                      params=dict(RUN_DATE, DOMAIN=d, CONTROL="SUM_AMOUNT")))
        t.append(task(f"recon_checksum_{d}", "11_table_stats",
                      deps=["post_backfill_settle"], cluster=MEDIUM, retries=1,
                      params=dict(RUN_DATE, DOMAIN=d, CONTROL="CHECKSUM",
                                  TARGET_TABLE=f"banking_silver.{d}_clean")))
        t.append(task(f"recon_gate_{d}",
                      deps=[f"recon_rowcount_{d}", f"recon_sum_{d}", f"recon_checksum_{d}"],
                      run_if="All Done",
                      condition_task={"op": "EQUAL_TO",
                                      "left": f"{{{{ tasks.recon_rowcount_{d}.values.dq_status }}}}",
                                      "right": "PASS"}))
        t.append(task(f"recon_repair_{d}", "10_quarantine_bad_records",
                      deps=[(f"recon_gate_{d}", "false")], cluster=MEDIUM, retries=1,
                      params=dict(RUN_DATE, DOMAIN=d)))
        t.append(task(f"recon_signoff_{d}", "09_publish_and_audit",
                      deps=[(f"recon_gate_{d}", "true")], cluster=LIGHT, retries=0,
                      params=dict(RUN_DATE, DOMAIN=d)))

    # --- stage 5: regional variance ---------------------------------------
    for r in REGIONS:
        t.append(task(f"recon_region_{r}", "08_gold_fraud_signal_summary",
                      deps=[f"recon_signoff_{d}" for d in DOMAINS],
                      run_if="All Done", cluster=MEDIUM, retries=1, retry_delay=60000,
                      params=dict(RUN_DATE, REGION=r)))
        t.append(task(f"region_variance_{r}", "06_validate_silver",
                      deps=[f"recon_region_{r}"], cluster=LIGHT, retries=1,
                      params=dict(RUN_DATE, REGION=r)))

    t.append(task("variance_rollup", "06_validate_silver",
                  deps=[f"region_variance_{r}" for r in REGIONS],
                  run_if="All Done", cluster=LIGHT, retries=1, params=RUN_DATE))
    t.append(task("variance_gate", deps=["variance_rollup"],
                  condition_task={"op": "LESS_THAN_OR_EQUAL",
                                  "left": "{{ tasks.variance_rollup.values.variance_pct }}",
                                  "right": "{{ job.parameters.variance_tolerance }}"}))
    t.append(task("variance_escalate", "09_publish_and_audit",
                  deps=[("variance_gate", "false")], cluster=LIGHT, retries=0,
                  params=dict(RUN_DATE, NOTIFY_ONLY="true", SEVERITY="HIGH")))
    t.append(task("variance_accept", "09_publish_and_audit",
                  deps=[("variance_gate", "true")], cluster=LIGHT, retries=0,
                  params=RUN_DATE))

    # --- stage 6: profile, report, close ----------------------------------
    t.append(task("collect_recon_stats", deps=["variance_accept", "variance_escalate"],
                  run_if="All Done", for_each_task={
                      "inputs": "{{ job.parameters.recon_tables }}", "concurrency": 4,
                      "run_if": "All Done",
                      "task": task("profile_recon_table", "11_table_stats",
                                   cluster=LIGHT, retries=1,
                                   params=dict(RUN_DATE, TARGET_TABLE="{{ loop_input }}"))}))
    t.append(task("collect_domain_stats", deps=["variance_accept", "variance_escalate"],
                  run_if="All Done", for_each_task={
                      "inputs": "{{ job.parameters.recon_domains }}", "concurrency": 4,
                      "run_if": "All Done",
                      "task": task("profile_recon_domain", "11_table_stats",
                                   cluster=LIGHT, retries=1,
                                   params=dict(RUN_DATE, DOMAIN="{{ loop_input }}"))}))
    t.append(task("reconciliation_report", "09_publish_and_audit",
                  deps=["collect_recon_stats", "collect_domain_stats"],
                  run_if="All Done", cluster=LIGHT, retries=1, params=RUN_DATE))
    t.append(task("archive_recon_evidence", "09_publish_and_audit",
                  deps=["reconciliation_report"], cluster=LIGHT, retries=1, params=RUN_DATE))
    t.append(task("notify_data_stewards", "09_publish_and_audit",
                  deps=["archive_recon_evidence"], run_if="All Done",
                  cluster=LIGHT, retries=0, params=dict(RUN_DATE, NOTIFY_ONLY="true")))
    t.append(task("notify_risk_office", "09_publish_and_audit",
                  deps=["archive_recon_evidence"], run_if="All Done",
                  cluster=LIGHT, retries=0, params=dict(RUN_DATE, NOTIFY_ONLY="true")))

    prev = ["notify_data_stewards", "notify_risk_office"]
    for job in CONTROLS:
        t.append(task(job, "09_publish_and_audit", deps=prev, run_if="All Done",
                      cluster=LIGHT, retries=1, params=dict(RUN_DATE, CONTROL_STEP=job)))
        prev = [job]
    t.append(task("final_signoff", "09_publish_and_audit", deps=prev,
                  run_if="All Done", cluster=LIGHT, retries=0, params=RUN_DATE))

    return {
        "name": "medallion_backfill_and_reconciliation",
        "description": (
            "Backfill and reconciliation control pipeline. Switch-case routes "
            "between a full reload, a six-slice delta replay and a repair-only "
            "path, re-runs the core medallion pipeline as a nested job, then "
            "drives an 8-domain x 3-control reconciliation matrix with per-domain "
            "gates, regional variance analysis, a tolerance gate and a "
            "seven-step close-out. 100 tasks."),
        "schedule_cron": "0 5 * * 0",
        "max_active_runs": 1,
        "tags": ["medallion", "banking", "demo", "backfill", "reconciliation", "large-dag"],
        "params": {
            "run_date": "2026-09-03",
            "backfill_mode": "delta_replay",
            "backfill_start": "2026-08-25",
            "backfill_end": "2026-08-30",
            "variance_tolerance": "0.5",
            "recon_domains": json.dumps(DOMAINS),
            "recon_tables": json.dumps([f"banking_silver.{d}_clean" for d in DOMAINS]),
        },
        "tasks": t,
    }


# --------------------------------------------------------------------------

def validate(pipeline):
    """Catch the mistakes that are easy to make at this size."""
    tasks = pipeline["tasks"]
    keys = [t["task_key"] for t in tasks]
    dupes = {k for k in keys if keys.count(k) > 1}
    assert not dupes, f"{pipeline['name']}: duplicate task keys {dupes}"

    seen = set()
    for t in tasks:
        for d in t.get("depends_on", []):
            assert d["task_key"] in seen, (
                f"{pipeline['name']}: {t['task_key']} depends on "
                f"{d['task_key']}, which is not defined earlier")
        seen.add(t["task_key"])

    # for_each inputs must resolve to a JSON-encoded *string* param, otherwise
    # the generated DAG dies in resolve_foreach_input with
    # "'list' object has no attribute 'strip'".
    for t in tasks:
        fe = t.get("for_each_task")
        if not fe:
            continue
        token = fe["inputs"].strip("{} ").strip()
        if token.startswith("job.parameters."):
            name = token.split(".", 2)[2]
            value = pipeline["params"].get(name)
            assert isinstance(value, str), (
                f"{pipeline['name']}: for_each param {name!r} must be a JSON string")
            assert isinstance(json.loads(value), list)

    for t in tasks:
        assert "retries" not in t or "run_job_task" not in t, (
            f"{pipeline['name']}: {t['task_key']} - retries is not allowed on run_job_task")
    return len(tasks)


def main():
    for build, filename in ((enterprise_platform, "enterprise_banking_medallion_platform.json"),
                            (backfill_reconciliation, "medallion_backfill_and_reconciliation.json")):
        pipeline = build()
        count = validate(pipeline)
        path = os.path.join(DEFINITIONS, filename)
        with open(path, "w") as fh:
            json.dump(pipeline, fh, indent=2)
            fh.write("\n")
        kinds = {}
        for t in pipeline["tasks"]:
            kind = next((k for k in t if k.endswith("_task")), "notebook_task")
            kinds[kind] = kinds.get(kind, 0) + 1
        print(f"{pipeline['name']:<42} {count:>3} tasks  {filename}")
        for k, v in sorted(kinds.items()):
            print(f"      {k:<20} {v}")


if __name__ == "__main__":
    main()
