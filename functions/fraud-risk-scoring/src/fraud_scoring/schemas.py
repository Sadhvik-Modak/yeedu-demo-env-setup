"""Claim field definitions shared by validation, inference, and docs/config examples."""

REQUIRED_FEATURES = (
    "claim_amount",
    "policy_tenure_months",
    "num_prior_claims",
    "premium_amount",
    "claim_to_premium_ratio",
    "days_to_report",
)

RISK_LABELS = ("legitimate", "fraud")

# Claims scoring at or above this probability get routed to the Special
# Investigations Unit instead of auto-approved.
FRAUD_PROBABILITY_THRESHOLD = 0.5


def extract_features(payload: dict) -> dict:
    """Pull the required claim feature keys out of an arbitrary request payload."""
    return {name: payload.get(name) for name in REQUIRED_FEATURES}


def missing_features(features: dict) -> list:
    """Return the subset of required feature names that are absent (None) from `features`."""
    return [name for name, value in features.items() if value is None]
