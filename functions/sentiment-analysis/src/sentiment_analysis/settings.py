"""Environment-driven configuration for the sentiment analysis function."""

import os

# Allow ops to tune sensitivity without a code change/redeploy.
POSITIVE_THRESHOLD = float(os.getenv("SENTIMENT_POSITIVE_THRESHOLD", "0.1"))
NEGATIVE_THRESHOLD = float(os.getenv("SENTIMENT_NEGATIVE_THRESHOLD", "-0.1"))
