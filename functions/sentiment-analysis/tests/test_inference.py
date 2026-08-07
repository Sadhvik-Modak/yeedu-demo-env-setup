from sentiment_analysis.inference import SentimentService


def test_analyze_detects_negative_sentiment():
    service = SentimentService()
    result = service.analyze("This medication is terrible and made me feel awful all week.")
    assert result["sentiment"] == "negative"
    assert result["polarity"] < 0


def test_analyze_detects_positive_sentiment():
    service = SentimentService()
    result = service.analyze("The staff were amazing and the treatment worked wonderfully.")
    assert result["sentiment"] == "positive"
    assert result["polarity"] > 0


def test_analyze_returns_all_fields():
    service = SentimentService()
    result = service.analyze("The package arrived on Tuesday.")
    assert set(result) == {"sentiment", "polarity", "subjectivity"}
