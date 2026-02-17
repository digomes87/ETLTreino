from datetime import datetime, timedelta
from domain import Transaction


DATETIME_FORMAT = datetime(2026, 1, 1, 0, 0, 0)

def test_transaction_model_validates_fields():
    """
    Ensure that Transaction model fields are valid.
    This timestamp fixed is just for validate the mapping in input
    """
    # fixed_ts = DATETIME_FORMAT
    t = Transaction(
        id="x",
        product="credit_card",
        amount=10.0,
        currency="USD",
        timestamp=DATETIME_FORMAT,
        status="posted",
    )
    assert t.amount == 10.0

def test_transaction_preserves_provided_timestamp_exactly():
    ts = DATETIME_FORMAT

    t = Transaction(
        id="y",
        product="credit_card",
        amount=5.0,
        currency="EUR",
        timestamp=DATETIME_FORMAT,
        status="pending",
    )
    assert t.timestamp == ts




# @pytest.fixture
# def fixed_ts():
#     return datetime(2026, 1, 1, 0, 0, 0)
#
#
# def test_transaction_model_validates_fields(fixed_ts):
#     t = Transaction(
#         id="x",
#         product="credit_card",
#         amount=10.0,
#         currency="USD",
#         timestamp=fixed_ts,
#         status="posted",
#     )
#     assert t.amount == 10.0
#
#
# def test_transaction_preserves_provided_timestamp_exactly(fixed_ts):
#     t = Transaction(
#         id="y",
#         product="credit_card",
#         amount=5.0,
#         currency="EUR",
#         timestamp=fixed_ts,
#         status="pending",
#     )
#     assert t.timestamp == fixed_ts

def test_transaction_accepts_past_end_future_timestamp():
    now = datetime.now()
    past = now - timedelta(days=365)
    future = now + timedelta(days=365)


    past_tx = Transaction(
        id="past",
        product="mortgage",
        amount=2000.0,
        currency="GBP",
        timestamp=past,
        status="posted",
    )

    future_tx = Transaction(
        id="future",
        product="auto_loan",
        amount=3000.0,
        currency="BRL",
        timestamp=future,
        status="pending",
    )

    assert past_tx.timestamp == past
    assert future_tx.timestamp == future