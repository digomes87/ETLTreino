from typing import Iterable, List
from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from core.interfaces import Validator


ALLOWED_PRODUCTS = {"credit_card", "personal_loan", "mortgage", "auto_loan"}
ALLOWED_CURRENCIES = {"USD", "EUR", "BRL", "GBP"}
ALLOWED_STATUS = {"posted", "pending", "reversed"}


class TransactionValidator(Validator):
    def validate(self, df: DataFrame) -> Iterable[str]:
        errors: List[str] = []
        nulls = df.filter(
            col("id").isNull()
            | col("product").isNull()
            | col("amount").isNull()
            | col("currency").isNull()
            | col("timestamp").isNull()
            | col("status").isNull()
        ).count()
        if nulls > 0:
            errors.append(f"Null fields detected: {nulls}")
        non_positive = df.filter(col("amount") <= 0).count()
        if non_positive > 0:
            errors.append(f"Non-positive amounts: {non_positive}")
        invalid_product = df.filter(~col("product").isin(*ALLOWED_PRODUCTS)).count()
        if invalid_product > 0:
            errors.append(f"Invalid products: {invalid_product}")
        invalid_currency = df.filter(~col("currency").isin(*ALLOWED_CURRENCIES)).count()
        if invalid_currency > 0:
            errors.append(f"Invalid currencies: {invalid_currency}")
        invalid_status = df.filter(~col("status").isin(*ALLOWED_STATUS)).count()
        if invalid_status > 0:
            errors.append(f"Invalid status: {invalid_status}")
        return errors
