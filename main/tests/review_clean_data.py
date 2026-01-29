import pandas as pd
import pandas.api.types as ptypes

# Import local (clean_data.py est dans le dossier parent: main/)
from clean_data import (
    clean_columns,
    fix_email_simple,
    clean_email,
    filter_full_name_two_words,
    clean_signup_date_2025,
    clean_age_int_range,
    clean_last_purchase_amount_nonneg,
    clean_country_2letters,
    dedupe_by_email,
    clean_customers_df,
)

# -------------------------
# clean_columns
# -------------------------
def test_clean_columns_strips_spaces():
    df = pd.DataFrame({" full_name ": ["John Doe"], "email  ": ["a@example.com"]})
    out = clean_columns(df)
    assert "full_name" in out.columns
    assert "email" in out.columns


# -------------------------
# EMAIL
# -------------------------
def test_fix_email_simple_lowercase_and_keeps_valid():
    assert fix_email_simple("John@Gmail.com") == "john@gmail.com"

def test_fix_email_simple_empty_to_na():
    assert pd.isna(fix_email_simple(""))

def test_fix_email_simple_repairs_missing_at():
    assert fix_email_simple("john.smithgmail.com") == "john.smith@gmail.com"

def test_clean_email_applies_column():
    df = pd.DataFrame({"email": ["john.smithgmail.com", "ok@yahoo.com", None]})
    out = clean_email(df)
    assert out.loc[0, "email"] == "john.smith@gmail.com"
    assert out.loc[1, "email"] == "ok@yahoo.com"
    assert pd.isna(out.loc[2, "email"])


# -------------------------
# FULL_NAME
# -------------------------
def test_filter_full_name_two_words_only_keeps_two_tokens():
    df = pd.DataFrame({"full_name": ["John Doe", "Alice", "  ", None, "Jean  Morel"]})
    out = filter_full_name_two_words(df)
    assert out["full_name"].tolist() == ["John Doe", "Jean  Morel"]


# -------------------------
# SIGNUP_DATE
# -------------------------
def test_clean_signup_date_2025_filters_and_parses():
    df = pd.DataFrame({"signup_date": ["2025-02-15", "2024-01-01", "2025-13-01", None]})
    out = clean_signup_date_2025(df)
    assert len(out) == 1
    assert str(out.iloc[0]["signup_date"].date()) == "2025-02-15"

def test_clean_signup_date_2025_dtype_datetime():
    df = pd.DataFrame({"signup_date": ["2025-02-15"]})
    out = clean_signup_date_2025(df)
    assert ptypes.is_datetime64_any_dtype(out["signup_date"])


# -------------------------
# AGE
# -------------------------
def test_clean_age_int_range_keeps_int_like_values():
    df = pd.DataFrame({"age": ["16", "25", "25.0", "15", "101", "abc", None]})
    out = clean_age_int_range(df)
    assert out["age"].tolist() == [16, 25, 25]

def test_clean_age_int_range_dtype_is_nullable_int():
    df = pd.DataFrame({"age": ["20"]})
    out = clean_age_int_range(df)
    assert str(out["age"].dtype) == "Int64"


# -------------------------
# LAST_PURCHASE_AMOUNT
# -------------------------
def test_clean_last_purchase_amount_nonneg_filters_negative():
    df = pd.DataFrame({"last_purchase_amount": ["10", "-5", "0", None, "12,5"]})
    out = clean_last_purchase_amount_nonneg(df)

    values = out["last_purchase_amount"].tolist()
    assert values[0] == 10.0
    assert values[1] == 0.0
    assert pd.isna(values[2])
    assert values[3] == 12.5


# -------------------------
# COUNTRY
# -------------------------
def test_clean_country_2letters_upper_and_trim():
    df = pd.DataFrame({"country": [" fr", "United Kingdom", "", None]})
    out = clean_country_2letters(df)
    assert out["country"].tolist() == ["FR", "UN", "", ""]


# -------------------------
# DEDUPE EMAIL
# -------------------------
def test_dedupe_by_email_keeps_first():
    df = pd.DataFrame({"email": ["a@example.com", "a@example.com", "b@example.com"]})
    out = dedupe_by_email(df)
    assert out["email"].tolist() == ["a@example.com", "b@example.com"]


# -------------------------
# END-TO-END
# -------------------------
def test_clean_customers_df_pipeline_end_to_end():
    df = pd.DataFrame({
        "full_name": ["John Doe", "Alice", None],
        "email": ["john.smithgmail.com", "bad", "john.smithgmail.com"],
        "signup_date": ["2025-02-15", "2025-01-10", "2024-01-01"],
        "age": ["25", "42", "30"],
        "last_purchase_amount": ["10", "-1", "5"],
        "country": ["fr", "FR", "uk"],
    })
    out = clean_customers_df(df)
    assert len(out) == 1
    assert out.loc[0, "full_name"] == "John Doe"
    assert out.loc[0, "email"] == "john.smith@gmail.com"
    assert out.loc[0, "country"] == "FR"