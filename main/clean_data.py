from __future__ import annotations

import pandas as pd

DOMAINS = ["example.com", "gmail.com", "yahoo.com", "outlook.com", "hotmail.com"]


# -------------------------
# 1) Nettoyage colonnes
# -------------------------
def clean_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out.columns = out.columns.astype(str).str.strip()
    return out


# -------------------------
# 2) EMAIL
# -------------------------
def fix_email_simple(x, domains=DOMAINS):
    s = str(x).strip().lower()
    if s in ("", "nan", "none"):
        return pd.NA
    if "@" in s:
        return s
    for dom in domains:
        if dom in s:
            return s.replace(dom, "@" + dom, 1)
    return pd.NA


def clean_email(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if "email" in out.columns:
        out["email"] = out["email"].apply(fix_email_simple)
    return out


# -------------------------
# 3) FULL_NAME
# -------------------------
def filter_full_name_two_words(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if "full_name" in out.columns:
        name = out["full_name"].fillna("").astype(str).str.strip()
        mask = name.str.match(r"^\S+\s+\S+$")
        out = out[mask].copy()
    return out


# -------------------------
# 4) SIGNUP_DATE
# -------------------------
def clean_signup_date_2025(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if "signup_date" in out.columns:
        signup_str = out["signup_date"].fillna("").astype(str).str.strip()
        out = out[signup_str.str.startswith("2025")].copy()

        out["signup_date"] = pd.to_datetime(out["signup_date"], errors="coerce")
        out = out[out["signup_date"].notna()].copy()
    return out


# -------------------------
# 5) AGE
# -------------------------
def clean_age_int_range(df: pd.DataFrame, min_age=16, max_age=100) -> pd.DataFrame:
    out = df.copy()
    if "age" in out.columns:
        age_str = out["age"].fillna("").astype(str).str.replace(",", ".", regex=False)
        age_num = pd.to_numeric(age_str, errors="coerce")

        mask = age_num.notna() & (age_num % 1 == 0) & (age_num >= min_age) & (age_num <= max_age)
        out = out[mask].copy()
        out["age"] = age_num[mask].astype("Int64")
    return out


# -------------------------
# 6) LAST_PURCHASE_AMOUNT
# -------------------------
def clean_last_purchase_amount_nonneg(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if "last_purchase_amount" in out.columns:
        amt_str = out["last_purchase_amount"].fillna("").astype(str).str.replace(",", ".", regex=False)
        amt = pd.to_numeric(amt_str, errors="coerce")

        out = out[~(amt < 0)].copy()
        out["last_purchase_amount"] = amt
    return out


# -------------------------
# 7) COUNTRY
# -------------------------
def clean_country_2letters(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if "country" in out.columns:
        out["country"] = out["country"].fillna("").astype(str).str.strip().str.upper().str[:2]
    return out


# -------------------------
# 8) DEDUPE EMAIL
# -------------------------
def dedupe_by_email(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if "email" in out.columns:
        out = out.drop_duplicates(subset=["email"], keep="first")
    return out


# -------------------------
# PIPELINE GLOBAL
# -------------------------
def clean_customers_df(df: pd.DataFrame) -> pd.DataFrame:
    out = clean_columns(df)
    out = clean_email(out)
    out = filter_full_name_two_words(out)
    out = clean_signup_date_2025(out)
    out = clean_age_int_range(out)
    out = clean_last_purchase_amount_nonneg(out)
    out = clean_country_2letters(out)
    out = dedupe_by_email(out)
    return out.reset_index(drop=True)