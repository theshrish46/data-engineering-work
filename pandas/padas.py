"""
PROJECT: Retail Sales Analytics Pipeline using Pandas
AUTHOR : Demo Analytics Project
PURPOSE:
    End-to-end retail analytics workflow using pandas.

FEATURES:
    - Data ingestion
    - Data cleaning
    - Feature engineering
    - KPI generation
    - Customer segmentation
    - Sales trend analysis
    - Inventory analysis
    - Export reporting
    - Logging
    - Exception handling

DATA ASSUMPTIONS:
    sales.csv
    customers.csv
    products.csv
"""

import pandas as pd
import numpy as np
import logging
from datetime import datetime
from pathlib import Path

# ============================================================
# LOGGING CONFIGURATION
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger(__name__)

# ============================================================
# CONFIGURATION
# ============================================================

BASE_DIR = Path("data")
OUTPUT_DIR = Path("output")

SALES_FILE = BASE_DIR / "sales.csv"
CUSTOMERS_FILE = BASE_DIR / "customers.csv"
PRODUCTS_FILE = BASE_DIR / "products.csv"

OUTPUT_DIR.mkdir(exist_ok=True)

# ============================================================
# DATA LOADING FUNCTIONS
# ============================================================

def load_csv_file(file_path: Path) -> pd.DataFrame:
    """
    Generic CSV loader with error handling.
    """
    try:
        logger.info(f"Loading file: {file_path}")
        df = pd.read_csv(file_path)
        logger.info(f"Loaded {len(df)} records")
        return df

    except FileNotFoundError:
        logger.error(f"File not found: {file_path}")
        return pd.DataFrame()

    except Exception as error:
        logger.error(f"Error loading file {file_path}: {error}")
        return pd.DataFrame()

# ============================================================
# DATA CLEANING FUNCTIONS
# ============================================================

def clean_sales_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and standardize sales dataset.
    """

    logger.info("Cleaning sales data")

    df.columns = df.columns.str.lower().str.strip()

    df.drop_duplicates(inplace=True)

    numeric_cols = ["quantity", "price", "discount"]

    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df["discount"] = df["discount"].fillna(0)

    df["quantity"] = df["quantity"].fillna(df["quantity"].median())

    df["price"] = df["price"].fillna(df["price"].mean())

    df["order_date"] = pd.to_datetime(
        df["order_date"],
        errors="coerce"
    )

    df = df[df["quantity"] > 0]
    df = df[df["price"] > 0]

    logger.info("Sales data cleaned successfully")

    return df


def clean_customer_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean customer dataset.
    """

    logger.info("Cleaning customer data")

    df.columns = df.columns.str.lower()

    df.drop_duplicates(subset=["customer_id"], inplace=True)

    df["customer_name"] = (
        df["customer_name"]
        .astype(str)
        .str.title()
    )

    df["city"] = df["city"].fillna("Unknown")

    df["signup_date"] = pd.to_datetime(
        df["signup_date"],
        errors="coerce"
    )

    logger.info("Customer data cleaned")

    return df


def clean_product_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean products dataset.
    """

    logger.info("Cleaning product data")

    df.columns = df.columns.str.lower()

    df["category"] = df["category"].fillna("General")

    df["product_name"] = (
        df["product_name"]
        .astype(str)
        .str.strip()
    )

    logger.info("Product data cleaned")

    return df

# ============================================================
# FEATURE ENGINEERING
# ============================================================

def generate_sales_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Generate analytical features.
    """

    logger.info("Generating sales features")

    df["gross_amount"] = df["quantity"] * df["price"]

    df["discount_amount"] = (
        df["gross_amount"] * df["discount"]
    ) / 100

    df["net_sales"] = (
        df["gross_amount"] - df["discount_amount"]
    )

    df["year"] = df["order_date"].dt.year

    df["month"] = df["order_date"].dt.month

    df["day_name"] = df["order_date"].dt.day_name()

    df["week_number"] = df["order_date"].dt.isocalendar().week

    logger.info("Feature engineering completed")

    return df

# ============================================================
# DATA MERGING
# ============================================================

def merge_datasets(
    sales_df: pd.DataFrame,
    customers_df: pd.DataFrame,
    products_df: pd.DataFrame
) -> pd.DataFrame:
    """
    Merge sales, customer and product datasets.
    """

    logger.info("Merging datasets")

    merged_df = sales_df.merge(
        customers_df,
        on="customer_id",
        how="left"
    )

    merged_df = merged_df.merge(
        products_df,
        on="product_id",
        how="left"
    )

    logger.info(
        f"Merged dataset shape: {merged_df.shape}"
    )

    return merged_df

# ============================================================
# KPI FUNCTIONS
# ============================================================

def calculate_kpis(df: pd.DataFrame) -> dict:
    """
    Calculate business KPIs.
    """

    logger.info("Calculating KPIs")

    kpis = {
        "total_sales": round(df["net_sales"].sum(), 2),
        "total_orders": int(df["order_id"].nunique()),
        "total_customers": int(df["customer_id"].nunique()),
        "average_order_value": round(
            df["net_sales"].mean(),
            2
        ),
        "top_category": (
            df.groupby("category")["net_sales"]
            .sum()
            .idxmax()
        ),
    }

    return kpis

# ============================================================
# CUSTOMER SEGMENTATION
# ============================================================

def customer_segmentation(df: pd.DataFrame) -> pd.DataFrame:
    """
    Segment customers based on sales.
    """

    logger.info("Running customer segmentation")

    customer_sales = (
        df.groupby("customer_id")["net_sales"]
        .sum()
        .reset_index()
    )

    conditions = [
        customer_sales["net_sales"] >= 10000,
        customer_sales["net_sales"] >= 5000,
        customer_sales["net_sales"] >= 1000
    ]

    choices = [
        "Premium",
        "Gold",
        "Silver"
    ]

    customer_sales["segment"] = np.select(
        conditions,
        choices,
        default="Regular"
    )

    logger.info("Segmentation completed")

    return customer_sales

# ============================================================
# SALES TREND ANALYSIS
# ============================================================

def monthly_sales_analysis(df: pd.DataFrame) -> pd.DataFrame:
    """
    Analyze monthly sales trends.
    """

    logger.info("Generating monthly sales trends")

    trend_df = (
        df.groupby(["year", "month"])["net_sales"]
        .sum()
        .reset_index()
    )

    trend_df["growth_rate"] = (
        trend_df["net_sales"]
        .pct_change() * 100
    )

    return trend_df

# ============================================================
# INVENTORY ANALYSIS
# ============================================================

def inventory_analysis(df: pd.DataFrame) -> pd.DataFrame:
    """
    Analyze product movement.
    """

    logger.info("Performing inventory analysis")

    inventory_df = (
        df.groupby(
            ["product_id", "product_name"]
        )["quantity"]
        .sum()
        .reset_index()
    )

    inventory_df.rename(
        columns={"quantity": "units_sold"},
        inplace=True
    )

    inventory_df.sort_values(
        by="units_sold",
        ascending=False,
        inplace=True
    )

    return inventory_df

# ============================================================
# EXPORT FUNCTIONS
# ============================================================

def export_dataframe(
    df: pd.DataFrame,
    filename: str
):
    """
    Export dataframe to CSV.
    """

    output_path = OUTPUT_DIR / filename

    try:
        df.to_csv(output_path, index=False)

        logger.info(
            f"Exported file: {output_path}"
        )

    except Exception as error:
        logger.error(
            f"Export failed: {error}"
        )

# ============================================================
# MAIN PIPELINE
# ============================================================

def run_pipeline():
    """
    Main analytics execution pipeline.
    """

    logger.info("Retail Analytics Pipeline Started")

    sales_df = load_csv_file(SALES_FILE)
    customers_df = load_csv_file(CUSTOMERS_FILE)
    products_df = load_csv_file(PRODUCTS_FILE)

    if sales_df.empty:
        logger.error("Sales dataset unavailable")
        return

    sales_df = clean_sales_data(sales_df)
    customers_df = clean_customer_data(customers_df)
    products_df = clean_product_data(products_df)

    sales_df = generate_sales_features(sales_df)

    final_df = merge_datasets(
        sales_df,
        customers_df,
        products_df
    )

    kpis = calculate_kpis(final_df)

    segmentation_df = customer_segmentation(final_df)

    monthly_trends_df = monthly_sales_analysis(final_df)

    inventory_df = inventory_analysis(final_df)

    export_dataframe(final_df, "final_sales_data.csv")

    export_dataframe(
        segmentation_df,
        "customer_segments.csv"
    )

    export_dataframe(
        monthly_trends_df,
        "monthly_sales_trends.csv"
    )

    export_dataframe(
        inventory_df,
        "inventory_analysis.csv"
    )

    logger.info("========== KPI SUMMARY ==========")

    for key, value in kpis.items():
        logger.info(f"{key}: {value}")

    logger.info("Retail Analytics Pipeline Completed")

# ============================================================
# SCRIPT ENTRY POINT
# ============================================================

if __name__ == "__main__":

    start_time = datetime.now()

    logger.info(f"Process started at {start_time}")

    run_pipeline()

    end_time = datetime.now()

    logger.info(f"Process ended at {end_time}")

    logger.info(
        f"Total runtime: {end_time - start_time}"
    )