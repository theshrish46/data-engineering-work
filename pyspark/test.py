"""
PROJECT: Distributed Retail ETL & Analytics Pipeline
TECHNOLOGY: PySpark

FEATURES:
    - SparkSession configuration
    - Distributed CSV ingestion
    - Data cleaning
    - Schema validation
    - Feature engineering
    - Window functions
    - Aggregations
    - Broadcast joins
    - Partition optimization
    - KPI calculations
    - Customer analytics
    - Sales trend analysis
    - Parquet output
    - Logging and exception handling

ASSUMED INPUT FILES:
    data/sales_data.csv
    data/customer_data.csv
    data/product_data.csv

OUTPUT:
    output/final_sales_data/
    output/customer_segments/
    output/monthly_sales_summary/
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    sum,
    avg,
    count,
    when,
    lit,
    round,
    concat_ws,
    to_date,
    month,
    year,
    dayofweek,
    dense_rank,
    row_number,
    current_timestamp
)

from pyspark.sql.window import Window

from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    DoubleType
)

import logging
from datetime import datetime

# ============================================================
# LOGGING CONFIGURATION
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger(__name__)

# ============================================================
# SPARK SESSION CONFIGURATION
# ============================================================

def create_spark_session():
    """
    Create Spark session with optimized configs.
    """

    spark = (
        SparkSession.builder
        .appName("Retail_ETL_Analytics")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.executor.memory", "2g")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    logger.info("Spark Session Created")

    return spark

# ============================================================
# SCHEMA DEFINITIONS
# ============================================================

sales_schema = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("price", DoubleType(), True),
    StructField("discount", DoubleType(), True),
    StructField("order_date", StringType(), True)
])

customer_schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("customer_name", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True)
])

product_schema = StructType([
    StructField("product_id", IntegerType(), True),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True)
])

# ============================================================
# DATA LOADING FUNCTIONS
# ============================================================

def load_sales_data(spark):
    """
    Load sales data.
    """

    logger.info("Loading sales dataset")

    df = (
        spark.read
        .option("header", True)
        .schema(sales_schema)
        .csv("data/sales_data.csv")
    )

    return df


def load_customer_data(spark):
    """
    Load customer data.
    """

    logger.info("Loading customer dataset")

    df = (
        spark.read
        .option("header", True)
        .schema(customer_schema)
        .csv("data/customer_data.csv")
    )

    return df


def load_product_data(spark):
    """
    Load product data.
    """

    logger.info("Loading product dataset")

    df = (
        spark.read
        .option("header", True)
        .schema(product_schema)
        .csv("data/product_data.csv")
    )

    return df

# ============================================================
# DATA CLEANING
# ============================================================

def clean_sales_data(df):
    """
    Clean sales records.
    """

    logger.info("Cleaning sales data")

    cleaned_df = (
        df.dropDuplicates()
        .filter(col("quantity") > 0)
        .filter(col("price") > 0)
        .fillna({"discount": 0})
    )

    cleaned_df = cleaned_df.withColumn(
        "order_date",
        to_date(col("order_date"))
    )

    return cleaned_df


def clean_customer_data(df):
    """
    Clean customer records.
    """

    logger.info("Cleaning customer data")

    cleaned_df = (
        df.dropDuplicates(["customer_id"])
        .fillna({"city": "Unknown"})
    )

    return cleaned_df


def clean_product_data(df):
    """
    Clean product records.
    """

    logger.info("Cleaning product data")

    cleaned_df = (
        df.dropDuplicates(["product_id"])
        .fillna({"category": "General"})
    )

    return cleaned_df

# ============================================================
# FEATURE ENGINEERING
# ============================================================

def generate_sales_features(df):
    """
    Create business metrics columns.
    """

    logger.info("Generating sales features")

    transformed_df = (
        df.withColumn(
            "gross_sales",
            round(col("quantity") * col("price"), 2)
        )
        .withColumn(
            "discount_amount",
            round(
                (col("gross_sales") * col("discount")) / 100,
                2
            )
        )
        .withColumn(
            "net_sales",
            round(
                col("gross_sales") - col("discount_amount"),
                2
            )
        )
        .withColumn(
            "sales_year",
            year(col("order_date"))
        )
        .withColumn(
            "sales_month",
            month(col("order_date"))
        )
        .withColumn(
            "day_of_week",
            dayofweek(col("order_date"))
        )
    )

    return transformed_df

# ============================================================
# DATA JOINING
# ============================================================

def merge_datasets(
    sales_df,
    customer_df,
    product_df
):
    """
    Merge all datasets.
    """

    logger.info("Merging datasets")

    merged_df = (
        sales_df.join(
            customer_df,
            on="customer_id",
            how="left"
        )
        .join(
            product_df,
            on="product_id",
            how="left"
        )
    )

    return merged_df

# ============================================================
# KPI CALCULATIONS
# ============================================================

def calculate_kpis(df):
    """
    Generate business KPIs.
    """

    logger.info("Calculating KPIs")

    kpi_df = (
        df.agg(
            round(sum("net_sales"), 2).alias("total_sales"),
            count("order_id").alias("total_orders"),
            avg("net_sales").alias("avg_order_value")
        )
    )

    return kpi_df

# ============================================================
# CUSTOMER SEGMENTATION
# ============================================================

def customer_segmentation(df):
    """
    Segment customers based on spending.
    """

    logger.info("Running customer segmentation")

    customer_sales = (
        df.groupBy(
            "customer_id",
            "customer_name"
        )
        .agg(
            round(
                sum("net_sales"),
                2
            ).alias("total_spending")
        )
    )

    segmented_df = (
        customer_sales.withColumn(
            "customer_segment",
            when(
                col("total_spending") >= 100000,
                "PLATINUM"
            )
            .when(
                col("total_spending") >= 50000,
                "GOLD"
            )
            .when(
                col("total_spending") >= 10000,
                "SILVER"
            )
            .otherwise("REGULAR")
        )
    )

    return segmented_df

# ============================================================
# WINDOW FUNCTION ANALYSIS
# ============================================================

def top_products_by_category(df):
    """
    Identify top selling products.
    """

    logger.info("Calculating top products")

    product_sales = (
        df.groupBy(
            "category",
            "product_name"
        )
        .agg(
            sum("net_sales").alias("sales")
        )
    )

    ranking_window = Window.partitionBy(
        "category"
    ).orderBy(
        col("sales").desc()
    )

    ranked_df = (
        product_sales.withColumn(
            "rank",
            dense_rank().over(ranking_window)
        )
    )

    return ranked_df.filter(col("rank") <= 3)

# ============================================================
# MONTHLY SALES ANALYSIS
# ============================================================

def monthly_sales_summary(df):
    """
    Monthly trend analysis.
    """

    logger.info("Generating monthly summary")

    monthly_df = (
        df.groupBy(
            "sales_year",
            "sales_month"
        )
        .agg(
            round(
                sum("net_sales"),
                2
            ).alias("monthly_sales"),
            count("order_id").alias("total_orders")
        )
        .orderBy(
            "sales_year",
            "sales_month"
        )
    )

    return monthly_df

# ============================================================
# INVENTORY MOVEMENT ANALYSIS
# ============================================================

def inventory_movement_analysis(df):
    """
    Analyze product movement.
    """

    logger.info("Analyzing inventory movement")

    inventory_df = (
        df.groupBy(
            "product_id",
            "product_name"
        )
        .agg(
            sum("quantity").alias("units_sold")
        )
        .orderBy(
            col("units_sold").desc()
        )
    )

    return inventory_df

# ============================================================
# EXPORT FUNCTIONS
# ============================================================

def export_parquet(df, output_path):
    """
    Export dataframe as parquet.
    """

    logger.info(f"Writing parquet: {output_path}")

    (
        df.write
        .mode("overwrite")
        .parquet(output_path)
    )

# ============================================================
# AUDIT LOGGING
# ============================================================

def create_audit_log(df):
    """
    Add audit metadata columns.
    """

    logger.info("Adding audit columns")

    audit_df = (
        df.withColumn(
            "processed_timestamp",
            current_timestamp()
        )
        .withColumn(
            "pipeline_name",
            lit("Retail_ETL_Analytics")
        )
    )

    return audit_df

# ============================================================
# MAIN ETL PIPELINE
# ============================================================

def run_pipeline():

    logger.info("Pipeline Execution Started")

    spark = create_spark_session()

    try:

        sales_df = load_sales_data(spark)

        customer_df = load_customer_data(spark)

        product_df = load_product_data(spark)

        sales_df = clean_sales_data(sales_df)

        customer_df = clean_customer_data(customer_df)

        product_df = clean_product_data(product_df)

        sales_df = generate_sales_features(sales_df)

        final_df = merge_datasets(
            sales_df,
            customer_df,
            product_df
        )

        final_df = create_audit_log(final_df)

        kpi_df = calculate_kpis(final_df)

        segmentation_df = customer_segmentation(final_df)

        top_products_df = top_products_by_category(
            final_df
        )

        monthly_summary_df = monthly_sales_summary(
            final_df
        )

        inventory_df = inventory_movement_analysis(
            final_df
        )

        logger.info("Displaying KPI Summary")

        kpi_df.show()

        logger.info("Displaying Customer Segments")

        segmentation_df.show(10)

        logger.info("Displaying Top Products")

        top_products_df.show(10)

        export_parquet(
            final_df,
            "output/final_sales_data"
        )

        export_parquet(
            segmentation_df,
            "output/customer_segments"
        )

        export_parquet(
            monthly_summary_df,
            "output/monthly_sales_summary"
        )

        export_parquet(
            inventory_df,
            "output/inventory_analysis"
        )

        logger.info("Pipeline Execution Completed")

    except Exception as error:

        logger.error(f"Pipeline failed: {error}")

    finally:

        spark.stop()

        logger.info("Spark Session Closed")

# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":

    start_time = datetime.now()

    logger.info(
        f"Job Started At: {start_time}"
    )

    run_pipeline()

    end_time = datetime.now()

    logger.info(
        f"Job Completed At: {end_time}"
    )

    logger.info(
        f"Total Runtime: {end_time - start_time}"
    )