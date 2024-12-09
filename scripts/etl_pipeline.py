from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, datediff, hour, dayofweek, when, avg, count,
    unix_timestamp, from_unixtime, lit, current_timestamp
)

class SupportTicketETL:
    def __init__(self, spark):
        """Initialize ETL with SparkSession"""
        self.spark = spark

    def extract(self, input_path):
        """Extract data from parquet files"""
        return self.spark.read.parquet(input_path)

    def transform(self, df):
        """Apply transformations to the support ticket data"""
        # Calculate resolution time in hours
        df = df.withColumn(
            "resolution_time_hours",
            when(col("resolution_date").isNotNull(),
                 (unix_timestamp("resolution_date") - unix_timestamp("creation_date")) / 3600
            ).otherwise(None)
        )
        
        # Add time-based features
        df = df.withColumn("creation_hour", hour("creation_date")) \
               .withColumn("creation_day", dayofweek("creation_date"))
        
        # Add SLA status
        df = df.withColumn(
            "sla_status",
            when(col("priority") == "Critical", 
                 when(col("resolution_time_hours") <= 4, "Within SLA")
                 .otherwise("Breached SLA"))
            .when(col("priority") == "High",
                 when(col("resolution_time_hours") <= 8, "Within SLA")
                 .otherwise("Breached SLA"))
            .when(col("priority") == "Medium",
                 when(col("resolution_time_hours") <= 24, "Within SLA")
                 .otherwise("Breached SLA"))
            .when(col("priority") == "Low",
                 when(col("resolution_time_hours") <= 48, "Within SLA")
                 .otherwise("Breached SLA"))
            .otherwise("Unknown")
        )
        
        # Calculate customer metrics
        customer_metrics = df.groupBy("customer_id").agg(
            count("ticket_id").alias("total_tickets"),
            avg("satisfaction_score").alias("avg_satisfaction"),
            count(when(col("sla_status") == "Breached SLA", True)).alias("sla_breaches")
        )
        
        # Join customer metrics back to main dataframe
        df = df.join(customer_metrics, "customer_id")
        
        return df

    def load(self, df, output_path):
        """Load processed data into parquet files"""
        # Save main processed data
        df.write.mode("overwrite").parquet(f"{output_path}/processed_tickets")
        
        # Create and save aggregated views
        
        # Daily metrics
        daily_metrics = df.groupBy(
            from_unixtime(unix_timestamp("creation_date"), "yyyy-MM-dd").alias("date")
        ).agg(
            count("ticket_id").alias("total_tickets"),
            avg("resolution_time_hours").alias("avg_resolution_time"),
            avg("satisfaction_score").alias("avg_satisfaction"),
            count(when(col("sla_status") == "Breached SLA", True)).alias("sla_breaches")
        )
        daily_metrics.write.mode("overwrite").parquet(f"{output_path}/daily_metrics")
        
        # Category metrics
        category_metrics = df.groupBy("issue_category").agg(
            count("ticket_id").alias("total_tickets"),
            avg("resolution_time_hours").alias("avg_resolution_time"),
            avg("satisfaction_score").alias("avg_satisfaction"),
            count(when(col("sla_status") == "Breached SLA", True)).alias("sla_breaches")
        )
        category_metrics.write.mode("overwrite").parquet(f"{output_path}/category_metrics")

    def run_pipeline(self, input_path, output_path):
        """Execute the full ETL pipeline"""
        print("Starting ETL pipeline...")
        
        # Extract
        print("Extracting data...")
        raw_df = self.extract(input_path)
        
        # Transform
        print("Transforming data...")
        processed_df = self.transform(raw_df)
        
        # Load
        print("Loading processed data...")
        self.load(processed_df, output_path)
        
        print("ETL pipeline completed successfully!")
        return processed_df

if __name__ == "__main__":
    # Initialize Spark
    spark = SparkSession.builder \
        .appName("Support Ticket ETL") \
        .master("local[*]") \
        .getOrCreate()
    
    # Run ETL
    etl = SupportTicketETL(spark)
    processed_df = etl.run_pipeline(
        input_path="../data/raw/support_tickets",
        output_path="../data/processed"
    )
    
    # Show sample results
    print("\nSample of processed tickets:")
    processed_df.show(5)
    
    # Stop Spark session
    spark.stop()