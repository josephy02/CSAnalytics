from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType
from datetime import datetime, timedelta
import random

def create_spark_session():
    """Initialize Spark session"""
    return SparkSession.builder \
        .appName("Support Ticket Generator") \
        .config("spark.driver.memory", "2g") \
        .config("spark.driver.host", "localhost") \
        .config("spark.driver.allowMultipleContexts", "true") \
        .config("spark.driver.extraJavaOptions", "-Djava.security.manager=allow") \
        .master("local[*]") \
        .getOrCreate()

def define_schema():
    """Define the schema for support tickets"""
    return StructType([
        StructField("ticket_id", StringType(), False),
        StructField("customer_id", StringType(), True),
        StructField("issue_category", StringType(), True),
        StructField("priority", StringType(), True),
        StructField("status", StringType(), True),
        StructField("creation_date", TimestampType(), True),
        StructField("resolution_date", TimestampType(), True),
        StructField("satisfaction_score", IntegerType(), True),
        StructField("description", StringType(), True)
    ])

def generate_sample_data(num_tickets=1000):
    """Generate sample support ticket data"""
    # Sample data values
    issue_categories = [
        'Login Issues', 'Performance', 'Billing', 'Security', 
        'API Integration', 'Database', 'Networking', 'Storage'
    ]
    priorities = ['Low', 'Medium', 'High', 'Critical']
    statuses = ['New', 'In Progress', 'Resolved', 'Closed', 'Escalated']
    
    # Generate data
    data = []
    end_date = datetime.now()
    start_date = end_date - timedelta(days=30)
    
    for i in range(num_tickets):
        # Generate base ticket data
        ticket_id = f'TICK-{i:05d}'
        customer_id = f'CUST-{random.randint(1, 100):03d}'
        category = random.choice(issue_categories)
        priority = random.choice(priorities)
        status = random.choice(statuses)
        
        # Generate creation date
        creation_date = start_date + timedelta(
            seconds=random.randint(0, int((end_date - start_date).total_seconds()))
        )
        
        # Generate resolution date for resolved/closed tickets
        resolution_date = None
        satisfaction_score = None
        if status in ['Resolved', 'Closed']:
            resolution_date = creation_date + timedelta(hours=random.randint(1, 72))
            satisfaction_score = random.randint(1, 5)
        
        # Generate description
        description = f"Customer reported {category.lower()} issue - Priority: {priority}"
        
        data.append((
            ticket_id, customer_id, category, priority, status,
            creation_date, resolution_date, satisfaction_score, description
        ))
    
    return data

def save_data(spark, data, schema, output_path):
    """Save the generated data as parquet file"""
    df = spark.createDataFrame(data, schema)
    df.write.mode("overwrite").parquet(output_path)
    return df

if __name__ == "__main__":
    # Initialize Spark
    spark = create_spark_session()
    
    # Generate data
    schema = define_schema()
    data = generate_sample_data(1000)
    
    # Save data
    output_path = "../data/raw/support_tickets"
    df = save_data(spark, data, schema, output_path)
    
    # Show sample of generated data
    print("\nSample of generated tickets:")
    df.show(5, truncate=False)
    
    # Print some basic statistics
    print("\nDataset Statistics:")
    print(f"Total Tickets: {df.count()}")
    print("\nDistribution by Category:")
    df.groupBy("issue_category").count().show()
    print("\nDistribution by Priority:")
    df.groupBy("priority").count().show()
    
    # Stop Spark session
    spark.stop()