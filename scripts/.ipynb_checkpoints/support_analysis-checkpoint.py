import matplotlib.pyplot as plt
# Remove the seaborn style dependency
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, when, hour, dayofweek
import pandas as pd

class SupportAnalyzer:
    def __init__(self, spark):
        """Initialize analyzer with SparkSession"""
        self.spark = spark
        # Use a basic matplotlib style instead
        plt.style.use('default')
        
    def load_data(self, processed_data_path):
        """Load processed data"""
        return self.spark.read.parquet(processed_data_path)
    
    def plot_ticket_distribution(self, df):
        """Plot ticket distribution by category and priority"""
        # Convert to Pandas for plotting
        category_dist = df.groupBy("issue_category") \
            .count() \
            .toPandas() \
            .sort_values("count", ascending=True)
        
        plt.figure(figsize=(10, 6))
        plt.barh(category_dist['issue_category'], category_dist['count'])
        plt.title('Ticket Distribution by Category')
        plt.xlabel('Count')
        plt.ylabel('Category')
        plt.tight_layout()
        plt.show()
        
        priority_dist = df.groupBy("priority") \
            .count() \
            .toPandas()
        
        plt.figure(figsize=(8, 6))
        plt.bar(priority_dist['priority'], priority_dist['count'])
        plt.title('Ticket Distribution by Priority')
        plt.xlabel('Priority')
        plt.ylabel('Count')
        plt.tight_layout()
        plt.show()
    
    def plot_resolution_times(self, df):
        """Plot resolution time analysis"""
        resolution_by_priority = df.filter(col("resolution_time_hours").isNotNull()) \
            .groupBy("priority") \
            .agg(avg("resolution_time_hours").alias("avg_resolution_hours")) \
            .toPandas()
        
        plt.figure(figsize=(8, 6))
        plt.bar(resolution_by_priority['priority'], 
                resolution_by_priority['avg_resolution_hours'])
        plt.title('Average Resolution Time by Priority')
        plt.xlabel('Priority')
        plt.ylabel('Hours')
        plt.tight_layout()
        plt.show()
    
    def plot_sla_analysis(self, df):
        """Plot SLA compliance analysis"""
        sla_by_priority = df.groupBy("priority", "sla_status") \
            .count() \
            .toPandas()
        
        priorities = sla_by_priority['priority'].unique()
        within_sla = [sla_by_priority[(sla_by_priority['priority'] == p) & 
                     (sla_by_priority['sla_status'] == 'Within SLA')]['count'].iloc[0] 
                     if len(sla_by_priority[(sla_by_priority['priority'] == p) & 
                     (sla_by_priority['sla_status'] == 'Within SLA')]) > 0 else 0 
                     for p in priorities]
        breached_sla = [sla_by_priority[(sla_by_priority['priority'] == p) & 
                       (sla_by_priority['sla_status'] == 'Breached SLA')]['count'].iloc[0]
                       if len(sla_by_priority[(sla_by_priority['priority'] == p) & 
                       (sla_by_priority['sla_status'] == 'Breached SLA')]) > 0 else 0 
                       for p in priorities]
        
        plt.figure(figsize=(10, 6))
        x = range(len(priorities))
        width = 0.35
        plt.bar([i - width/2 for i in x], within_sla, width, label='Within SLA')
        plt.bar([i + width/2 for i in x], breached_sla, width, label='Breached SLA')
        plt.xticks(x, priorities)
        plt.title('SLA Status by Priority')
        plt.xlabel('Priority')
        plt.ylabel('Count')
        plt.legend()
        plt.tight_layout()
        plt.show()
    
    def plot_hourly_pattern(self, df):
        """Plot hourly ticket pattern"""
        hourly_pattern = df.groupBy("creation_hour") \
            .count() \
            .toPandas() \
            .sort_values("creation_hour")
        
        plt.figure(figsize=(12, 6))
        plt.plot(hourly_pattern['creation_hour'], 
                hourly_pattern['count'], 
                marker='o')
        plt.title('Ticket Creation Pattern by Hour')
        plt.xlabel('Hour of Day')
        plt.ylabel('Number of Tickets')
        plt.grid(True)
        plt.tight_layout()
        plt.show()
    
    def generate_insights(self, df):
        """Generate key insights from the data"""
        # Overall metrics
        total_tickets = df.count()
        avg_resolution = df.filter(col("resolution_time_hours").isNotNull()) \
            .select(avg("resolution_time_hours")).first()[0]
        sla_breach_rate = df.filter(col("sla_status") == "Breached SLA").count() / total_tickets * 100
        avg_satisfaction = df.filter(col("satisfaction_score").isNotNull()) \
            .select(avg("satisfaction_score")).first()[0]
        
        print("=== Support Ticket Analysis Insights ===")
        print(f"\nTotal Tickets Analyzed: {total_tickets}")
        print(f"Average Resolution Time: {avg_resolution:.2f} hours")
        print(f"SLA Breach Rate: {sla_breach_rate:.2f}%")
        print(f"Average Customer Satisfaction: {avg_satisfaction:.2f}/5.0")
        
        # Top issues
        print("\nTop Issue Categories:")
        df.groupBy("issue_category") \
            .count() \
            .orderBy(col("count").desc()) \
            .show(5)
        
        # SLA breach analysis
        print("\nSLA Breach Analysis by Priority:")
        df.groupBy("priority") \
            .agg(
                count(when(col("sla_status") == "Breached SLA", 1)).alias("breached_sla_count"),
                (count(when(col("sla_status") == "Breached SLA", 1)) / count("*") * 100).alias("breach_rate")
            ) \
            .orderBy("priority") \
            .show()
    
    def run_analysis(self, data_path):
        """Run complete analysis"""
        df = self.load_data(data_path)
        
        # Generate visualizations
        self.plot_ticket_distribution(df)
        self.plot_resolution_times(df)
        self.plot_sla_analysis(df)
        self.plot_hourly_pattern(df)
        
        # Generate insights
        self.generate_insights(df)
        
        return df

if __name__ == "__main__":
    # Initialize Spark
    spark = SparkSession.builder \
        .appName("Support Ticket Analysis") \
        .master("local[*]") \
        .getOrCreate()
    
    # Run analysis
    analyzer = SupportAnalyzer(spark)
    analyzer.run_analysis("../data/processed/processed_tickets")
    
    # Stop Spark session
    spark.stop()