import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import os

# Cache the data loading
@st.cache_data
def load_data():
    """Load processed data from parquet files using pandas"""
    import pandas as pd
    try:
        # Adjust the path as needed
        return pd.read_parquet("../data/processed/processed_tickets")
    except Exception as e:
        st.error(f"Error loading data: {e}")
        return None

def main():
    st.set_page_config(page_title="Support Ticket Analytics", layout="wide")
    
    # Header
    st.title("AWS Support Ticket Analytics Dashboard")
    st.write("Real-time analytics for support ticket monitoring and analysis")
    
    # Load data using pandas instead of Spark
    df = load_data()
    
    if df is None:
        st.error("Failed to load data. Please check the data path and file existence.")
        return
    
    # Sidebar filters
    st.sidebar.header("Filters")
    
    # Priority filter
    priorities = df['priority'].unique().tolist()
    selected_priorities = st.sidebar.multiselect("Priority", priorities, default=priorities)
    
    # Filter data based on selection
    if selected_priorities:
        filtered_df = df[df['priority'].isin(selected_priorities)]
    else:
        filtered_df = df
    
    # Top metrics
    st.header("Key Metrics")
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        total_tickets = len(filtered_df)
        st.metric("Total Tickets", total_tickets)
    
    with col2:
        avg_resolution = filtered_df['resolution_time_hours'].mean()
        st.metric("Avg Resolution Time (hrs)", f"{avg_resolution:.1f}")
    
    with col3:
        sla_breach_rate = (filtered_df['sla_status'].eq('Breached SLA').mean() * 100)
        st.metric("SLA Breach Rate", f"{sla_breach_rate:.1f}%")
    
    with col4:
        avg_satisfaction = filtered_df['satisfaction_score'].mean()
        st.metric("Avg Satisfaction", f"{avg_satisfaction:.2f}/5.0")
    
    # Charts
    st.header("Ticket Analysis")
    
    # Row 1: Category and Priority Distribution
    col1, col2 = st.columns(2)
    
    with col1:
        # Category Distribution
        category_dist = filtered_df['issue_category'].value_counts().reset_index()
        category_dist.columns = ['issue_category', 'count']
        
        fig_category = px.bar(category_dist, 
                            x='count', 
                            y='issue_category',
                            title='Tickets by Category',
                            orientation='h')
        st.plotly_chart(fig_category, use_container_width=True)
    
    with col2:
        # Priority Distribution
        priority_dist = filtered_df['priority'].value_counts().reset_index()
        priority_dist.columns = ['priority', 'count']
        
        fig_priority = px.pie(priority_dist,
                            values='count',
                            names='priority',
                            title='Tickets by Priority')
        st.plotly_chart(fig_priority, use_container_width=True)
    
    # Row 2: Resolution Time and SLA Analysis
    col1, col2 = st.columns(2)
    
    with col1:
        # Resolution Time by Priority
        resolution_by_priority = filtered_df.groupby('priority')['resolution_time_hours'].mean().reset_index()
        
        fig_resolution = px.bar(resolution_by_priority,
                              x='priority',
                              y='resolution_time_hours',
                              title='Average Resolution Time by Priority')
        st.plotly_chart(fig_resolution, use_container_width=True)
    
    with col2:
        # SLA Status by Priority
        sla_by_priority = filtered_df.groupby(['priority', 'sla_status']).size().reset_index(name='count')
        
        fig_sla = px.bar(sla_by_priority,
                        x='priority',
                        y='count',
                        color='sla_status',
                        title='SLA Status by Priority',
                        barmode='group')
        st.plotly_chart(fig_sla, use_container_width=True)
    
    # Detailed Data View
    st.header("Detailed Data View")
    st.dataframe(filtered_df)

if __name__ == "__main__":
    main()