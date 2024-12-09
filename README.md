# AWS Support Ticket Analytics Dashboard

A data engineering project that processes and visualizes AWS support ticket data using PySpark and Streamlit.

## Features
- ETL pipeline for support ticket data processing
- Real-time analytics dashboard
- SLA monitoring and compliance tracking
- Performance metrics visualization

## Tech Stack
- PySpark for data processing
- Streamlit for visualization
- Pandas for data manipulation
- Plotly for interactive charts

## Setup
1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Generate sample data:
```bash
python scripts/generate_data.py
```

3. Run ETL pipeline:
```bash
python scripts/etl_pipeline.py
```

4. Launch dashboard:
```bash
streamlit run scripts/dashboard.py
```

## Live App
[Interactive Dashboard](https://csanalytics-aws-support-metrics.streamlit.app/)