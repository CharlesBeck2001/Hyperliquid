#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Dec 25 12:20:09 2024

@author: charlesbeck
"""

import pandas as pd
import matplotlib.pyplot as plt
import mysql.connector
import streamlit as st
import requests

#ip = requests.get('https://api.ipify.org').text
#st.write(f"Your Streamlit app's public IP is: {ip}")
# Database connection
conn = mysql.connector.connect(
    host = st.secrets["host"],
    user = st.secrets["user"],
    password = st.secrets["password"],
    database = st.secrets["database"],
    port = st.secrets["port"]
)

# Create a cursor object to interact with the database
cursor = conn.cursor()

# Define the asset variable
asset = 'ZRO'

# Create a query to fetch distinct users
cvf_table_query = """
WITH LogScaledTrades AS (
    SELECT
        volume,
        LOG(10, volume) AS log_volume,
        SUM(volume) OVER (ORDER BY volume) AS cumulative_volume
    FROM hyperfluid_trades
    WHERE asset = %s -- Parameterized placeholder for the asset variable
    ORDER BY log_volume ASC
),
TotalVolume AS (
    SELECT SUM(volume) AS total_volume
    FROM hyperfluid_trades
    WHERE asset = %s -- Parameterized placeholder for the asset variable
),
CumulativePercentages AS (
    SELECT
        log_volume,
        cumulative_volume,
        (SELECT total_volume FROM TotalVolume) AS total_volume,
        cumulative_volume / (SELECT total_volume FROM TotalVolume) AS cumulative_percent
    FROM LogScaledTrades
),
Bins AS (
    SELECT
        log_volume,
        cumulative_percent,
        NTILE(2500) OVER (ORDER BY cumulative_percent) AS bin
    FROM CumulativePercentages
),
RankedBins AS (
    SELECT
        log_volume,
        cumulative_percent,
        bin,
        ROW_NUMBER() OVER (PARTITION BY bin ORDER BY log_volume DESC) AS row_num
    FROM Bins
)
SELECT
    log_volume,
    cumulative_percent
FROM RankedBins
WHERE row_num = 1
ORDER BY bin;
"""


# Create a query to fetch distinct users
cdf_table_query = """
WITH LogScaledTrades AS (
    SELECT
        volume,
        LOG(10, volume) AS log_volume,
        COUNT(trade_id) OVER (ORDER BY volume) AS cumulative_trades
    FROM hyperfluid_trades
    WHERE asset = %s -- Parameterized placeholder for the asset variable
    ORDER BY log_volume ASC
    
),
TotalTrades AS (
    SELECT COUNT(trade_id) AS total_trades
    FROM hyperfluid_trades
    WHERE asset = %s -- Parameterized placeholder for the asset variable
),
CumulativePercentages AS (
    SELECT
        log_volume,
        cumulative_trades,
        (SELECT total_trades FROM TotalTrades) AS total_trades,
        cumulative_trades / (SELECT total_trades FROM TotalTrades) AS cumulative_percent
    FROM LogScaledTrades
),
Bins AS (
    SELECT
        log_volume,
        cumulative_percent,
        NTILE(2500) OVER (ORDER BY cumulative_percent) AS bin
    FROM CumulativePercentages
),
RankedBins AS (
    SELECT
        log_volume,
        cumulative_percent,
        bin,
        ROW_NUMBER() OVER (PARTITION BY bin ORDER BY log_volume DESC) AS row_num
    FROM Bins
)
SELECT
    log_volume,
    cumulative_percent
FROM RankedBins
WHERE row_num = 1
ORDER BY bin;
"""


cvf_table_query_total = """
WITH LogScaledTrades AS (
    SELECT
        volume,
        LOG(10, volume) AS log_volume,
        SUM(volume) OVER (ORDER BY volume) AS cumulative_volume
    FROM hyperfluid_trades
    ORDER BY log_volume ASC
),
TotalVolume AS (
    SELECT SUM(volume) AS total_volume
    FROM hyperfluid_trades
),
CumulativePercentages AS (
    SELECT
        log_volume,
        cumulative_volume,
        (SELECT total_volume FROM TotalVolume) AS total_volume,
        cumulative_volume / (SELECT total_volume FROM TotalVolume) AS cumulative_percent
    FROM LogScaledTrades
),
Bins AS (
    SELECT
        log_volume,
        cumulative_percent,
        NTILE(2500) OVER (ORDER BY cumulative_percent) AS bin
    FROM CumulativePercentages
),
RankedBins AS (
    SELECT
        log_volume,
        cumulative_percent,
        bin,
        ROW_NUMBER() OVER (PARTITION BY bin ORDER BY log_volume DESC) AS row_num
    FROM Bins
)
SELECT
    log_volume,
    cumulative_percent
FROM RankedBins
WHERE row_num = 1
ORDER BY bin;
"""


# Create a query to fetch distinct users
cdf_table_query_total = """
WITH LogScaledTrades AS (
    SELECT
        volume,
        LOG(10, volume) AS log_volume,
        COUNT(trade_id) OVER (ORDER BY volume) AS cumulative_trades
    FROM hyperfluid_trades
    ORDER BY log_volume ASC
    
),
TotalTrades AS (
    SELECT COUNT(trade_id) AS total_trades
    FROM hyperfluid_trades
),
CumulativePercentages AS (
    SELECT
        log_volume,
        cumulative_trades,
        (SELECT total_trades FROM TotalTrades) AS total_trades,
        cumulative_trades / (SELECT total_trades FROM TotalTrades) AS cumulative_percent
    FROM LogScaledTrades
),
Bins AS (
    SELECT
        log_volume,
        cumulative_percent,
        NTILE(2500) OVER (ORDER BY cumulative_percent) AS bin
    FROM CumulativePercentages
),
RankedBins AS (
    SELECT
        log_volume,
        cumulative_percent,
        bin,
        ROW_NUMBER() OVER (PARTITION BY bin ORDER BY log_volume DESC) AS row_num
    FROM Bins
)
SELECT
    log_volume,
    cumulative_percent
FROM RankedBins
WHERE row_num = 1
ORDER BY bin;
"""

# Query to find the top 5 assets by trade count
top_assets_query = """
SELECT asset
FROM hyperfluid_trades
GROUP BY asset
ORDER BY COUNT(trade_id) DESC
LIMIT 5;
"""

cursor.execute(top_assets_query)
default_assets = [row[0] for row in cursor.fetchall()] + ['Total']

# Streamlit app
st.title("Hyperliquid CVF and CDF Visualization")

# User input for asset selection
cursor.execute("SELECT DISTINCT asset FROM hyperfluid_trades")
assets = [row[0] for row in cursor.fetchall()] + ['Total']
# Set the default assets as the top 5 most traded assets
selected_assets = st.multiselect(
    "Select assets to visualize",
    options=assets,
    default=default_assets  # Default is set to the top 5 assets by trade count
)

if selected_assets:
    cvf_data = []
    cdf_data = []
    
    for asset in selected_assets:
        # Fetch CVF data
        if asset == 'Total':
            
            cursor.execute(cvf_table_query_total)
            cvf_results = cursor.fetchall()
            df_cvf = pd.DataFrame(cvf_results, columns=["log_volume", "cumulative_percent"])
            df_cvf = df_cvf[df_cvf['log_volume'] > 0]
            df_cvf['asset'] = asset
            cvf_data.append(df_cvf)

            # Fetch CDF data
            cursor.execute(cdf_table_query_total)
            cdf_results = cursor.fetchall()
            df_cdf = pd.DataFrame(cdf_results, columns=["log_volume", "cumulative_percent"])
            df_cdf = df_cdf[df_cdf['log_volume'] > 0]
            df_cdf['asset'] = asset
            
            df_cdf['log_volume'] = pd.to_numeric(df_cdf['log_volume'], errors='coerce')
            df_cdf['cumulative_percent'] = pd.to_numeric(df_cdf['cumulative_percent'], errors='coerce')
            df_cdf = df_cdf.dropna(subset=["log_volume", "cumulative_percent"])
            if not df_cdf.empty:
                cdf_data.append(df_cdf)
        else:
            cursor.execute(cvf_table_query, (asset, asset))
            cvf_results = cursor.fetchall()
            df_cvf = pd.DataFrame(cvf_results, columns=["log_volume", "cumulative_percent"])
            df_cvf = df_cvf[df_cvf['log_volume'] > 0]
            df_cvf['asset'] = asset
            cvf_data.append(df_cvf)

            # Fetch CDF data
            cursor.execute(cdf_table_query, (asset, asset))
            cdf_results = cursor.fetchall()
            df_cdf = pd.DataFrame(cdf_results, columns=["log_volume", "cumulative_percent"])
            df_cdf = df_cdf[df_cdf['log_volume'] > 0]
            df_cdf['asset'] = asset
            
            df_cdf['log_volume'] = pd.to_numeric(df_cdf['log_volume'], errors='coerce')
            df_cdf['cumulative_percent'] = pd.to_numeric(df_cdf['cumulative_percent'], errors='coerce')
            df_cdf = df_cdf.dropna(subset=["log_volume", "cumulative_percent"])
            if not df_cdf.empty:
                cdf_data.append(df_cdf)
        
    # Combine data for plotting
    cvf_combined = pd.concat(cvf_data)
    cdf_combined = pd.concat(cdf_data)
    
    
    st.subheader(f"CVF for Selected Assets") 
    # Plot CVFs
    st.line_chart(
        data=cvf_combined,
        x="log_volume",
        y="cumulative_percent",
        color="asset"
    )
    st.subheader(f"CDF for Selected Assets") 
    # Plot CDFs
    st.line_chart(
        data=cdf_combined,
        x="log_volume",
        y="cumulative_percent",
        color="asset"
    )
else:
    st.write("Please select at least one asset.")




