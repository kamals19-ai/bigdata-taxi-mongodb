import pandas as pd
import streamlit as st
import matplotlib.pyplot as plt

from src.utils.db import get_collection

st.set_page_config(page_title="Taxi Gold Dashboard", layout="wide")

st.title("NYC Taxi Dashboard (Gold Layer: agg_trips)")
st.caption("All charts are generated from MongoDB Atlas aggregated collection: agg_trips")

# Load aggregated data
agg = get_collection("agg_trips")
docs = list(agg.find({}, {"_id": 0}))

if not docs:
    st.error("No documents found in agg_trips. Run the aggregation step first.")
    st.stop()

df = pd.DataFrame(docs)

required_cols = {"month", "VendorID", "trip_count", "avg_fare", "avg_tip", "total_revenue"}
missing = required_cols - set(df.columns)
if missing:
    st.error(f"Missing required fields: {missing}")
    st.stop()

df = df.sort_values("month")

# Sidebar filters
st.sidebar.header("Filters")
months = sorted(df["month"].unique())
vendors = sorted(df["VendorID"].unique())

selected_months = st.sidebar.multiselect("Month(s)", months, default=list(months))
selected_vendors = st.sidebar.multiselect("VendorID(s)", vendors, default=list(vendors))

filtered = df[
    df["month"].isin(selected_months)
    & df["VendorID"].isin(selected_vendors)
]

st.subheader("Aggregated Data Preview")
st.dataframe(filtered, use_container_width=True)

# Visualization 1: Trips by month
st.subheader("1) Trip Count by Month")
fig1 = plt.figure()
filtered.groupby("month")["trip_count"].sum().plot(kind="bar")
plt.xlabel("Month")
plt.ylabel("Trip Count")
plt.title("Trips by Month")
st.pyplot(fig1)

# Visualization 2: Revenue by vendor
st.subheader("2) Total Revenue by Vendor")
fig2 = plt.figure()
filtered.groupby("VendorID")["total_revenue"].sum().plot(kind="bar")
plt.xlabel("VendorID")
plt.ylabel("Total Revenue")
plt.title("Revenue by Vendor")
st.pyplot(fig2)

# Visualization 3: Average fare trend
st.subheader("3) Average Fare by Month")
fig3 = plt.figure()
filtered.groupby("month")["avg_fare"].mean().plot(marker="o")
plt.xlabel("Month")
plt.ylabel("Average Fare")
plt.title("Average Fare Trend")
st.pyplot(fig3)

st.success("Dashboard loaded successfully from MongoDB.")
