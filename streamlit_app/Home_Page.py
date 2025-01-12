import os
import streamlit as st
import s3fs
from dotenv import load_dotenv
import utils as ut


load_dotenv()


if __name__ == "__main__":
    minio_filesystem = s3fs.S3FileSystem(
        key=os.getenv("MINIO_ROOT_USER"),
        secret=os.getenv("MINIO_ROOT_PASSWORD"),
        client_kwargs={"endpoint_url": "http://127.0.0.1:9000"}
    )

    st.title("Card Transaction Analytics")

    client_df = ut.read_parquet_pandas(minio_filesystem, "financials/data/presentation/client_summary")
    merchant_df = ut.read_parquet_pandas(minio_filesystem, "financials/data/presentation/merchant_summary")
    card_df = ut.read_parquet_pandas(minio_filesystem, "financials/data/presentation/card_summary")
    yoy_growth = ut.read_parquet_pandas(minio_filesystem, "financials/data/presentation/yoy_growth")
    client_recency = ut.read_parquet_pandas(minio_filesystem, "financials/data/presentation/client_recency")
    st.write("Year to Year Growth Analysis")
    st.dataframe(yoy_growth.style.format({"YoY Growth": "{:.2%}"}))
    st.write(
        f"Total spent grouped by client age"
    )
    st.bar_chart(client_df, x="Client Age", y="Total Spent")
    st.write("Total spent by year grouped by gender")
    st.bar_chart(client_df, x="Year", y="Total Spent", color="Gender")
