import os
import streamlit as st
import s3fs
from dotenv import load_dotenv
import extract as ex


load_dotenv()




if __name__ == "__main__":
    minio_filesystem = s3fs.S3FileSystem(
        key=os.getenv("MINIO_ROOT_USER"),
        secret=os.getenv("MINIO_ROOT_PASSWORD"),
        client_kwargs={"endpoint_url": "http://127.0.0.1:9000"}
    )

    st.title("Card Transaction Analytics")

    client_df = ex.read_parquet_pandas(minio_filesystem, "financials/data/presentation/client_summary")
    merchant_df = ex.read_parquet_pandas(minio_filesystem, "financials/data/presentation/merchant_summary")
    card_df = ex.read_parquet_pandas(minio_filesystem, "financials/data/presentation/card_summary")

