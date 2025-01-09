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
    st.sidebar.success("Select a page above")

    client_df = ut.read_parquet_pandas(minio_filesystem, "financials/data/presentation/client_summary")
    merchant_df = ut.read_parquet_pandas(minio_filesystem, "financials/data/presentation/merchant_summary")
    card_df = ut.read_parquet_pandas(minio_filesystem, "financials/data/presentation/card_summary")


    st.dataframe(client_df)
    # st.bar_chart(client_df, x="Client Age", y="AVG Transaction")
    # st.bar_chart(client_df, x="Year", y="AVG Transaction", color="Gender")
    st.dataframe(merchant_df)
    st.dataframe(card_df)

