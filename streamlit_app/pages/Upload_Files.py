import os
import tempfile
import streamlit as st
from dotenv import load_dotenv
from minio import Minio

load_dotenv()


def minio_upload_data(bucket_name, source_path, destination_path):
    minio_client = Minio(
        os.getenv("NODE_IP"),  # Replace with any node's IP and port
        access_key=os.getenv("MINIO_ROOT_USER"),
        secret_key=os.getenv("MINIO_ROOT_PASSWORD"),
        secure=False
    )
    found = minio_client.bucket_exists(bucket_name)
    if not found:
        minio_client.make_bucket(bucket_name)
        print("Created bucket", bucket_name)
    else:
        print("Bucket", bucket_name, "already exists")

    minio_client.fput_object(bucket_name, destination_path, source_path)
    print(f"Uploaded {source_path} to MinIO bucket {bucket_name}")

if __name__ == "__main__":
    st.title("Upload Files")

    data_type = st.selectbox(
        "Choose data label", ["Transaction Data", "Card Data", "Client Data"]
    )

    if data_type is not None:
        uploaded_file = st.file_uploader("Upload transaction file", type="csv")
        if uploaded_file is not None:
            with tempfile.NamedTemporaryFile(delete=False) as temp_file:
                temp_file.write(uploaded_file.getvalue())
                temp_file_path = temp_file.name
            match data_type:
                case "Transaction Data":
                    minio_upload_data(
                        "financials", temp_file_path, "data/raw/test_data.csv"
                    )
                case "Card Data":
                    minio_upload_data(
                        "financials", temp_file_path, "data/raw/test_data.csv"
                    )
            st.write("File uploaded!")