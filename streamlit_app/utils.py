import pandas as pd
import s3fs


def read_parquet_pandas(file_system: s3fs.S3FileSystem, folder_path: str) -> pd.DataFrame:
    """
    Reads parquet files from Minio bucket and return as a pandas DataFrame.
    :param file_system: S3FileSystem instance.
    :param folder_path: Absolute path to source data.
    :return: Pandas DataFrame.
    """
    file_list = file_system.glob(f"{folder_path}/*.parquet")
    dfs = []
    for file in file_list:
        full_path = f"s3://{file}"
        df = pd.read_parquet(
            full_path,
            storage_options={
                "key": file_system.key,
                "secret": file_system.secret,
                "client_kwargs": file_system.client_kwargs,
            }
        )
        dfs.append(df)
    combined_df = pd.concat(dfs, ignore_index=True)
    return combined_df
