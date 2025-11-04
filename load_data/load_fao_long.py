import os
from load_data.load_utils import load_to_bigquery

if __name__ == "__main__":
    parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    load_path = os.path.join(parent_dir, "output_data", "fao_long_cleaned")

    load_to_bigquery(
        table_name="data_fao_long",
        source_folder=load_path,
        dataset="fao_dataset"
    )
