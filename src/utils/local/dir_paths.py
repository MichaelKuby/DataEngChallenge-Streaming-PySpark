import os


def get_data_dir(base_dir):
    return os.path.join(base_dir, "../data")


def get_json_dir(data_dir):
    return os.path.join(data_dir, "json_files")


def get_csv_dir(data_dir):
    return os.path.join(data_dir, "csv_files")


def get_parquet_dir(data_dir):
    return os.path.join(data_dir, "parquet_files")


def get_checkpoint_dir(data_dir, checkpoint_dir_name):
    return os.path.join(data_dir, checkpoint_dir_name)
