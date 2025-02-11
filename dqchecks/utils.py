from pyspark.sql import SparkSession

def simple_hdfs_ls(path: str) -> list:
    import datetime
    spark = SparkSession.builder.appName("spark_entry_job").getOrCreate()
    jvm = spark.sparkContext._jvm
    fs_root = jvm.java.net.URI.create("")
    conf = spark.sparkContext._jsc.hadoopConfiguration()
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(fs_root, conf)

    path_glob = jvm.org.apache.hadoop.fs.Path(path)
    status_list = fs.globStatus(path_glob)
    
    # Generate a list of tuples with the file path and last modification time
    file_info = []
    for status in status_list:
        file_path = status.getPath().toString()
        last_modified_time = status.getModificationTime()  # Get last modified time in milliseconds
        # Convert last modified time from milliseconds to a readable format
        if isinstance(last_modified_time, float) or isinstance(last_modified_time, int):
            last_modified_datetime = datetime.datetime.fromtimestamp(last_modified_time / 1000.0)
        else:
            last_modified_datetime = last_modified_time
        new_val = {"name": file_path, "last_modified": last_modified_datetime}
        if new_val not in file_info:
            file_info.append(new_val)
    return file_info
