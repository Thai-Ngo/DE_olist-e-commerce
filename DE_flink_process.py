import glob, datetime, os

from pyflink.common.serialization import Encoder
from pyflink.common.typeinfo import Types
from pyflink.common import WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.file_system import (FileSource, StreamFormat, FileSink,
                                                       OutputFileConfig, RollingPolicy)

def customers_data_process():
    print("Extract customers data from raw olist data!")
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
 
    ds = env.from_source(
        source=FileSource.for_record_stream_format(
            StreamFormat.text_line_format(),
            "/home/thai/Documents/WORK_SPACE/DE_DATASET/raw_olist_data.csv"
        ).process_static_file_set().build(),
        watermark_strategy=WatermarkStrategy.for_monotonous_timestamps(),
        source_name="file_source"
    )

    ds = ds.map(lambda i: ','.join(i.split(',')[0:5]), output_type=Types.STRING())
    
    ds.sink_to(
        sink=FileSink.for_row_format(
            base_path="/home/thai/Documents/WORK_SPACE/DE_OUTPUT/customers_dir",
            encoder=Encoder.simple_string_encoder()
        ).with_output_file_config(
            OutputFileConfig.builder()
                .with_part_prefix("prefix")
                .with_part_suffix(".ext")
                .build()
        ).with_rolling_policy(RollingPolicy.default_rolling_policy()
        ).build()
    )
    
    env.execute("customers_data_process")
    
    local_out_file = f"/home/thai/Documents/WORK_SPACE/DE_OUTPUT/customers_dir/flink.customers_dataset.csv"
    if os.path.exists(local_out_file):
        os.remove(local_out_file)
        
    flink_out_file = glob.glob(f"/home/thai/Documents/WORK_SPACE/DE_OUTPUT/customers_dir/{datetime.datetime.now().strftime('%Y-%m-%d--%H')}/prefix-*.ext")[0]
    os.rename(flink_out_file, local_out_file)

def orders_data_process():
    DE_typ = 'orders'
    
    print(f"Extract {DE_typ} data from raw olist data!")
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
 
    ds = env.from_source(
        source=FileSource.for_record_stream_format(
            StreamFormat.text_line_format(),
            "/home/thai/Documents/WORK_SPACE/DE_DATASET/raw_olist_data.csv"
        ).process_static_file_set().build(),
        watermark_strategy=WatermarkStrategy.for_monotonous_timestamps(),
        source_name="file_source"
    )
    
    def preprocessing(line):
        list = line.split(',')
        if list[9] == "NULL":
            list[9] = ""
        if list[10] == "NULL":
            list[10] = ""
        if list[11] == "NULL":
            list[11] = ""
        
        return ','.join([list[5]] + [list[0]] + list[7:13])
        

    ds = ds.map(preprocessing, output_type=Types.STRING())
    
    ds.sink_to(
        sink=FileSink.for_row_format(
            base_path=f"/home/thai/Documents/WORK_SPACE/DE_OUTPUT/{DE_typ}_dir",
            encoder=Encoder.simple_string_encoder()
        ).with_output_file_config(
            OutputFileConfig.builder()
                .with_part_prefix("prefix")
                .with_part_suffix(".ext")
                .build()
        ).with_rolling_policy(RollingPolicy.default_rolling_policy()
        ).build()
    )
    
    env.execute(f"{DE_typ}_data_process")
    
    local_out_file = f"/home/thai/Documents/WORK_SPACE/DE_OUTPUT/{DE_typ}_dir/flink.{DE_typ}_dataset.csv"
    if os.path.exists(local_out_file):
        os.remove(local_out_file)
        
    flink_out_file = glob.glob(f"/home/thai/Documents/WORK_SPACE/DE_OUTPUT/{DE_typ}_dir/{datetime.datetime.now().strftime('%Y-%m-%d--%H')}/prefix-*.ext")[0]
    os.rename(flink_out_file, local_out_file)

def order_items_data_process():
    DE_typ = 'order_items'
    
    print(f"Extract {DE_typ} data from raw olist data!")
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
 
    ds = env.from_source(
        source=FileSource.for_record_stream_format(
            StreamFormat.text_line_format(),
            "/home/thai/Documents/WORK_SPACE/DE_DATASET/raw_olist_data.csv"
        ).process_static_file_set().build(),
        watermark_strategy=WatermarkStrategy.for_monotonous_timestamps(),
        source_name="file_source"
    )
    
    def preprocessing(line):
        list = line.split(',')
        
        return ','.join([list[5]] + list[14:20])
        

    ds = ds.map(preprocessing, output_type=Types.STRING())
    
    ds.sink_to(
        sink=FileSink.for_row_format(
            base_path=f"/home/thai/Documents/WORK_SPACE/DE_OUTPUT/{DE_typ}_dir",
            encoder=Encoder.simple_string_encoder()
        ).with_output_file_config(
            OutputFileConfig.builder()
                .with_part_prefix("prefix")
                .with_part_suffix(".ext")
                .build()
        ).with_rolling_policy(RollingPolicy.default_rolling_policy()
        ).build()
    )
    
    env.execute(f"{DE_typ}_data_process")
    
    local_out_file = f"/home/thai/Documents/WORK_SPACE/DE_OUTPUT/{DE_typ}_dir/flink.{DE_typ}_dataset.csv"
    if os.path.exists(local_out_file):
        os.remove(local_out_file)
        
    flink_out_file = glob.glob(f"/home/thai/Documents/WORK_SPACE/DE_OUTPUT/{DE_typ}_dir/{datetime.datetime.now().strftime('%Y-%m-%d--%H')}/prefix-*.ext")[0]
    os.rename(flink_out_file, local_out_file)

def order_payments_data_process():
    DE_typ = 'order_payments'
    
    print(f"Extract {DE_typ} data from raw olist data!")
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
 
    ds = env.from_source(
        source=FileSource.for_record_stream_format(
            StreamFormat.text_line_format(),
            "/home/thai/Documents/WORK_SPACE/DE_DATASET/raw_olist_data.csv"
        ).process_static_file_set().build(),
        watermark_strategy=WatermarkStrategy.for_monotonous_timestamps(),
        source_name="file_source"
    )
    
    def preprocessing(line):
        list = line.split(',')
        
        return ','.join([list[5]] + list[21:25])
        

    ds = ds.map(preprocessing, output_type=Types.STRING())
    
    ds.sink_to(
        sink=FileSink.for_row_format(
            base_path=f"/home/thai/Documents/WORK_SPACE/DE_OUTPUT/{DE_typ}_dir",
            encoder=Encoder.simple_string_encoder()
        ).with_output_file_config(
            OutputFileConfig.builder()
                .with_part_prefix("prefix")
                .with_part_suffix(".ext")
                .build()
        ).with_rolling_policy(RollingPolicy.default_rolling_policy()
        ).build()
    )
    
    env.execute(f"{DE_typ}_data_process")
    
    local_out_file = f"/home/thai/Documents/WORK_SPACE/DE_OUTPUT/{DE_typ}_dir/flink.{DE_typ}_dataset.csv"
    if os.path.exists(local_out_file):
        os.remove(local_out_file)
        
    flink_out_file = glob.glob(f"/home/thai/Documents/WORK_SPACE/DE_OUTPUT/{DE_typ}_dir/{datetime.datetime.now().strftime('%Y-%m-%d--%H')}/prefix-*.ext")[0]
    os.rename(flink_out_file, local_out_file)

def order_reviews_data_process():
    print("Extract order reviews data from simple olist data!")
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
 
    ds = env.from_source(
        source=FileSource.for_record_stream_format(
            StreamFormat.text_line_format(),
            "/home/thai/Documents/WORK_SPACE/DE_DATASET/olist_order_reviews_dataset.csv"
        ).process_static_file_set().build(),
        watermark_strategy=WatermarkStrategy.for_monotonous_timestamps(),
        source_name="file_source"
    )
    
    ds.sink_to(
        sink=FileSink.for_row_format(
            base_path="/home/thai/Documents/WORK_SPACE/DE_OUTPUT/order_reviews_dir",
            encoder=Encoder.simple_string_encoder()
        ).with_output_file_config(
            OutputFileConfig.builder()
                .with_part_prefix("prefix")
                .with_part_suffix(".ext")
                .build()
        ).with_rolling_policy(RollingPolicy.default_rolling_policy()
        ).build()
    )
    
    env.execute("order_reviews_data_process")
    
    local_out_file = f"/home/thai/Documents/WORK_SPACE/DE_OUTPUT/order_reviews_dir/flink.order_reviews_dataset.csv"
    if os.path.exists(local_out_file):
        os.remove(local_out_file)
        
    flink_out_file = glob.glob(f"/home/thai/Documents/WORK_SPACE/DE_OUTPUT/order_reviews_dir/{datetime.datetime.now().strftime('%Y-%m-%d--%H')}/prefix-*.ext")[0]
    os.rename(flink_out_file, local_out_file)
    
def products_data_process():
    print("Extract products data from simple olist data!")
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
 
    ds = env.from_source(
        source=FileSource.for_record_stream_format(
            StreamFormat.text_line_format(),
            "/home/thai/Documents/WORK_SPACE/DE_DATASET/olist_products_dataset.csv"
        ).process_static_file_set().build(),
        watermark_strategy=WatermarkStrategy.for_monotonous_timestamps(),
        source_name="file_source"
    )
    
    def preprocessing(line):
        list = line.split(',')
        if list[1] == "":
            list[1] = "N/A"
        if list[2] == "":
            list[2] = "0"
        if list[3] == "":
            list[3] = "0"
        if list[4] == "":
            list[4] = "0"
        if list[5] == "":
            list[5] = "0"
        if list[6] == "":
            list[6] = "0"
        if list[7] == "":
            list[7] = "0"
        if list[8] == "":
            list[8] = "0"
        
        return ','.join(list)
    
    ds = ds.map(preprocessing, output_type=Types.STRING())
    
    ds.sink_to(
        sink=FileSink.for_row_format(
            base_path="/home/thai/Documents/WORK_SPACE/DE_OUTPUT/products_dir",
            encoder=Encoder.simple_string_encoder()
        ).with_output_file_config(
            OutputFileConfig.builder()
                .with_part_prefix("prefix")
                .with_part_suffix(".ext")
                .build()
        ).with_rolling_policy(RollingPolicy.default_rolling_policy()
        ).build()
    )
    
    env.execute("products_data_process")
    
    local_out_file = f"/home/thai/Documents/WORK_SPACE/DE_OUTPUT/products_dir/flink.products_dataset.csv"
    if os.path.exists(local_out_file):
        os.remove(local_out_file)
        
    flink_out_file = glob.glob(f"/home/thai/Documents/WORK_SPACE/DE_OUTPUT/products_dir/{datetime.datetime.now().strftime('%Y-%m-%d--%H')}/prefix-*.ext")[0]
    os.rename(flink_out_file, local_out_file)
    
if __name__ == '__main__':
    customers_data_process()
    orders_data_process()
    order_items_data_process()
    order_payments_data_process()
    order_reviews_data_process()
    products_data_process()