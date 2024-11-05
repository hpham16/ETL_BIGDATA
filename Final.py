import findspark
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import os 
from datetime import datetime, timedelta
from pyspark.sql import functions as F
from pyspark.sql.window import Window


spark = SparkSession.builder.config("spark.driver.memory", "8g").getOrCreate()

#đọc data
def read_data(path):
    df = spark.read.json(path)
    return df

def frist_process(df, file):
    df = df.select('_source.*')
    df = df.drop('Mac')
    dated = convert_name_to_date(file)
    df = df.withColumn("date", lit(dated))  # Thêm cột 'date'
    return df

#Xử lý data
def tranfrom_data(df) :
    df = df.withColumn("Category",
        when((col("AppName") == 'CHANNEL') | 
            (col("AppName") =='KPLUS'), "TruyenHinhDuration")
        .when((col("AppName") == 'VOD') | 
            (col("AppName") =='FIMS'), "PhimDuration")
        .when((col("AppName") == 'RELAX'), "GiaiTriDuration")
        .when((col("AppName") == 'CHILD'), "ThieuNhiDuration")
        .when((col("AppName") == 'SPORT'), "TheThaoDuration")
        .otherwise("Error"))
    return df



#tổng hợp và pivot
def summarize_and_pivot_data(df):
    df = df.groupBy('Contract','Category','date').sum('TotalDuration').withColumnRenamed('sum(TotalDuration)','TotalDuration')
    result =  df.groupBy('Contract','date').pivot('Category').sum('TotalDuration').fillna(0)
    return result


def convert_to_datevalue(value):
    date_value = datetime.strptime(value, "%Y%m%d").date()
    return date_value

def date_range(start_date, end_date):
    date_list = []
    current_date = start_date
    while current_date <= end_date:
        date_list.append(current_date.strftime("%Y%m%d"))  # Trả về định dạng chuỗi YYYYMMDD
        current_date += timedelta(days=1)  # Sử dụng timedelta
    return date_list

def generate_date_range(from_date, to_date):
    from_date = convert_to_datevalue(from_date)
    to_date = convert_to_datevalue(to_date)
    date_list = date_range(from_date, to_date)
    return date_list

def convert_name_to_date(filename):
    # Chuyển đổi định dạng từ YYYYMMDD thành YYYY-MM-DD
    date_obj = datetime.strptime(filename, '%Y%m%d').date()  # Chuyển đổi thành đối tượng datetime
    # Chuyển đối tượng datetime thành chuỗi theo định dạng YYYY-MM-DD
    return date_obj.strftime('%Y-%m-%d')

#hàm đọc nhiều file và tổng hợp thành 1 files
def read_listfile(list_file,path):
    final_result = None
    for file in list_file:
        print(f"==> Reading file {file} <==")
        path_new = path + '\\' + file + '.json'
        data_now = read_data(path_new)
   
        data_dated = frist_process(data_now, file)

        # df_contracts = data_dated.select('Contract')
        # # Đếm số lượng Contract khác nhau
        # distinct_count = df_contracts.distinct().count()
        # print(f"Số lượng Contract khác nhau trong file: {distinct_count}")

        data_dated.show()
        
        if final_result is None:
            final_result = data_dated  # Gán giá trị lần đầu
        else:
            final_result = final_result.union(data_dated)  # Nối với DataFrame hiện tại

    return final_result

def process_mostwatch(df):
    df = df.withColumn("Most_watch",
        when(df.GiaiTriDuration == greatest(
            df.GiaiTriDuration, df.PhimDuration, df.ThieuNhiDuration, df.TheThaoDuration, df.TruyenHinhDuration)
            , "GiaiTriDuration")
        .when(df.PhimDuration == greatest(
            df.GiaiTriDuration, df.PhimDuration, df.ThieuNhiDuration, df.TheThaoDuration, df.TruyenHinhDuration)
            , "PhimDuration")
        .when(df.ThieuNhiDuration == greatest(
            df.GiaiTriDuration, df.PhimDuration, df.ThieuNhiDuration, df.TheThaoDuration, df.TruyenHinhDuration)
            , "ThieuNhiDuration")
        .when(df.TheThaoDuration == greatest(
            df.GiaiTriDuration, df.PhimDuration, df.ThieuNhiDuration, df.TheThaoDuration, df.TruyenHinhDuration)
            , "TheThaoDuration")
        .when(df.TruyenHinhDuration == greatest(
            df.GiaiTriDuration, df.PhimDuration, df.ThieuNhiDuration, df.TheThaoDuration, df.TruyenHinhDuration), 
            "TruyenHinhDuration")
        .otherwise(None)
    )
    df = df.withColumn("taste_customer",
        concat_ws(" - ",
            when(df.GiaiTriDuration > 0, "GT"),
            when(df.PhimDuration > 0, "PT"),
            when(df.TheThaoDuration > 0, "TT"),
            when(df.ThieuNhiDuration > 0, "TN"),
            when(df.TruyenHinhDuration > 0, "TH")
        )
    )

    
    active_df = df.groupBy("Contract").agg(countDistinct("date").alias("active"))
    print('------------------------')
    print('Data active')
    active_df.show()
    # Kết hợp lại với DataFrame gốc
    df = df.join(active_df, on="Contract", how="left")
    df = df.select('Contract','date','Most_watch','taste_customer','active')
    return df



#hàm xử lý các ngày theo hình thức 1
def process_range_days(df):
    print('------------------------')
    print('Starting tranform data')
    data = tranfrom_data(df)
    print('------------------------')
    print('Starting summarize and pivot data ')
    result = summarize_and_pivot_data(data)
    print('------------------------')
    print('Task successful')
    return result


#-----------------------------------Data Search-------------------------------
def read_par_data(path):
    df = spark.read.parquet(path)
    return df

def most_search(df):    
    df_keyword = df.select('user_id','keyword') # lấy ra cột user và key
    df_keyword = df_keyword.dropna(subset=["user_id", "keyword"]) # bỏ các cột null
    data = df_keyword.groupBy("user_id", "keyword").count() # tạo cột count và đếm số lần search
    data = data.withColumnRenamed("count", "search_count").orderBy("search_count") # đổi tên và sắp sếp theo cột đổi tên
    window = Window.partitionBy("user_id").orderBy(col("search_count").desc()) # phải tạo partition để tìm max của từng user trong df
    data_with_rank = data.withColumn("rank", row_number().over(window)) # tạo thêm 1 cột rank và sử dụng lấy rank theo partion đã tạo 
    data_filter = data_with_rank.filter((col('rank') == 1) & (col('search_count') > 1)).orderBy("search_count", ascending=False) # lấy ra các cột có rank = 1
    data_filter = data_filter.select('user_id', 'keyword')
    data_filter = data_filter.withColumnRenamed('keyword','most_searched')
    return data_filter

#đánh nhãn ngẫu nhiên vì đánh nhãn chính xác ko có thời gian
def add_label(data_filter):
    categories = ['Âm nhạc', 'Giải trí', 'Phim', 'Thể thao', 'Nghệ thuật', 'Không xác định']
    data_filter = data_filter.withColumn("category", 
                    when(rand() < 1/6, categories[0])
                    .when(rand() < 2/6, categories[1])
                    .when(rand() < 3/6, categories[2])
                    .when(rand() < 4/6, categories[3])
                    .when(rand() < 5/6, categories[4])
                    .otherwise(categories[5]))
    return data_filter

def merce_data_month(data1,data2):
    data1 = data1.withColumnRenamed("most_searched", "most_searched_t6") \
                 .withColumnRenamed("category", "category_t6")
    data2 = data2.withColumnRenamed("most_searched", "most_searched_t7") \
                 .withColumnRenamed("category", "category_t7")
    df = data1.join(data2, on = "user_id", how='inner')
    df = df.withColumn("Trending_Type", 
                       when(col('category_t6') == col('category_t7'), "Unchanged") \
                       .otherwise("Changed"))
    df = df.withColumn("Previous",
                       when(col('Trending_Type') == "Unchanged", "Unchanged") \
                       .otherwise(concat_ws(" - ",
                                (df.category_t6),
                                (df.category_t7),
        ))
    )
    return df

def process_search_data(path):    
    df = read_par_data(path)
    df = most_search(df)
    df = add_label(df)
    return df


if __name__ == "__main__":
    findspark.init()

    # Kết nối với PostgresSQL
    jdbc_url = "jdbc:postgresql://127.0.0.1:5432/OLAP_data"
    jdbc_properties = {
    "user": "postgres",
    "password": "1",
    "driver": "org.postgresql.Driver"
    }
    #---------------------------------------------Xử lý content data---------------------------------
    path = 'D:\\Hoc_DE\\Data\\Dataset\\log_content'
    list_file = generate_date_range('20220401', '20220403')
    count_list = len(list_file)

    print(f"==> Starting read {count_list} file <==")
    print('------------------------')
    df = read_listfile(list_file, path)

    final_result = process_range_days(df)
    final_result.show()
    num_rows_final = final_result.count()
    print(f"==> Number of rows after process: {num_rows_final} <==")

    df_mostwatch = process_mostwatch(final_result)
    df_mostwatch.show()

    print("Starting write content data to PostgreSQL...............")
    print('------------------------')
    df_mostwatch.write.jdbc(
    url=jdbc_url,
    table="contracts",    # Tên bảng trong cơ sở dữ liệu
    mode="append",           # Chế độ ghi: 'append', 'overwrite', 'ignore', hoặc 'error'
    properties=jdbc_properties
    )

    print("Save content data to PostgreSQL SUCCESSFUL!")
    print('------------------------')
    #---------------------------------------------Xử lý search data---------------------------------

    path_t6 = "D:\\Hoc_DE\\Data\\Dataset\\log_search\\20220601"
    path_t7 = "D:\\Hoc_DE\\Data\\Dataset\\log_search\\20220701"

    df_t6 = process_search_data(path_t6)
    df_t7 = process_search_data(path_t7)
    
    data_mer = merce_data_month(df_t6,df_t7)
    data_mer.show()

    print("Starting write SEARCH data to PostgreSQL...............")
    print('------------------------')
    data_mer.write.jdbc(
    url=jdbc_url,
    table="user_searches",
    mode="append",      
    properties=jdbc_properties
    )
    
    print("Save SEARCH data to PostgreSQL SUCCESSFUL!")
    print('------------------------')
