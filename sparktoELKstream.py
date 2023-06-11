import pyspark
import glob
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.functions import from_json, explode
from pyspark.sql.types import ArrayType, StructType, StructField, StringType, DoubleType
from pyspark.sql.functions import date_format
from pyspark import SparkContext
from pyspark.mllib.stat import Statistics

# Create a SparkSession
spark = SparkSession.builder.appName("parquetReaderEarthquake").getOrCreate()
spark

def elasticsearch_insert(index_name, data): 

    from elasticsearch import Elasticsearch 
    from elasticsearch import helpers 
    from datetime import datetime 
    import pandas as pd 

    def safe_date(date_value): 

        return ( 
            pd.to_datetime(date_value) if not pd.isna(date_value) 
                else  datetime(2010,1,1,0,0) 
        ) 


    def filterKeys(document): 
        return {key: document[key] for key in use_these_keys }

    def doc_generator(data, index_name): 
        df_iter = data.toPandas().iterrows()
        for index, document in df_iter: 
            yield { 
                    "_index": index_name, 
                    "_type": "_doc", 
                    "_source": filterKeys(document), 
                } 

    #    raise StopIteration 
    es_client = Elasticsearch(['http://18.156.117.103:9200'],http_compress=True) 

    # 
    index_name=str(index_name) 

    #READ AND MANUPLATE THE DATASET 

    use_these_keys = data.columns 
    helpers.bulk(es_client, doc_generator(data,index_name)) 

    return print(str(index_name) + " inserted") 
    return print(data.shape)

def group_city(city):
    # Access the city feature
    
    # Define the grouping logic
    if city in ['Hatay', 'Osmaniye','Ağrı', 'Van', 'Hakkari', 'Gaziantep',  'Kahramanmaraş','Siirt','Adana','Malatya','Adıyaman', 'Elazığ','Bingöl', 'Osmaniye', 'Şanlıurfa']:
        city_group = 'Doğu Anadolu'
   
    elif city in ['Tekirdağ', 'İstanbul','Çanakkale','Kocaeli','Sakarya','Artvin','Düzce','Bolu','Karabük', 'Çankırı','Kastamonu','Çorum','Samsun','Amasya','Tokat','Sivas',   'Giresun','Erzincan','Erzurum','Muş', 'Bitlis', 'Bartın', 'Gümüşhane']:
        city_group = 'Kuzey Anadolu'
    
    elif city in ['Balıkesir', 'İzmir','Eskişehir', 'Aydın', 'Kayseri', 'Kırşehir', 'Konya', 'Kütahya','Manisa', 'Muğla', 'Isparta', 'Denizli', 'Burdur','Aksaray', 'Uşak', 'Afyonkarahisar', 'Antalya', 'Afyon', 'Bursa', 'Bilecik', 'Ankara']:
        city_group = 'Batı Anadolu'
    
    else:
        city_group = ''
    
    
    # Return a new row with the grouped city
    return city_group

def sparktoELK(data):
    byte_array_to_ascii = udf(lambda x: bytearray(x).decode('utf-8'))
    eQ_df_corrected = data.withColumn("ascii_value",byte_array_to_ascii("value"))
    ascii_eQ = eQ_df_corrected.select("ascii_value")
    schema_eQ = StructType([
        StructField("date", StringType(), True),
        StructField("rms", StringType(), True),
        StructField("eventID", StringType(), True),
        StructField("location", StringType(), True),
        StructField("latitude", StringType(), True),
        StructField("longitude", StringType(), True),
        StructField("depth", StringType(), True),
        StructField("type", StringType(), True),
        StructField("magnitude", StringType(), True),
        StructField("country", StringType(), True),
        StructField("province", StringType(), True),
        StructField("district", StringType(), True),
        StructField("neighborhood", StringType(), True),
        StructField("isEventUpdate", StringType(), True),
        StructField("lastUpdateDate", StringType(), True)
    ])
    parsed_eQ = eQ_df_corrected.select(from_json("ascii_value", schema_eQ).alias("parsed_value_eQ"))
    extracted_eQ = parsed_eQ.select("parsed_value_eQ.*")
    earthquake_data = extracted_eQ.withColumn("date", date_format("date", "yyyy-MM-dd'T'HH:mm")).distinct()
    spark = SparkSession.builder.getOrCreate()
    spark
    rdd_data = earthquake_data.rdd
    new_rdd = rdd_data.map(lambda row: (row.date, row.country, row.province, group_city(row.province),row.district, float(row.depth), float(row.magnitude),
float(row.longitude), float(row.latitude), str(str(row.latitude)+','+str(row.longitude)), float(row.eventID), 
row.isEventUpdate, row.lastUpdateDate, row.location, row.neighborhood, float(row.rms), row.type)).distinct()
    
    schema_eQ2 = StructType([
        StructField('date', StringType(), nullable=True),
        StructField('country', StringType(), nullable=True),
        StructField('province', StringType(), nullable=True),
        StructField('fault_line', StringType(), nullable=True),
        StructField('district', StringType(), nullable=True),
        StructField('depth', StringType(), nullable=True),
        StructField('magnitude', StringType(), nullable=True),
        StructField('longitude', StringType(), nullable=True),
        StructField('latitude', StringType(), nullable=True),
        StructField('pointLocation', StringType(), nullable=True),
        StructField('eventID', StringType(), nullable=True),
        StructField('isEventUpdate', StringType(), nullable=True),
        StructField('lastUpdateDate', StringType(), nullable=True),
        StructField('location', StringType(), nullable=True),
        StructField('neighborhood', StringType(), nullable=True),
        StructField('rms', StringType(), nullable=True),
        StructField('type', StringType(), nullable=True)
    ])
    elastic_eartquake_df = spark.createDataFrame(new_rdd, schema_eQ2)
    elasticsearch_insert('earthquake-all',elastic_eartquake_df)
    return ("inserted")

EQ_DIR = "/home/ubuntu/parquet-files/eq/"
previous_parquet_files=[]
old_parquet_files = glob.glob(EQ_DIR + "/*.parquet")
old_parquet_data = spark.read.parquet(*old_parquet_files)
if old_parquet_data:
    get_rdd(old_parquet_data)
            
while True: 
    current_parquet_files = glob.glob(EQ_DIR + "/*.parquet") 
    new_parquet_files = set(current_parquet_files) - set(old_parquet_files)
    new_parquet_files =  set(new_parquet_files) - set(previous_parquet_files)
    if len(new_parquet_files) > 0:
        previous_parquet_files.append(new_parquet_files)
        new_parquet_data=spark.read.parquet(*new_parquet_files)
        get_rdd(new_parquet_data)
        
        
def rdd_functions(data):
    byte_array_to_ascii = udf(lambda x: bytearray(x).decode('utf-8'))
    eQ_df_corrected = data.withColumn("ascii_value", byte_array_to_ascii("value"))
    ascii_eQ = eQ_df_corrected.select("ascii_value")
    schema_eQ = StructType([
        StructField("date", StringType(), True),
        StructField("rms", StringType(), True),
        StructField("eventID", StringType(), True),
        StructField("location", StringType(), True),
        StructField("latitude", StringType(), True),
        StructField("longitude", StringType(), True),
        StructField("depth", StringType(), True),
        StructField("type", StringType(), True),
        StructField("magnitude", StringType(), True),
        StructField("country", StringType(), True),
        StructField("province", StringType(), True),
        StructField("district", StringType(), True),
        StructField("neighborhood", StringType(), True),
        StructField("isEventUpdate", StringType(), True),
        StructField("lastUpdateDate", StringType(), True)
    ])
    parsed_eQ = eQ_df_corrected.select(from_json("ascii_value", schema_eQ).alias("parsed_value_eQ"))
    extracted_eQ = parsed_eQ.select("parsed_value_eQ.*")
    earthquake_data = extracted_eQ.withColumn("date", date_format("date", "yyyy-MM-dd'T'HH:mm")).distinct()
    spark = SparkSession.builder.getOrCreate()
    spark
    #create rdd
    rdd_data = earthquake_data.rdd
    new_rdd = rdd_data.map(lambda row: (row.date, row.country, row.province, group_city(row.province), 
                                    row.district, float(row.depth), float(row.magnitude),
                                    float(row.longitude), float(row.latitude),    str(str(row.latitude)+','+str(row.longitude)), float(row.eventID), 
                                    row.isEventUpdate, row.lastUpdateDate, row.location,
                                    row.neighborhood, float(row.rms), row.type)).distinct()
    #append
    numeric_columns = []
    for row in new_rdd.take(1):
        for idx, value in enumerate(row):
            if isinstance(value, (int, float)):
                numeric_columns.append(idx)

    # Filter out the numeric columns
    descriptive_rdd = new_rdd.map(lambda row: tuple(row[idx] for idx in numeric_columns))

    # Print the new RDD
    descriptive_rdd.take(1)
    
    # Define the feature names
    feature_names = ["depth","magnitude","longitude", "latitude", "eventID", "rms"]

    # Calculate descriptive statistics
    summary = Statistics.colStats(descriptive_rdd)

    # Get the count of rows
    count = summary.count()

    # Initialize result lists
    sum_list = []
    mean_list = []
    variance_list = []
    std_dev_list = []
    max_list = []
    min_list = []

    # Calculate descriptive statistics for each column
    for i in range(len(feature_names)):
        feature_name = feature_names[i]

        sum_column = summary.mean()[i] * summary.count()
        mean_column = summary.mean()[i]
        variance_column = summary.variance()[i]
        std_dev_column = float(variance_column) ** 0.5
        max_column = summary.max()[i]
        min_column = summary.min()[i]

        # Append the calculated values to the result lists
        sum_list.append(sum_column)
        mean_list.append(mean_column)
        variance_list.append(variance_column)
        std_dev_list.append(std_dev_column)
        max_list.append(max_column)
        min_list.append(min_column)

    # Print the descriptive statistics for each column
    for i in range(len(feature_names)):
        print(f"{feature_names[i]}:")
        print("  Count:", count)
        print("  Sum:", sum_list[i])
        print("  Mean:", mean_list[i])
        print("  Variance:", variance_list[i])
        print("  Standard Deviation:", std_dev_list[i])
        print("  Maximum:", max_list[i])
        print("  Minimum:", min_list[i])
        print()
        
    dogu_anadolu_4_rdd = new_rdd.filter(lambda row: row[3] == 'Doğu Anadolu').filter(lambda row: row[6] >= 4).map(lambda row : (row[0], row[1], row[3], row[2],  row[4], row[5], row[6]))

    kuzey_anadolu_4_rdd = new_rdd.filter(lambda row: row[3] == 'Kuzey Anadolu').filter(lambda row: row[6] >= 4).map(lambda row : (row[0], row[1], row[3], row[2], row[4], row[5], row[6]))

    batı_anadolu_4_rdd = new_rdd.filter(lambda row: row[3] == 'Batı Anadolu').filter(lambda row: row[6] >= 4).map(lambda row : (row[0], row[1], row[3], row[2], row[4], row[5], row[6]))


    print('--------------------------------------------------------------')    
    print('Doğu Anadolu fay hattı üzerinde olan 4 ve üzeri depremler :') 
    print('--------------------------------------------------------------')    

    for row in dogu_anadolu_4_rdd.take(3):
        print(row)

    print('--------------------------------------------------------------')    
    print('Kuzey Anadolu fay hattı üzerinde olan 4 ve üzeri depremler :') 
    print('--------------------------------------------------------------')  

    for row in kuzey_anadolu_4_rdd.take(3):
        print(row)
    
    print('--------------------------------------------------------------')    
    print('Batı Anadolu fay hattı üzerinde olan 4 ve üzeri depremler :') 
    print('--------------------------------------------------------------')  

    for row in batı_anadolu_4_rdd.take(3):
        print(row)
        
    print('--------------------------------------------------------------')    
    print('Doğu Anadolu fay hattı üzerinde Olan 4 ve üzeri deprem olan iller :') 
    print('--------------------------------------------------------------')  
    print(dogu_anadolu_4_rdd.map(lambda row: row[3]).distinct().collect())  

    print('--------------------------------------------------------------')    
    print('Kuzey Anadolu fay hattı üzerinde Olan 4 ve üzeri deprem olan iller :') 
    print('--------------------------------------------------------------')  
    print(kuzey_anadolu_4_rdd.map(lambda row: row[3]).distinct().collect())

    print('--------------------------------------------------------------')    
    print('Batı Anadolu fay hattı üzerinde Olan 4 ve üzeri deprem olan iller :') 
    print('--------------------------------------------------------------')  
    print(batı_anadolu_4_rdd.map(lambda row: row[3]).distinct().collect())
    
    other_eq_4_rdd = new_rdd.filter(lambda row: row[3] == '').filter(lambda row: row[6] >= 4).map(lambda row : (row[0], row[1], row[3], row[2],  row[4], row[5], row[6]))

    all_eq_4_rdd = dogu_anadolu_4_rdd.union(kuzey_anadolu_4_rdd).union(batı_anadolu_4_rdd).union(other_eq_4_rdd)

    print("Doğu Anadolu fay hattı üzerinde olan 4 ve üzeri deprem sayısı :",dogu_anadolu_4_rdd.count())
    print("Kuzey Anadolu fay hattı üzerinde olan 4 ve üzeri deprem sayısı :",kuzey_anadolu_4_rdd.count())
    print("Batı Anadolu fay hattı üzerinde olan 4 ve üzeri deprem sayısı :",batı_anadolu_4_rdd.count())
    print("Ana fay hattı üzerinde olmayan 4 ve üzeri deprem sayısı :",other_eq_4_rdd.count())
    print("Tüm 4 ve üzeri deprem sayısı :",all_eq_4_rdd.count())

    rdd_keyby = all_eq_4_rdd.keyBy(lambda w: w[2])
    print(rdd_keyby.take(3))
    
    rdd_grouped = all_eq_4_rdd.groupBy(lambda row: (row[3]))

    rdd_count = rdd_grouped.mapValues(len)
    
    for key, count in rdd_count.collect():
        print("Key: ", key)
        print("Count: ", count)
        
    keyValueRDD = all_eq_4_rdd.map(lambda x: (x[2], x))  # burada fay hattı kolonunu en başa da koyarak oduplicate etmiş oldum. ve onu key olarak görmesini sağladım.
    keyValueRDD.take(1)

    rdd_grouped = keyValueRDD.groupByKey()

    # Count the number of elements in each group
    rdd_count = rdd_grouped.mapValues(len)
    
    # Print the results
    for key, count in rdd_count.collect():
        print("Key: ", key)
        print("Count: ", count)
        
    # Apply the reduce function to the "magnitude" column
    max_magnitude = new_rdd.map(lambda x: x[6]).reduce(lambda x, y: max(x, y))
    min_magnitude = new_rdd.map(lambda x: x[6]).reduce(lambda x, y: min(x, y))

    # Print the results
    print("Maximum Magnitude:", max_magnitude)
    print("Minimum Magnitude:", min_magnitude)
    
    return ("RDD Functions are completed.")
    
