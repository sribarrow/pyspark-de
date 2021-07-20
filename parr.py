from pyarrow import parquet, dataset
from pyspark.sql import SparkSession
from pyspark.sql.functions import date_format, to_date

class PDataFrame:
    data_folder = "csv/"
    pq_filepath = "pq/weather.parquet"  
    
    def __init__(self, src_folder: str = data_folder, dest_file: str = pq_filepath):
        self.src = src_folder
        self.dest = dest_file

    def import_from_dir(self):
        try:
            ds = dataset.dataset(self.src, format="csv")
        except:
            raise FileNotFoundError("")
        self.__write_to_parquet(ds, self.dest)
        return True
    
    def __write_to_parquet(self, ds: object, fp: str) -> None:
        table = ds.to_table()
        wf = parquet.ParquetWriter(fp, ds.schema, compression = 'SNAPPY')
        for i in range(3):
            wf.write_table(table,row_group_size=10000 )
        wf.close()

    def read_from_parquet_with_ps(self):
        weather = []
        try:
            spark = SparkSession.builder.appName('WeatherApp').getOrCreate()
            df = spark.read.parquet(self.dest, inferSchema=True)
            max_temp = df.agg({'ScreenTemperature': 'max'}).collect()
            max_temp = max_temp[0][0]
            weather.append(max_temp)
            obs_date = df.withColumn('ObservationDate', date_format(df.ObservationDate, 'dd-MM-yyyy')).filter(df["ScreenTemperature"] == max_temp).select('ObservationDate', 'Region').distinct().collect()
            weather.append(obs_date[0][0])
            weather.append(obs_date[0][1])
            return weather
        except:
            raise FileNotFoundError ("Path does not exist")

if __name__ == "__main__":
    # import_from_dir()
    choice = input("\t\t\t1. Which date was hottest day? \n \
                    2. What was the temperature on that day? \n \
                    3. In which region was the hottest day?\n \
                        Type 1, 2 or 3 (Default = 1): " )
    if (choice.isdigit()): choice = int(choice)
    result = read_from_parquet_with_ps()
    if choice == 1:
        print(f"The hottest day is {result[1]}")
    elif choice == 2:
        print(f"The temperature on that day is {result[0]}")
    else:
        print(f"The region with the hottest day {result[2]}")
