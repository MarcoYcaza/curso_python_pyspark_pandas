from sqlite3 import Timestamp
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.functions import col
from pyspark.sql.functions import to_timestamp
from datetime import datetime
from datetime import time
from pyspark.sql.types import TimestampType


spark = SparkSession.builder.master("local[*]").appName("EComerce").getOrCreate()


df = spark.read.csv(r"C:\Users\MAFOGUJF\Ejercicios Python\eCommerce\2019-Dec.csv", header=True)

df1 = df

df1 = df1.drop("event_time")

class ECommerce:
        
    def __init__(df , df1):
        self.df = df
        self.df1 = df1
   

    def eliminar_columnas(self):
    
        cogerFecha = lambda x:  "{:02d}:{:02d}:{:04d}".format(datetime.strptime(x [:19].split()[0], '%Y-%m-%d').day,datetime.strptime(x [:19].split()[0], '%Y-%m-%d').month, datetime.strptime(x [:19].split()[0], '%Y-%m-%d').year)
        ponerFecha = udf(cogerFecha)

        cogerHora = lambda x:  "{:02d}:{:02d}:{:02d}".format(datetime.strptime(x [:19].split()[1], '%H:%M:%S').hour,datetime.strptime(x [:19].split()[1], '%H:%M:%S').minute, datetime.strptime(x [:19].split()[1], '%H:%M:%S').second)
        ponerHora = udf(cogerHora)
    
        columns_length = self.df.columns
        
        for x in columns_length:
            if x != "event_time":
                self.df = self.df.drop(x)
            else:            
                self.df = self.df.withColumn("Hora", ponerHora(col("event_time")))
                self.df = self.df.withColumn("event_time", ponerFecha(col("event_time")))
                
        return self.df

    def unirDfs(self):
        self.df = self.df.withColumn("ID",monotonically_increasing_id())
        self.df1 = self.df1.withColumn("ID",monotonically_increasing_id())
        
        return self.df.join(self.df1, "ID").drop("ID")



df_res = ECommerce(df,df1).eliminar_columnas()

df3    = ECommerce(df_res,df1).unirDfs()

df3.show()

#df3.printSchema()

#df.coalesce(2).write.mode("overwrite").parquet(r"C:\Users\MAFOGUJF\Ejercicios Python\Particiones")



