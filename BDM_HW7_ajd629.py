from pyspark import SparkContext
from pyspark.sql import HiveContext
from datetime import date, datetime, timedelta
from geopy.distance import vincenty

def citibikeStream(idx, iterator): 
    if idx == 0:
        iterator.next()
    import csv
    reader = csv.reader(iterator)
    for row in reader:
        if (row[3][:10] == '2015-02-01') & (row[6] == 'Greenwich Ave & 8 Ave'):
            startTime = datetime.strptime(row[3], "%Y-%m-%d %H:%M:%S+%f")
            yield startTime, row[0]
            
def taxiFilter(idx, iterator):
    if idx == 0:
        iterator.next()
    import csv
    reader = csv.reader(iterator)
    for row in reader:
        if (row[4] != 'NULL') & (row[5] != 'NULL'):
            if (vincenty((40.73901691,-74.00263761), (float(row[4]), float(row[5]))).miles) <= 0.25:
                dropOffTime = datetime.strptime(row[1], "%Y-%m-%d %H:%M:%S.%f")
                timePlusTen = dropOffTime + timedelta(seconds = 600)
                yield dropOffTime,timePlusTen

sc = SparkContext()                
spark = HiveContext(sc)

#Calling file using sparkcontext sc
taxi = sc.textFile('/tmp/yellow.csv.gz').cache()
citibike = sc.textFile('/tmp/citibike.csv').cache()


cb = citibike.mapPartitionsWithIndex(citibikeStream)
yellow = taxi.mapPartitionsWithIndex(taxiFilter)


cb_df = cb.toDF(['start_time', 'ride'])
yellow_df = yellow.toDF(['dropoff_time', 'delta_ten'])

unique_bike = yellow_df.join(cb_df).filter((yellow_df.dropoff_time < cb_df.start_time) & (yellow_df.delta_ten > cb_df.start_time))

print len(unique_bike.toPandas()['ride'].unique())