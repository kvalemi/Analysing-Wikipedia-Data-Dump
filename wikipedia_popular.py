import sys
from pyspark.sql import SparkSession, functions, types
import re

spark = SparkSession.builder.appName('reddit averages').getOrCreate()
spark.sparkContext.setLogLevel('WARN')

assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
assert spark.version >= '2.3' # make sure we have Spark 2.3+


# Define the data schema
wiki_schema = types.StructType([

    types.StructField('language', types.StringType()),
    types.StructField('title', types.StringType()),
    types.StructField('requested', types.IntegerType()),
    types.StructField('bytes', types.IntegerType())

])

# UDF: Parse the filename to extract the date
def p_to_h(t):
    t_search = re.search('\d{8}\-\d{2}', t)

    if(t_search):
        return(t_search.group(0))
    return(None)



def main(in_directory, out_directory):
    
    # Read in the wikipedia data
    wiki_data = spark.read.csv(path = in_directory, sep = ' ', schema = wiki_schema).withColumn('filename', functions.input_file_name())

    # Filter to get the required format
    wiki_data = wiki_data.filter(wiki_data['language'] == 'en')
    wiki_data = wiki_data.filter(wiki_data['title'] != 'Main_Page')
    wiki_data = wiki_data.filter("title not like 'Special:%'")

    # Cache that bad boy
    wiki_data = wiki_data.cache()

    # Define the UDF for converting filename column to time column
    path_to_hour = functions.udf(p_to_h, returnType = types.StringType())  

    # Call the UDF
    wiki_data = wiki_data.withColumn("time", path_to_hour(wiki_data['filename']))

    # Group by the hour and get the max
    max_req_per_hour = wiki_data.groupBy('time')
    max_req_per_hour = max_req_per_hour.agg(functions.max('requested')).select('time','max(requested)')

    # Rename the columns to something more meaningful
    max_req_per_hour = max_req_per_hour.withColumnRenamed("max(requested)", "requested_max")
    max_req_per_hour = max_req_per_hour.withColumnRenamed("time", "time_max")

    # Get the name of the max reqs page
    max_reqs = wiki_data.join(max_req_per_hour, (wiki_data.time == max_req_per_hour.time_max) & (wiki_data.requested == max_req_per_hour.requested_max))
    max_reqs = max_reqs.select("time", "title", "requested")

    # sort by time and number of requests
    sorted_reqs = max_reqs.sort('time', 'title')

    sorted_reqs.show()

    # Write the output as csv
    sorted_reqs.write.csv(out_directory, mode='overwrite')



if __name__=='__main__':
    in_directory = sys.argv[1]
    out_directory = sys.argv[2]
    main(in_directory, out_directory)
