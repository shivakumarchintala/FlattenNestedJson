import os  # native python library
from jproperties import Properties  # external library installed

# pyspark libraries
from pyspark import SparkContext
from pyspark.sql import SparkSession

# other python files imported
import JsonToFlat as Jf

# sc = SparkContext(master='local', appName='FlatJson')
spark = SparkSession.builder.appName("FlatJson").master("local").config("spark.sql.warehouse.dir",
                                                                        "E:/BigData_Mini_Projects/FlatJson/spark-warehouse").enableHiveSupport().getOrCreate()

config_file_path = "FlatJson.properties"  # this is properties file that has the required parameters.

configurations = Properties()
with open(config_file_path, 'rb') as config_file:
    configurations.load(config_file)

files_read = []

while True:
    json_files = [file_name for file_name in os.listdir(configurations.get('SourceDir_Path').data) if
                  file_name.endswith('.json')]
    if len(json_files) > 0:  # condition to check if folder has any json files

        not_processed_files = list(
            set(json_files).difference(files_read))  # pulling out the files that are not processed
        if len(not_processed_files) > 0:
            current_file = not_processed_files[0]
            df = spark.read.option("multiline", "true").json(
                configurations.get('SourceDir_Path').data + "\\" + current_file)  # reading the file to dataframe

            final = Jf.Ops(df).json_to_flat()  # Applying the flattening function
            final.printSchema()

            # post conversion starts here
            post_conv = Jf.PostFlattenHandy(df=final, database_name=configurations.get('Database_Name').data,
                                            table_name=configurations.get('Target_Table_Name').data,
                                            spark_Variable=spark)  # Creating the object of PostFlattenHandy imported from JsonToFlat file

            if post_conv.check_if_table_exists():  # to check if the table exists

                if post_conv.the_alter_schema_combo():
                    '''this validates schema of incoming 
                    dataframe with existing table in Target Database. if there is any 
                    difference it will alter schemas of incoming dataframe and target table accordingly
                     and will only append once both dataframe and target table have same schema'''
                    post_conv.append_data_table()

            else:

                post_conv.create_table()

            # adding processed file to files read so that there will not be any data replications
            files_read.append(current_file)
        else:
            print("job done successfully....  :)")
            break

    else:
        break
