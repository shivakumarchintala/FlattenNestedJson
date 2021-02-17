from pyspark.sql.functions import col, explode_outer
from pyspark.sql import types as T


class Ops:

    def json_to_flat(self):   # method to flatten the dataframe
        df = self.df
        nested_columns = dict([(x.name, x.dataType) for x in df.schema.fields if
                               isinstance(x.dataType, T.StructType) or isinstance(x.dataType, T.ArrayType)])

        if len(nested_columns) > 0:
            col_name = list(nested_columns.keys())[0]
            if isinstance(nested_columns[col_name], T.ArrayType):

                df = df.withColumn(col_name, explode_outer(col_name))

                return Ops(df).json_to_flat()

            elif isinstance(nested_columns[col_name], T.StructType):

                df = df.select("*", *[col(col_name + "." + x.name).alias(col_name + "_" + x.name) for x in
                                      nested_columns[col_name]]).drop(col_name)
                return Ops(df).json_to_flat()
        else:
            return df

    def __init__(self, df):
        self.df = df


class PostFlattenHandy:

    def append_data_table(self):   # to append dataframe with table
        self.df.write.saveAsTable(name=self.table_name, mode='append')

    def change_incoming_schema(self, columns):  # will change the schema of incoming dataframe
        columns = columns
        command = [f"Null as {x}" for x in columns]
        self.df = self.df.selectExpr("*", *command)  # "concat()"

    def GetExistingTableColumns(self):  # this will get the columns of target table in database.
        columns = []
        try:
            columns = self.spark_Variable.sql("select * from " + self.table_name + " limit 1").columns
            print("getting", columns)
            return columns
        except Exception as e:
            return columns

    def PoCon_schema_validation(self):  # this performs schema validation of incoming dataframe and existing target table and returns two variables
        existing_table_columns = self.GetExistingTableColumns()
        incoming_table_columns = self.df.columns
        print(existing_table_columns, incoming_table_columns)
        x = set(existing_table_columns).difference(incoming_table_columns)
        y = set(incoming_table_columns).difference(existing_table_columns)
        columns = []
        out = ""
        if len(existing_table_columns) == len(incoming_table_columns):
            columns = []
        elif len(existing_table_columns) > len(incoming_table_columns):
            columns = list(x)
            out = "incoming"
        elif len(existing_table_columns) < len(incoming_table_columns):
            out = "existing"
            columns = [j for j in self.df.dtypes if j[0] in list(y)]
            print(columns)
        return out, columns

    def check_if_table_exists(self):  # to check if the table exists
        out = None
        try:
            print("Checking if table exists...")
            # self.spark_Variable.read.table(self.table_name)
            self.spark_Variable.sql("select * from " + self.table_name + " limit 1")
            print("Table Exists")
            out = True
        except Exception as e:
            print(e)
            out = False
        return out

    def create_table(self):  # create table in target database
        self.df.write.saveAsTable(self.table_name)
        return True

    def alter_table_schema(self, columns):  # this will alter the target table schema when needed.
        print("alter table schema entered")
        print(columns)
        a = str(columns).strip("[").strip("]")
        print("stage1: {}".format(a))
        b = " ".join(",".join(a.split("), (")).split(", "))
        print("stage2: {}".format(b))
        c = "".join([x for x in b if x != "'"])
        print("stage3: {}".format(c))

        command = "ALTER TABLE " + self.table_name + " ADD COLUMNS " + c
        c = None
        print(command)
        self.spark_Variable.sql(command)
        print("table altered successfully")
        return True


    def the_alter_schema_combo(self):
        from collections import Counter
        existing_table_columns = self.GetExistingTableColumns()
        incoming_table_columns = self.df.columns

        while Counter(existing_table_columns) != Counter(incoming_table_columns):


            val, columns = self.PoCon_schema_validation()

            ''' performing schema validation considering 
                there will be at-least one same b/w incoming and existing columns in database.
                 'val' contains "incoming" or "existing" to change schema of database or incoming dataframe.
                 'columns' variable contains columns that are different. '''

            print("checking schema check", columns)

            if len(columns) > 0 and val == 'existing':
                print("entering alter schema")
                self.alter_table_schema(columns=columns)
            elif len(columns) > 0 and val == 'incoming':
                print("enter incoming change")
                self.change_incoming_schema(columns)

            existing_table_columns = self.GetExistingTableColumns()
            incoming_table_columns = self.df.columns
            print("The alter combo region\n")
            print(existing_table_columns, "\n", incoming_table_columns)


        return True





    def __init__(self, df, database_name, table_name, spark_Variable):
        self.df = df
        self.database_name = database_name
        self.table_name = table_name
        self.spark_Variable = spark_Variable
