# Import the already available Spark session
# This works when Spark is already running in your environment
from pyspark.shell import spark

# Import PySpark data types
from pyspark.sql.types import StringType, ArrayType, StructType, StructField, MapType

# Import PySpark functions used in this program
from pyspark.sql.functions import explode, split, array, array_contains, map_keys, map_values


# Run the code below only when this file is executed directly
if __name__ == "__main__":

    # ---------------------------------------------------------
    # PART 1: READ CSV FILE
    # ---------------------------------------------------------

    # Read the CSV file
    # header=True means Spark will use the first row as column names
    df = spark.read.option("header", True) \
        .csv("file:///home/takeo/simple-zipcodes.csv")

    # Print the schema
    # Schema means column names and data types
    print("Schema of simple-zipcodes.csv")
    df.printSchema()

    # Show all data from the CSV file
    print("Data from CSV file")
    df.show()

    # ---------------------------------------------------------
    # PART 2: WRITE PARTITIONED FILES BY STATE
    # ---------------------------------------------------------

    # Write the DataFrame to disk
    # partitionBy("State") means Spark will create separate folders for each state
    # mode("overwrite") means replace old output if it already exists
    df.write.option("header", True) \
        .partitionBy("State") \
        .mode("overwrite") \
        .csv("file:///tmp/parts/zipcodes-state")

    # ---------------------------------------------------------
    # PART 3: WRITE PARTITIONED FILES BY STATE AND CITY
    # ---------------------------------------------------------

    # Here Spark creates folders using both State and City
    # Example:
    # /tmp/parts/zipcodes-city-state/State=AL/City=SPRINGVILLE
    df.write.option("header", True) \
        .partitionBy("State", "City") \
        .mode("overwrite") \
        .csv("file:///tmp/parts/zipcodes-city-state")

    # ---------------------------------------------------------
    # PART 4: USE repartition() AND partitionBy() TOGETHER
    # ---------------------------------------------------------

    # repartition(2) means divide the DataFrame into 2 partitions in memory
    # Then partitionBy("State") writes the data into folders on disk
    df.repartition(2).write.option("header", True) \
        .partitionBy("State") \
        .mode("overwrite") \
        .csv("file:///tmp/parts/zipcodes-state-more")

    # ---------------------------------------------------------
    # PART 5: CONTROL NUMBER OF RECORDS PER FILE
    # ---------------------------------------------------------

    # maxRecordsPerFile=2 means each output file will contain at most 2 rows
    # This is useful when you want smaller output files
    df.write.option("header", True) \
        .option("maxRecordsPerFile", 2) \
        .partitionBy("State") \
        .mode("overwrite") \
        .csv("file:///tmp/parts/multi-zipcodes-state")

    # ---------------------------------------------------------
    # PART 6: READ ONE SPECIFIC PARTITION
    # ---------------------------------------------------------

    # Read only one partition folder instead of reading the full dataset
    # This reads only records for State=AL and City=SPRINGVILLE
    dfSinglePart = spark.read.option("header", True) \
        .csv("file:////tmp/parts/zipcodes-city-state/State=AL/City=SPRINGVILLE")

    # Print schema of the selected partition
    print("Schema of one specific partition")
    dfSinglePart.printSchema()

    # Show data from the selected partition
    print("Data from one specific partition")
    dfSinglePart.show()

    # ---------------------------------------------------------
    # PART 7: READ PARTITIONED DATA AND RUN SQL
    # ---------------------------------------------------------

    # Read the whole partitioned folder
    parqDF = spark.read.option("header", True) \
        .csv("file:////tmp/parts/zipcodes-city-state/")

    # Create a temporary SQL view called ZIPCODE
    # This lets us run SQL queries on this DataFrame
    parqDF.createOrReplaceTempView("ZIPCODE")

    # Run SQL query to get records where State is AL and City is SPRINGVILLE
    print("SQL result for State=AL and City=SPRINGVILLE")
    spark.sql("select * from ZIPCODE where State='AL' and City='SPRINGVILLE'").show()

    # ---------------------------------------------------------
    # PART 8: ARRAYTYPE EXAMPLE
    # ---------------------------------------------------------

    # Create sample data with array columns
    # languagesAtSchool and languagesAtWork are arrays
    data = [
        ("James,,Smith", ["Java", "Scala", "C++"], ["Spark", "Java"], "OH", "CA"),
        ("Michael,Rose,", ["Spark", "Java", "C++"], ["Spark", "Java"], "NY", "NJ"),
        ("Robert,,Williams", ["CSharp", "VB"], ["Spark", "Python"], "UT", "NV")
    ]

    # Create schema for the array example
    schema = StructType([
        StructField("name", StringType(), True),
        StructField("languagesAtSchool", ArrayType(StringType()), True),
        StructField("languagesAtWork", ArrayType(StringType()), True),
        StructField("currentState", StringType(), True),
        StructField("previousState", StringType(), True)
    ])

    # Create DataFrame using sample data and schema
    df = spark.createDataFrame(data=data, schema=schema)

    # Print schema of array example
    print("Schema for ArrayType example")
    df.printSchema()

    # Show array example data
    print("Data for ArrayType example")
    df.show()

    # ---------------------------------------------------------
    # PART 9: explode() ON ARRAY COLUMN
    # ---------------------------------------------------------

    # explode() creates one new row for each item inside the array
    # So one array column becomes many rows
    print("explode() example")
    df.select(df.name, explode(df.languagesAtSchool)).show()

    # ---------------------------------------------------------
    # PART 10: split() EXAMPLE
    # ---------------------------------------------------------

    # split() breaks a string into an array using a delimiter
    # Here the delimiter is comma
    print("split() example")
    df.select(split(df.name, ",").alias("nameAsArray")).show()

    # ---------------------------------------------------------
    # PART 11: array() EXAMPLE
    # ---------------------------------------------------------

    # array() combines multiple columns into one array column
    # Here currentState and previousState are combined into a new column called States
    print("array() example")
    df.select(df.name, array(df.currentState, df.previousState).alias("States")).show()

    # ---------------------------------------------------------
    # PART 12: array_contains() EXAMPLE
    # ---------------------------------------------------------

    # array_contains() checks whether the array contains a specific value
    # Here we check whether languagesAtSchool contains "Java"
    print("array_contains() example")
    df.select(df.name, array_contains(df.languagesAtSchool, "Java").alias("array_contains")).show()

    # ---------------------------------------------------------
    # PART 13: MAPTYPE EXAMPLE
    # ---------------------------------------------------------

    # Create schema with a map column
    # properties is a dictionary-like column
    schema = StructType([
        StructField("name", StringType(), True),
        StructField("properties", MapType(StringType(), StringType()), True)
    ])

    # Create sample dictionary data
    dataDictionary = [
        ('James', {'hair': 'black', 'eye': 'brown'}),
        ('Michael', {'hair': 'brown', 'eye': None}),
        ('Robert', {'hair': 'red', 'eye': 'black'}),
        ('Washington', {'hair': 'grey', 'eye': 'grey'}),
        ('Jefferson', {'hair': 'brown', 'eye': ''})
    ]

    # Create DataFrame using map data
    df = spark.createDataFrame(data=dataDictionary, schema=schema)

    # Print schema of map example
    print("Schema for MapType example")
    df.printSchema()

    # Show map data
    print("Data for MapType example")
    df.show(truncate=False)

    # ---------------------------------------------------------
    # PART 14: EXTRACT MAP VALUES USING rdd.map()
    # ---------------------------------------------------------

    # Read values from the map column using RDD transformation
    # This gets the values of keys: hair and eye
    df3 = df.rdd.map(lambda x: (x.name, x.properties["hair"], x.properties["eye"])) \
        .toDF(["name", "hair", "eye"])

    # Print schema of extracted map values
    print("Map values extracted using rdd.map()")
    df3.printSchema()

    # Show extracted values
    df3.show()

    # ---------------------------------------------------------
    # PART 15: EXTRACT MAP VALUES USING getItem()
    # ---------------------------------------------------------

    # Another way to get values from the map column
    # getItem("hair") gets the value of key "hair"
    # getItem("eye") gets the value of key "eye"
    print("Map values using getItem()")
    df.withColumn("hair", df.properties.getItem("hair")) \
        .withColumn("eye", df.properties.getItem("eye")) \
        .drop("properties") \
        .show()

    # ---------------------------------------------------------
    # PART 16: EXTRACT MAP VALUES USING BRACKET SYNTAX
    # ---------------------------------------------------------

    # Another simple way to get map values
    # df.properties["hair"] gets the hair value
    # df.properties["eye"] gets the eye value
    print("Map values using bracket syntax")
    df.withColumn("hair", df.properties["hair"]) \
        .withColumn("eye", df.properties["eye"]) \
        .drop("properties") \
        .show()

    # ---------------------------------------------------------
    # PART 17: explode() ON MAP COLUMN
    # ---------------------------------------------------------

    # explode() on a map gives key and value as separate columns
    # Each key-value pair becomes a separate row
    print("explode() on map")
    df.select(df.name, explode(df.properties)).show()

    # ---------------------------------------------------------
    # PART 18: map_keys() EXAMPLE
    # ---------------------------------------------------------

    # map_keys() returns all keys from the map column
    print("map_keys() example")
    df.select(df.name, map_keys(df.properties)).show()

    # ---------------------------------------------------------
    # PART 19: GET DISTINCT MAP KEYS AS A PYTHON LIST
    # ---------------------------------------------------------

    # First get all keys from the map
    # explode() makes one row per key
    # distinct() removes duplicates
    keysDF = df.select(explode(map_keys(df.properties))).distinct()

    # Convert Spark rows into a normal Python list
    keysList = keysDF.rdd.map(lambda x: x[0]).collect()

    # Print the list of unique keys
    print("Distinct map keys as Python list")
    print(keysList)

    # ---------------------------------------------------------
    # PART 20: map_values() EXAMPLE
    # ---------------------------------------------------------

    # map_values() returns all values from the map column
    print("map_values() example")
    df.select(df.name, map_values(df.properties)).show()
