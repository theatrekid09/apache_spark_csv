from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.functions import when


if __name__ == '__main__':
    scSpark = SparkSession \
        .builder \
        .appName("reading csv") \
        .getOrCreate()

    # data_file_2 = 'supermarket.csv'
    # sdfData2 = scSpark.read.csv(data_file_2, header=True, sep=",").cache()
    # DataFrame[Invoice ID: string, Branch: string, City: string, Customer type: string, Gender: string, Product line: string, Unit price: string, Quantity: string, Tax 5%: string, Total: string, Date: string, Time: string, Payment: string, cogs: string, gross margin percentage: string, gross income: string, Rating: string]
    # gender = sdfData2.groupBy('Gender').count()
    # print(gender.show())

    # +------+-----+
    # |Gender|count|
    # +------+-----+
    # |Female|  501|
    # |  Male|  499|
    # +------+-----+


    ### sql spark

    # sdfData2.registerTempTable("sales")
    # output = scSpark.sql('SELECT * FROM sales')
    # output = scSpark.sql('SELECT * FROM sales WHERE `Unit Price` > 20 AND `Quantity` >= 7')
    # output = scSpark.sql('SELECT COUNT(*) as total, City FROM sales GROUP BY City')
    # output.show()

    # uploaded multiple csvs and join all csvs together - Extract and Transform data
    data_file = 'data*.csv'
    sdfData = scSpark.read.csv(data_file, header=True, sep=",").cache()
    # sdfData -> DataFrame[name: string, age: string, country: string]
    # print('Total Records = {}'.format(sdfData.count()))
    # sdfData.show()
    # create a new table 
    sdfData.registerTempTable("clientList")
    output = scSpark.sql("SELECT * FROM clientList")
    output =  output.withColumn("occupation",  when(output.country == "Pakistan", "teacher").otherwise("engineer"))
    output.show()

    #     Total Records = 4
    # +------+---+--------+
    # |  name|age| country|
    # +------+---+--------+
    # | adnan| 40|Pakistan|
    # |  maaz|  9|Pakistan|
    # | musab|  4|Pakistan|
    # |ayesha| 32|Pakistan|
    # +------+---+--------+

    #after all files combined:

    #     Total Records = 8
    # +-------+---+--------+
    # |   name|age| country|
    # +-------+---+--------+
    # | noreen| 23| England|
    # |  Aamir|  9|Pakistan|
    # |  Noman|  4|Pakistan|
    # |Rasheed| 12|Pakistan|
    # |  adnan| 40|Pakistan|
    # |   maaz|  9|Pakistan|
    # |  musab|  4|Pakistan|
    # | ayesha| 32|Pakistan|
    # +-------+---+--------+

# LOAD - L -> JSON,XML,RDBMS

t = output.coalesce(1).write.format('csv').option("header", "true").save('occupation.csv')
# print(t)