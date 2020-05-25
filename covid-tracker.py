from pyspark import SparkConf, SparkContext, SparkFiles
from pyspark.sql import SQLContext
from pyspark.sql.types import DateType, LongType, IntegerType, FloatType, StringType, StructType, StructField
from urllib.request import urlopen
import matplotlib.pyplot as plt
import pandas as pd

def equivalent_type(f):
    if f == 'datetime64[ns]': return DateType()
    elif f == 'int64': return LongType()
    elif f == 'int32': return IntegerType()
    elif f == 'float64': return FloatType()
    else: return StringType()

def define_structure(string, format_type):
    try: typo = equivalent_type(format_type)
    except: typo = StringType()
    return StructField(string, typo)

# Given pandas dataframe, it will return a spark's dataframe.
def pandas_to_spark(pandas_df):
    columns = list(pandas_df.columns)
    types = list(pandas_df.dtypes)
    struct_list = []
    for column, typo in zip(columns, types): 
      struct_list.append(define_structure(column, typo))
    p_schema = StructType(struct_list)
    return sqlContext.createDataFrame(pandas_df, p_schema)

conf = SparkConf().setMaster("local").setAppName("CovidTracker")
sc = SparkContext(conf = conf)

response = urlopen("https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_global.csv")
pdf = pd.read_csv(response)

sqlContext = SQLContext(sc)

sdf = pandas_to_spark(pdf)

sdf.createOrReplaceTempView('covid_data')
#sdf.printSchema()

TurkeyCovidDF = sqlContext.sql(
    """ SELECT * FROM covid_data --WHERE `Country/Region` = "Turkey" """
)
#TurkeyCovidDF.show()

TCDF = TurkeyCovidDF.drop('Lat','Long')
#TCDF.show()

PUPTCDF = TCDF.toPandas().set_index(['Country/Region','Province/State']).transpose()
PUPTCDF['Tarih'] = PUPTCDF.index
colnames = PUPTCDF.columns.tolist()
colnames = colnames[-1:] + colnames[:-1]
PUPTCDF = PUPTCDF[colnames]
PUPTCDF.reset_index(drop=True, inplace=True)

PUPTCDF.Tarih = pd.to_datetime(PUPTCDF.Tarih).dt.strftime('%d/%m/%Y')
print(PUPTCDF)
#UPTCDF = pandas_to_spark(PUPTCDF)
#UPTCDF.show()

PUPTCDF.to_excel('turkey_covid_dataset.xlsx', engine='xlsxwriter')

plt.plot(PUPTCDF['Tarih'], PUPTCDF['Turkey'])
plt.show()