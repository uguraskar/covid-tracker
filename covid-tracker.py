from pyspark import SparkConf, SparkContext, SparkFiles
from pyspark.sql import SQLContext
from pyspark.sql.types import DateType, LongType, IntegerType, FloatType, StringType, StructType, StructField
from urllib.request import urlopen
import matplotlib.pyplot as plt
import pandas as pd
import json

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

def country_converter(pandas_df, country_column):
    pandas_df[country_column].replace({'Bolivia, Plurinational State of':'Bolivia',
                          'Brunei Darussalam':'Brunei',
                          'Burma':'Myanmar',
                          'Cabo Verde':'Cape Verde',
                          'Congo':'Republic of the Congo',
                          'Congo (Brazzaville)':'Republic of the Congo',
                          'Congo, the Democratic Republic of the':'Democratic Republic of the Congo',
                          'Congo (Kinshasa)':'Democratic Republic of the Congo',
                          'Côte d\'Ivoire':'Cote d\'Ivoire',
                          'Czechia':'Czech Republic',
                          'Swaziland':'Eswatini',
                          'Holy See (Vatican City State)':'Holy See',
                          'Iran, Islamic Republic of':'Iran',
                          'Korea, Republic of':'South Korea',
                          'Korea, South':'South Korea',
                          'Lao People\'s Democratic Republic':'Laos',
                          'Libyan Arab Jamahiriya':'Libya',
                          'Moldova, Republic of':'Moldova',
                          'Macedonia, the former Yugoslav Republic of':'North Macedonia',
                          'Russian Federation':'Russia',
                          'South Sudan':'Sudan',
                          'Syrian Arab Republic':'Syria',
                          'Taiwan, Province of China':'Taiwan',
                          'Taiwan*':'Taiwan',
                          'Tanzania, United Republic of':'Tanzania',
                          'US':'United States',
                          'Venezuela, Bolivarian Republic of':'Venezuela',
                          'Viet Nam':'Vietnam'}, 
                     inplace=True)

conf = SparkConf().setMaster("local").setAppName("CovidTracker")
sc = SparkContext(conf = conf)

response = urlopen("https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_global.csv")
pdf = pd.read_csv(response)

sqlContext = SQLContext(sc)

sdf = pandas_to_spark(pdf)

sdf.createOrReplaceTempView('covid_data')

TurkeyCovidDF = sqlContext.sql(
    """ SELECT * FROM covid_data --WHERE `Country/Region` = "Turkey" """
)

TCDF = TurkeyCovidDF.drop('Lat','Long')

PUPTCDF = TCDF.toPandas().set_index(['Country/Region','Province/State']).transpose()
PUPTCDF['Date'] = PUPTCDF.index
colnames = PUPTCDF.columns.tolist()
colnames = colnames[-1:] + colnames[:-1]
PUPTCDF = PUPTCDF[colnames]
PUPTCDF.reset_index(drop=True, inplace=True)

PUPTCDF.Date = pd.to_datetime(PUPTCDF.Date).dt.strftime('%d/%m/%Y') #Tarihi ISO formatından DD/MM/YYYY ye çeviriyor
PUPTCDF.to_excel('exports/Excel/Covid-Tracker/ww_city_covid_dataset.xlsx', engine='xlsxwriter')

#Şehir kırılımını kaldırıp tüm şehirleri ülke altında toplamak
first_tuple_elements = [a_tuple[0] for a_tuple in colnames]
PUPTCDF.columns = first_tuple_elements
CPUPTCDF = PUPTCDF.groupby(level=0, axis=1).sum()
cols = CPUPTCDF.columns.tolist()
cols.insert(0, cols.pop(cols.index('Date'))) 
CPUPTCDF = CPUPTCDF.reindex(columns = cols) 
CPUPTCDF.to_excel('exports/Excel/Covid-Tracker/ww_country_covid_dataset.xlsx', engine='xlsxwriter')

cols.remove("Date")
GCCCDF = pd.melt(CPUPTCDF, id_vars=['Date'], value_vars=cols, var_name='Country', value_name='Confirmed Case')
country_converter(GCCCDF, "Country")
GCCCDF.to_excel('exports/Excel/Covid-Tracker/ww_cc_covid_dataset.xlsx', engine='xlsxwriter')

#Ölen Caseler
response = urlopen("https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_global.csv")
DPDF = pd.read_csv(response)
DPDF = DPDF.set_index(['Country/Region','Province/State','Lat','Long']).transpose()
DPDF['Date'] = DPDF.index
colnames = DPDF.columns.tolist()
colnames = colnames[-1:] + colnames[:-1]
DPDF = DPDF[colnames]
DPDF.reset_index(drop=True, inplace=True)
DPDF.Date = pd.to_datetime(DPDF.Date).dt.strftime('%d/%m/%Y')
first_tuple_elements = [a_tuple[0] for a_tuple in colnames]
DPDF.columns = first_tuple_elements
DPDF = DPDF.groupby(level=0, axis=1).sum()
cols = DPDF.columns.tolist()
cols.insert(0, cols.pop(cols.index('Date'))) 
DPDF = DPDF.reindex(columns = cols)
cols.remove("Date")
DPDF = pd.melt(DPDF, id_vars=['Date'], value_vars=cols, var_name='Country', value_name='Deaths')
country_converter(DPDF, "Country")
DPDF.to_excel('exports/Excel/Covid-Tracker/ww_dg_covid_dataset.xlsx', engine='xlsxwriter')

#İyileşen Caseler
response = urlopen("https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_recovered_global.csv")
RPDF = pd.read_csv(response)
RPDF = RPDF.set_index(['Country/Region','Province/State','Lat','Long']).transpose()
RPDF['Date'] = RPDF.index
colnames = RPDF.columns.tolist()
colnames = colnames[-1:] + colnames[:-1]
RPDF = RPDF[colnames]
RPDF.reset_index(drop=True, inplace=True)
RPDF.Date = pd.to_datetime(RPDF.Date).dt.strftime('%d/%m/%Y')
first_tuple_elements = [a_tuple[0] for a_tuple in colnames]
RPDF.columns = first_tuple_elements
RPDF = RPDF.groupby(level=0, axis=1).sum()
cols = RPDF.columns.tolist()
cols.insert(0, cols.pop(cols.index('Date'))) 
RPDF = RPDF.reindex(columns = cols)
cols.remove("Date")
RPDF = pd.melt(RPDF, id_vars=['Date'], value_vars=cols, var_name='Country', value_name='Recovered')
country_converter(RPDF, "Country")
RPDF.to_excel('exports/Excel/Covid-Tracker/ww_rg_covid_dataset.xlsx', engine='xlsxwriter')

#Ülkeler
response = urlopen("https://raw.githubusercontent.com/eesur/country-codes-lat-long/master/country-codes-lat-long-alpha3.json")
response = json.loads(response.read().decode('utf-8'))
response = response["ref_country_codes"]
response = json.dumps(response)
countries = pd.read_json(response)
countries.rename(columns={'country':'Country'},inplace=True)
country_converter(countries, "Country")
non_standart_countries= [pd.Series(['West Bank and Gaza','none','none',-1,31.9522,35.2332], index=countries.columns),
pd.Series(['Kosovo','XK','none',-1,42.602636,20.902977], index=countries.columns),
pd.Series(['Diamond Princess','none','none',-1,0,0], index=countries.columns),
pd.Series(['MS Zaandam','none','none',-1,0,0], index=countries.columns)]
countries = countries.append(non_standart_countries, ignore_index=True)
countries.to_excel('exports/Excel/Covid-Tracker/countries.xlsx', engine='xlsxwriter')

#Total
JCDF = GCCCDF.merge(DPDF, on=['Country','Date']).merge(RPDF, on=['Country','Date']).merge(countries, how='left', on='Country')
JCDF.to_excel('exports/Excel/Covid-Tracker/ww_total_covid_dataset.xlsx', engine='xlsxwriter')
JCDF.to_csv('exports/csv/Covid-Tracker/ww_total_covid_dataset.csv')
#plt.plot(PUPTCDF['Date'], PUPTCDF['Turkey'])
#plt.show()