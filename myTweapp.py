import sys
import os
import conda


from pyspark import SparkContext, sparkConf
#import  pyspark
import findspark
#findspark.init('/path_to_spark/spark-x.x.x-bin-hadoopx.x')
import py4j
import pyspark


from pyspark.sql import SparkSession
import matplotlib.pyplot as plt
import pandas
import matplotlib.ticker as ticker


conda_file_dir = conda.__file__
conda_dir = conda_file_dir.split('lib')[0]
proj_lib = os.path.join(os.path.join(conda_dir, 'share'), 'proj')
os.environ["PROJ_LIB"] = proj_lib
from mpl_toolkits.basemap import Basemap


def init_folder(filename):
    folder = outs_folder + filename
    os.system("rm -rf " + folder)
    return folder

def save_to_folder(df, folder, filename ):
    plt.savefig(plots_folder+filename+".png", dpi = 1200)
    df.rdd.coalesce(1, True).saveAsTextFile(folder)
    plt.close()



# Top 10 languages used in tweets
def query1():
    filename = "query1p"
    folder = init_folder(filename)

    df = spark.sql("SELECT  place.country, count(*) AS c from table WHERE place.country is not null GROUP BY Place.country ORDER BY c DESC")
    x = df.toPandas()["country"].values.tolist()[:10]
    y = df.toPandas()["c"].values.tolist()[:10]
    total_number_of_tweets = sum(df.toPandas()["c"].values.tolist())
    print('total_number_of_tweets', total_number_of_tweets)     
    plt.bar(x,y)
    plt.title("Tcountry post about COVID19")
    plt.xlabel("countrey")
    plt.ylabel("counts")
    save_to_folder(df, folder, filename)


def query2():
      filename = "query2p"
      folder = init_folder(filename)
      tweets_dist_person = spark.sql(" SELECT   user.id_str, COUNT(user.id_str) AS count from table WHERE user.id_str is not null and entities.hashtags[0].text in ('coronavirus','Coronavirus','CoronaVirus','Coronavirus','COVID19') GROUP BY user.id_str ORDER BY count DESC")
      x = tweets_dist_person.toPandas()["id_str"].values.tolist()[:10]
      y = tweets_dist_person.toPandas()["count"].values.tolist()[:10]
      total_number_of_tweets = sum(tweets_dist_person.toPandas()["count"].values.tolist())
      print('total_number_of_tweets', total_number_of_tweets)
      figure = plt.figure()
      axes = figure.add_axes([0.35, 0.1, 0.60, 0.85])
      plt.barh(x,y, color = 'blue')
      plt.title("Top 10 Tweeters about coronavirus")
      plt.ylabel("User id")
      plt.xlabel("Number of Tweets")
      save_to_folder(tweets_dist_person, folder, filename)

def query3():
    filename = "query3p"
    folder = init_folder(filename)

    hashtagsDF = spark.sql("SELECT hashtags, COUNT(*) AS count FROM (SELECT explode(entities.hashtags.text) AS hashtags FROM table) WHERE hashtags IS NOT NULL GROUP BY hashtags ORDER BY count DESC")

    # # # Pie chart, where the slices will be ordered and plotted counter-clockwise:
    labels = hashtagsDF.toPandas()["hashtags"].values.tolist()[:10]
    sizes = hashtagsDF.toPandas()["count"].values.tolist()[:10]
    fig1, ax1 = plt.subplots()
    ax1.pie(sizes, labels=labels,labeldistance = 1.1, autopct='%1.1f%%', shadow=False, startangle=360)
    ax1.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle.
    plt.title("Hashtags Distribution")
    save_to_folder(hashtagsDF, folder, filename)

def query4():
    filename = "query4p"
    folder = init_folder(filename)

    tweets_from_country = spark.sql("SELECT place.country_code, COUNT(*) AS count FROM table WHERE place.country_code IS NOT NULL GROUP BY place.country_code ORDER BY count DESC")
#display(tweets_from_country)
    x = tweets_from_country.toPandas()["country_code"].values.tolist()[:10]
    number_of_tweets_from_country = tweets_from_country.toPandas()["count"].values.tolist()
    y = number_of_tweets_from_country[:10]

    plt.rcParams.update({'axes.titlesize': 'small'})  
    plt.barh(x,y, color = 'red')
    plt.title("Top 10 Country Codes Available In Tweets")
    plt.ylabel("Countries")
    plt.xlabel("Number of Tweets")
    save_to_folder(tweets_from_country, folder, filename)

def query5():
    filename = "query5p"
    folder = init_folder(filename)

    import matplotlib.ticker as ticker
    tweet_distributionDF = spark.sql("SELECT SUBSTRING(created_at,12,5) as time_in_hour, COUNT(*) AS count FROM table GROUP BY time_in_hour ORDER BY time_in_hour ")
    x = pandas.to_numeric(tweet_distributionDF.toPandas()["time_in_hour"].str[:2].tolist()) + pandas.to_numeric(tweet_distributionDF.toPandas()["time_in_hour"].str[3:5].tolist())/60
    y = tweet_distributionDF.toPandas()["count"].values.tolist()

    tick_spacing = 1
    fig, ax = plt.subplots(1, 1)
    ax.plot(x, y)
    ax.xaxis.set_major_locator(ticker.MultipleLocator(tick_spacing))

    plt.title("Tweets Distribution By Minute")
    plt.xlabel("Hours (UTC)")
    save_to_folder(tweet_distributionDF, folder, filename)

def query6():
    filename = "query6p"
    folder = init_folder(filename)
    tweets_from_USA = spark.sql("SELECT user.location, COUNT(*) AS count FROM table WHERE user.location LIKE '%USA%' GROUP BY user.location ORDER BY count DESC")
    # # Pie chart, where the slices will be ordered and plotted counter-clockwise:
    labels = tweets_from_USA.toPandas()["location"].values.tolist()[:10]
    sizes = tweets_from_USA.toPandas()["count"].values.tolist()[:10]
    explode = (0.1, 0, 0, 0, 0, 0, 0, 0, 0, 0 )  # only "explode" the 1st slice
    fig1, ax1 = plt.subplots()
    ax1.pie(sizes, explode=explode, labels=labels, autopct='%1.1f%%', shadow=False, startangle=90)
    ax1.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle.
    plt.title("Tweets Distribution in USA")
    save_to_folder(tweets_from_USA, folder, filename)

def query7():
    filename = "query7p"
    folder = init_folder(filename)



    coord = spark.sql("SELECT coordinates.coordinates FROM table WHERE coordinates IS NOT NULL")
    coordDF = coord.select(coord.coordinates[0], coord.coordinates[1])
    x = coordDF.toPandas()["coordinates[0]"].values.tolist()[:10]
    y = coordDF.toPandas()["coordinates[1]"].values.tolist()[:10]

    m = Basemap(projection='merc', llcrnrlat=-80, urcrnrlat=80, llcrnrlon=-180, urcrnrlon=180, lat_ts=20, resolution='c')
  #  m.drawcoastlines()
    #m.drawcountries()
   # m.drawlsmask(land_color='coral',ocean_color='aqua',lakes=True)
    m.fillcontinents(color='coral', lake_color='#FFFFFF')

    m.drawmapboundary(fill_color='#FFFFFF')

    for i in range(len(x)):
        x1, y1 = m(x[i], y[i])
        m.plot(x1, y1, 'r.')

    plt.title("GPS Coordinates of the Twitter Accounts")

    save_to_folder(coord, folder, filename)

def query8():
    filename = "query8p"
    folder = init_folder(filename)

    q1 = spark.sql(" SELECT count(extended_tweet.full_text) from table where lower(extended_tweet.full_text) like '%coronavirus%' and  lower(extended_tweet.full_text) like '%china%' ")
    q2= spark.sql(" SELECT count(extended_tweet.full_text) from table where lower(extended_tweet.full_text) like '%coronavirus%' and  lower(extended_tweet.full_text) like '%covid19%'")
    q3 = spark.sql("SELECT count(extended_tweet.full_text) from table where lower(extended_tweet.full_text) like '%coronavirus%' and  lower(extended_tweet.full_text) like '%usa%'")
    q4 = spark.sql("SELECT extended_tweet.full_text from table where lower(extended_tweet.full_text) like '%coronavirus%' and  lower(extended_tweet.full_text) like '%ital%'")
    from pyspark.sql import Row

    india = Row(country='india', number='2')
    italy = Row(country='italy', number='80')
    japan = Row(country='japan', number='9')
    usa = Row(country='usa', number='13')
    china = Row(country='china', number='76')
    france = Row(country='france', number='6')
    dfcon = spark.createDataFrame([india,italy,japan,usa,france,china])
    dfcon.createOrReplaceTempView("countrytweet")
    dfcon1=spark.sql('SELECT country,number from countrytweet')

    x = dfcon1.toPandas()["country"].values.tolist()
    y = dfcon1.toPandas()["number"].values.tolist()
#print(y)
    plt.bar(x,y)


    save_to_folder(dfcon1, folder, filename)

def query9():
    filename = "query9p"
    folder = init_folder(filename)

    df3=spark.sql("SELECT entities.hashtags[0].text, count(*)as c from table where entities.hashtags[0].text is not null group by entities.hashtags[0].text order by c desc limit 6")
    c=spark.sql("SELECT count(entities.hashtags[0].text) from table")
    #display(df3)


    x = df3.toPandas()["entities.hashtags AS `hashtags`[0].text"].values.tolist()[:10]
    y = df3.toPandas()["c"].values.tolist()[:10]
#print(y)
    plt.bar(x,y)
    save_to_folder(df3, folder, filename)







if __name__ == "__main__":

#   configuration part
    plots_folder = './plots/'
    outs_folder = './outs/'

    if not os.path.exists('plots'):
       os.mkdir('plots')
       print('Directory plots created.')
    else:
       print('Directory plots already exists. Deleting the content')
       os.system('rm -f ./plots/*') 


    if not os.path.exists('outs'):
       os.mkdir('outs')
       print('Directory outs created')
    else:
       print('Directory outs already exists. Deleting the content')
       os.system('rm -rf ./outs/*') 


    print("Hello PySPark Application Started ...")
    spark = SparkSession.builder.appName("HelloWorld PySpark Application").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("INFO")

    tweetsDF = spark.read.json("./data/twitter_data.json", multiLine=False)
    tweetsDF.createOrReplaceTempView("table")

    query1()
    query2()
    query3()
    query4()
    query5()
    query6()
    query7()
    query8()
    query9()
   

    spark.stop()
    print("PsSpark completed and cleaning up")
    os.system('rm -rf spark-warehouse')