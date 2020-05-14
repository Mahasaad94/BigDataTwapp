
# Twitter Data Analysis

# Technology
.Python<br /> 
.Pyspark<br />
.Flask<br />
.Matplotlib<br />
.Pandas<br />

# start the app 
.$ python myTwaapp.py<br />
. got to http://127.0.0.1:5000/
# to run Docker image : 
.$ docker build -t twitter_app .
.$ docker run --rm -p 8888:8888 -v $(pwd):/app --name my_twitter_app twitter_app
