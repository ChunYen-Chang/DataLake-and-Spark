<p align="center">
  <img width="750" height="300" src="https://upload.cc/i1/2019/09/02/xE82LS.jpg">
</p>

# Data lake for Sparkify (By using Spark + AWS S3)
#### PROJECT BACKGROUND AND SUMMARY
###### *BACKGROUND*
Sparkify is a startup company which provides the music streaming app. Recently, this compaby has grown their business and needs an efficient way to store and organize its growing-up amount of data. It decides to move their data from its on-premise database to a cloude data lake solution--AWS S3. This data lake includes two kinds of data, "user activity on the app" and "songs data". Both of them are in json format.

###### *PROJECT DESCRIPTION*
The analytics team in this company is interested in understanding their user activity on its music streaming app in order to provide better user experience for their user. However, due to the huge amount of data resides in data lake, it is impossible to do the ETL task by stand-alone computer. To overcome this problem, the analytical team decides to build a Spark cluster and use this Spark cluster to do the ETL take. Thus, in this project, it aims for **building an ETL pipeline that extracts data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables.** This will help their analytics team to find insights from their users.

###### *DETAILS AND DATA MODELING*
In this project, it will create an ETL data pipeline. This data pipeline is based on Spark cluster, this Spark cluster extracts JSON data from AWS S3, transforms data into a format which fits the analytical team wants, and loads the result back into S3 in parquet format. The data modeling for this project is using dimensional model. There are five tables--one fact table and four dimensional tables. The fact table is songplay, it includes information about songplay history. The dimension tables are user, song, artist, and time. User table includes the user's personal information. Song table includes the song's information. Time table includes when a song is played. The structure can be seen in the below picture.

<p align="center">
  <img src="https://upload.cc/i1/2019/08/25/gM9qd6.jpg">
</p>

------------
#### FILES IN THE REPOSITORY
1. **etl.py**: a python script which is used for launching a Spark Cluster, getting data from Sparkify data lake (AWS S3), transforming data into a format which Sparkify analytical team wants, and load the result back to S3

2. **dl.cfg**: a configuration file which contains the necessary information of connecting to S3

3. **test_code.ipynb**: a jupyter notebook file which is written for developing the python code for etl.py. If you want to do some further modifications, please run the command in this file first and put some other codes you want.

------------
#### HOW TO RUN THE PROJECT
To run the project, you only need **etl.py**. Steps are listed below.
1. Modify **dl.cfg** file. You need modify the content in [AWS CREDS]--to fill your AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY. If you need any further helps, please check the AWS document. The link is below. https://docs.aws.amazon.com/general/latest/gr/managing-aws-access-keys.html

2. Find the path of your **spark-submit** file. You can tyoe `which spark-submit` in your terminal to find the path. In my case, this file path is **/opt/conda/bin/spark-subnit**

3. type `/opt/conda/bin/spark-subnit --master yarn ./etl.py` in your terminal to start the process of launching a Spark Cluster, getting data from Sparkify data lake (AWS S3), transforming data into a format which Sparkify analytical team wants, and load the result back to S3


