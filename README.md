<p align="center">
  <img width="750" height="300" src="https://github.com/ChunYen-Chang/Migrate-Local-Data-Storage-and-Process-to-AWS-Cloud/blob/master/images/logo.jpg">
</p>

# Migrate local data storage and process to AWS Cloud
#### PROJECT BACKGROUND AND SUMMARY
###### *BACKGROUND*
Sparkify is a startup company which provides the music streaming app. Recently, this compaby has grown their business and needs an efficient way to store and organize its growing-up amount of data. It decides to move their data from its on-premise data storage server to a cloud data lake solution--AWS S3. The data kept in the on-premise data storage server is in json format and the data relates to "user activity on the app" and "songs detailed information provided by Sparkify".

###### *PROJECT DESCRIPTION*
The analytics team in this company is interested in understanding their user activity on its music streaming app in order to provide better user experience for their user. However, due to the huge amount of incoming streaming user data, it is impossible to store all data in a on-premise data storage server. Also, the great amount of incoming streaming data also make conducting the data wrangling process in an on-premise stand-alone serve impossible. To overcome first problems, we decide to move the on-premise single data storage server to a cloud data lake (AWS S3) for accommodating the data. For second problem, we choose to launch a Spark cluster on AWS EMR and use this Spark cluster to tackle the data processing bottleneck problem. In sum, in this project, there are two goals. The first one is **moving local data to cloud data lake.** The second one is **using AWS EMR to do the data wrangling task by extracting data from S3, processing the data using Spark, and dumping the data back into S3 in a column-based data format to achieve storage-efficient.** 

###### *SYSTEM ARCHITECTURE*
<p align="center">
  <img width="850" height="275" src="https://github.com/ChunYen-Chang/Migrate-Local-Data-Storage-and-Process-to-AWS-Cloud/blob/master/images/system_architecture.jpeg">
</p>

###### *DETAILS AND DATA MODELING*
In this project, it will create a manually ETL data pipeline. This data pipeline is based on Spark cluster, this Spark cluster extracts JSON data from AWS S3, transforms data into a format which fits the analytical team wants, and loads the result back into S3 in parquet format. The data modeling for this project is using star schema model. There are five tables--one fact table and four dimensional tables. The fact table is songplay, it includes information about songplay history. The dimension tables are user, song, artist, and time. User table includes the user's personal information. Song table includes the song's information. Time table includes when a song is played. The structure can be seen in the below picture.



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


