<p align="center">
  <img width="750" height="300" src="https://github.com/ChunYen-Chang/Migrate-Local-Data-Storage-and-Process-to-AWS-Cloud/blob/master/images/logo.jpg">
</p>

# Migrate local data storage and process to AWS Cloud
#### PROJECT BACKGROUND AND SUMMARY
###### *BACKGROUND*
A startup company which provides the music streaming app recently has grown their business and needs an efficient way to store and organize its growing-up amount of data. It decides to move their data from its on-premise data storage and processing local server to a cloud data lake solution--AWS S3 and cloud data processing platform--AWS spark cluster. 

###### *PROJECT DESCRIPTION*
This company has two different kind of data. The first one is the data about the user activity on the app. The second one is the data about the song the company provides. These two data are stored in JSON format. Now, the analytics team in this company is interested in understanding their user activity based on this two kinds of data in order to provide better APP user experience. However, due to the great amount of data, it is hard to store all data in a on-premise data storage server and impossible to conduct the data wrangling process in an on-local stand-alone serve. To tackle the storage capacity problem, we decide to use the cloud data lake (AWS S3) to replace the original on-premise single data storage server for accommodating the great amount of data. To deal with the data processing problem, we choose to launch a Spark cluster through AWS EMR and use this Spark cluster to consume the great amount of data and do the data processing tasks. Apart from this, we also store the after-processing data in PARQUET format to maxmize the storage efficient in cloud data lake. In sum, in this project, there are two goals. The first one is **moving local data to cloud data lake.** The second one is **using AWS EMR to do the data wrangling task by extracting data from S3, processing the data using Spark, and dumping the data back into S3 in a Parquet format to achieve storage-efficient.** 

#### SYSTEM ARCHITECTURE
<p align="center">
  <img width="850" height="275" src="https://github.com/ChunYen-Chang/Migrate-Local-Data-Storage-and-Process-to-AWS-Cloud/blob/master/images/system_architecture.jpeg">
</p>

The picture shows the whole system structure. The left side is about moveing data from local to cloud data lake,and, the right side is about using cloud Spark cluster to execute the data wrangling process. In this project, we use the AWS boto3 packages, which is a python toolbox developed by AWS for allowing python developers to interact woth AWS service through AWS RESTful APIs, to program all python codes. For example, there two code scripts. The first script allows us to create a directory in the cloud data lake, scan all files in one local directory and upload these files to this cloud data lake each by each. The second script allows us to start a cloud Spark cluster, extract data from a cloud data lake directory, put data in the Spark cluster for data processing, dump after-processing data back to another directory in the cloud data lake, and terminate the Spark cluster. 
The benefits of using boto3 are two. First, user don't have to interact with AWS though AWS website dashboard. From our experience, when using AWS service though the AWS website dashboard, users are likely click wrong bottoms or forget to give some important system parameter and it usually cause unpredictable problems. By using boto3, we can build a standard process of launching and using these AWS service. The second benefit is we can execute the python code in an automatic way. If we want to execute the code automatically, we use crontab in Linux to trigger this code in a specific time. We just degine relating settings and let computers to finish all tasks. Engineers like to make something to make their life easier, is it :)

------------
#### FILES IN THE REPOSITORY
1. **README.md**: An introduction to this project

2. **image directory**: A directory for keeping images for this project

3. **code directory**: A directory for keeping all python scripts
    - **Function_ConnectAWS.py**: contain the self-defined funcions for connecting to AWS service
    - **Function_Encryption.py**: contain the self-defined funcions for encrypt and decrypt the AWS key and secret key
    - **Function_SSH.py**: contain the self-defined funcions for connection to AWS Spark master node by SSH
    - **ETL_DataProcessbySpark.py**: the python script for Spark submit command
    - **ETL_CloudProcessing.py**: the python script for starting a cloud Spark cluster, extracting data from a cloud data lake directory, putting data in the Spark cluster for data processing, dumping after-processing data back to another directory in the cloud data lake, and terminating the Spark cluster. 
    - **ETL_LocaltoCloudDataLake.py**: the python script for creating a directory in cloud data lake and uploading data from local directory
    - **AWS_key**: a directory which contains the AWS_key file

------------
#### HOW TO RUN THE PROJECT
To run the project, You need to create an AWS account and get your AWS key / secret key. Then, execute **ETL_LocaltoCloudDataLake.py** and **ETL_CloudProcessing.py** python script in your local machine. That it. Enjoy it.
