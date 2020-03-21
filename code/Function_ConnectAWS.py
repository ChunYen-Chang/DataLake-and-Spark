# import necessary packages
import boto3
import os
import Function_Encryption


class ConnectAWS():
    def create_session(self, aws_key, aws_secret_key):
        """
        Description: Create a session with AWS
        Parameters: aws_key - encrypted aws key
                    aws_secret_key - encrypted aws secret key
        Return: session
        """
        # decrypt aws_key and aws_secret_key
        Encryption_obj = Function_Encryption.AWSEncryption()
        aws_key = Encryption_obj.decrypt_AWSKEY(aws_key)
        aws_secret_key = Encryption_obj.decrypt_AWSKEY(aws_secret_key)
    
        # initiate a session
        try:
            session = boto3.session.Session(aws_access_key_id = aws_key,
                                       aws_secret_access_key = aws_secret_key)
            return session
        except:
            print('Error: Please check your AWS key and password !')
        

    def choose_AWS_service(self, session, AWS_service, Region = None):
        """
        Description: Connect to a specific AWS service
        Parameters: session - the session created by create_session function
                    AWS_service - the AWS service name you want to connect
                    Region - define the AWS server region
        Return: session
        """
        session = session.client(AWS_service, region_name = Region)
        return session


    def check_bucket_exist(self, session, bucket_name):
        """
        Description: Check a bucket exist or not
        Parameters: session - the session created by choose_AWS_service function
                                with S3 service
                    bucket_name - the bucket name you want to check
        Return: True / False
        """
        response = session.list_buckets()
        Buckets_list = response['Buckets']
        for each_bucket in Buckets_list:
            if each_bucket['Name'] == bucket_name:
                print('Error: Bucket already existed')
                return False
        return True
    
    
    def create_bucket(self, session, bucket_name):
        """
        Description: Create a bucket in AWS S3
        Parameters: session - the session created by choose_AWS_service function 
                              with S3 service
                    bucket_name - the bucket name you want to create
        Return: None
        """
        # check the bucket doesn't exist
        if not self.check_bucket_exist(session, bucket_name):
            return
        
        # create a bucket
        session.create_bucket(Bucket = bucket_name)
        print('Success: Bucket created')
    

    def upload_file_S3(self, session, target_bucket, local_dir):
        """
        Description: Upload local files to AWS S3 bucket
        Parameters: session - the session created by choose_AWS_service function
                              with S3 service
                    target_bucket - the bucket you want to dump data files
                    local_dir - the local directory you keep uploaded files
        Return: None
        """
        file_list = []
        count = 0
    
        # check the bucket doesn't exist
        if not self.check_bucket_exist(session, bucket_name):
            return
    
        # get all files in a specific directory
        for (dirpath, dirname, filenames) in os.walk(local_dir):
            for each_filename in filenames:
                temp_path = dirpath + '/' + each_filename
                temp_filename = each_filename
                file_list.append([temp_filename, temp_path])

        # upload local files to AWS S3
        for each_file in filename_list:       
            local_file_name = each_file[0]
            S3_filename = each_file[1]
            session.upload_file(local_file_name, target_bucket, S3_filename)

            count += 1
            print('Process: File ' + count + ' is finished')
        print('Success: All files are uploaded')
    
    
    def return_EMR_status(self, session, target_EMR):
        """
        Description: Retuen an EMR status
        Parameters: session - the session created by choose_AWS_service function 
                              with EMR service
                    target_EMR - the EMR cluster name you want to check
        Return: cluster status / False
        """
        cluster_status = None

        response = session.list_clusters()
        cluster_infs = response['Clusters']
        for each_inf in cluster_infs:
            #print(each_inf
            if each_inf['Name'] == target_EMR:
                cluster_status = each_inf['Status']['State']

        #if clusters_list[]
        if not cluster_status:
            print('Error: Cannot find inf !')
            return False
        else:
            return cluster_status
        
        
    def create_EMR(self, session, cluster_name, cluster_log_location, EC2key, SubnetId):
        """
        Description: Create an EMR cluster
        Parameters: session - the session created by choose_AWS_service function
                              with EMR service
                    cluster_name - the cluster name you want
                    cluster_log_location - a s3 bucket location for storing this cluster
                                           log information, please create this bucket in
                                           the beginning
                    EC2key - a SSH key for you to log in the cluster's master node. Please
                             create this SSH in a .pem formate in your AWS console in the
                             beginning. When you run this function, the function will check
                             the SSH you create in your AWS console.
                    SubnetId - Your subnet Id, you can find the information in VPC dashboard.
        Return: None
        Note: When you choose the ReleaseLabel parameter, please be aware that each released
              version has different packages. It means that maybe in one version, you can find
              some packages but you cannot find these packages in another released version
        """
        # start an EMR cluster
        session.run_job_flow(
            Name = cluster_name,
            LogUri = cluster_log_location,
            ReleaseLabel = 'emr-5.19.0',
            Instances = {
               'InstanceGroups': [
                    {
                        'Name': 'Master',
                        'Market': 'ON_DEMAND',
                        'InstanceRole': 'MASTER',
                        'InstanceType': 'm5.xlarge',
                        'InstanceCount': 1,
                    },
                    {
                        'Name': "Slave",
                        'Market': 'ON_DEMAND',
                        'InstanceRole': 'CORE',
                        'InstanceType': 'm5.xlarge',
                        'InstanceCount': 2,
                    }
                ],
                'Ec2KeyName': EC2key,
                'KeepJobFlowAliveWhenNoSteps': True,
                'TerminationProtected': False,
                'Ec2SubnetId': SubnetId,
            }, 
            Applications = [
                {
                    'Name': 'Hadoop',
                },
                {
                    'Name': 'Spark',
                },
                {
                    'Name': 'Livy',
                },
                {
                    'Name': 'Hue',
                }
            ],
            VisibleToAllUsers = False,
            JobFlowRole = 'EMR_EC2_DefaultRole',
            ServiceRole = 'EMR_DefaultRole')

        # check whether we launch the cluster successfully or not
        EMRresponse = self.return_EMR_status(session, cluster_name)
        if EMRresponse in ['RUNNING', 'WAITING', 'STARTING']:
            print('Success: EMR cluster created')
        else:
            print('Error: Failed to create an EMR cluster')    
            
            
    def teiminate_EMR(self, session, cluster_name):
        """
        Description: Terminate an EMR cluster
        Parameters: session - the session created by choose_AWS_service function
                              with EMR service
                    cluster_name - the cluster you want to terminate
        Return: True / False
        """
        cluster_id = None

        # find JobFlowIds
        response = session.list_clusters()
        cluster_infs = response['Clusters']
        for each_inf in cluster_infs:
            if each_inf['Name'] == cluster_name:
                cluster_id = each_inf['Id']
                break
        if not cluster_id:
            print('Error: EMR cluster does not exist')
            return 

        # terminate EMR cluster
        session.terminate_job_flows(JobFlowIds = [cluster_id])

        # check the EMR status
        EMRresponse = self.return_EMR_status(session, cluster_name)
        if EMRresponse in ['TERMINATING', 'TERMINATED']:
            print('Success: EMR cluster terminates')
        else:
            print('Error: Failed to terminate this EMR cluster')