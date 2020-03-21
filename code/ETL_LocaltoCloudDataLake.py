# import necessary package
import Function_ConnectAWS


def main():
    """
    Description: This function helps users make a connection to AWS service, 
                 create a directory in Cloud data lake(AWS S3), and upload
                 data from local directory to the directory in Cloud data lake
    Parameters: None
    Return: None
    """
    # settings
    bucket_name = 'musiccompany_raw_data'
    local_dir = '/Users/adminstrator/rawdata'
    
    # Load encrypted AWS key and AWS secret key
    with open('AWS_key/aws_key.cfg', 'r') as f:
        code = f.read()
    AWS_encrypted_key = code.split('\n')[1].split('=')[1]
    AWS_encrypted_secret_key = code.split('\n')[2].split('=')[1]
    AWSAccessKeyId = AWS_encrypted_key
    AWSSecretKey = AWS_encrypted_secret_key
    
    # create an object
    AWS_connection = Function_ConnectAWS.ConnectAWS()
    
    # connect to AWS
    asession = AWS_connection.create_session(AWSAccessKeyId, AWSSecretKey)
    
    # choose s3 service
    s3_session = AWS_connection.choose_AWS_service(asession, 's3')
    
    # create a bucket, the bucket name is musiccompany_raw_data
    AWS_connection.create_bucket(s3_session, bucket_name):
    
    # upload local data to cloud data lake
    AWS_connection.upload_file_S3(s3_session, bucket_name, local_dir)

    
if __name__ == '__main__':
    main()
