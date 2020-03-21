# import necessary packages
import ConnectAWS_function
import SSH_function
import ETL_DataProcessbySpark


def main():
    """
    Description: Help users make a connection to AWS service, create an EMR
                 cluster, check the cluster status, and terminate the cluster
    Parameters: None
    Return: None
    """
    # settings
    EMR_Region = 'us-west-2'
    EMR_cluster_name = 'musicprocessing'
    EMR_log_location = 's3://emr_log'
    EMR_ssh_key_file = 'fakesshfilename'
    EMR_ssh_key_file_path = '/ubuntu/fakesshfilepath'
    EMR_subnet_name = 'subnet-01b6c7671b680c8e8'
    EMR_master_dns = '192.168.2.129'
    EMR_user_name = 'ubuntu'    
    uploaded_file_path = '/admistrator/ETL_DataProcessbySpark.py'
    remote_path = '/ubuntu/dataprocessing/'

    # Load encrypted AWS key and AWS secret key
    with open('AWS_key/dl.cfg', 'r') as f:
        code = f.read()
    AWS_encrypted_key = code.split('\n')[1].split('=')[1]
    AWS_encrypted_secret_key = code.split('\n')[2].split('=')[1]
    AWSAccessKeyId = AWS_encrypted_key
    AWSSecretKey = AWS_encrypted_secret_key

    # creat a connection
    AWS_connection = ConnectAWS()
    
    # choose EMR service
    asession = AWS_connection.create_session(AWSAccessKeyId, AWSSecretKey)
    EMR_session = AWS_connection.choose_AWS_service(asession, 'emr', Region = EMR_Region)

    # create a cluster
    AWS_connection.create_EMR(EMR_session, EMR_cluster_name, EMR_log_location, EMR_ssh_key_file, EMR_subnet_name)

    # check cluster status
    AWS_connection.return_EMR_status(EMR_session, EMR_cluster_name)
    
    # upload ETL_DataProcessbySpark.py to EMR cluster for following data processing
    ssh = SSH_function()
    ssh_client = ssh.connect_server(EMR_ssh_key_file, EMR_master_dns, EMR_user_name)
    ssh.upload_file(ssh_client, uploaded_file_path, remote_path)
    ssh.close_connection(ssh_client)
    
    # ssh to cluster master node to rum spark submit command
    ssh = SSH_function()
    ssh_client = ssh.connect_server(EMR_ssh_key_file, EMR_master_dns, EMR_user_name)
    ssh.execute_command('/bin/spark-submit --master yarn --deploy-mode cluster /ubuntu/dataprocessing/etl.py')
    ssh.close_connection(ssh_client)
    
    # tetminate this cluster
    AWS_connection.teiminate_EMR(EMR_session, EMR_cluster_name)

    
if __name__ == '__main__':
    main()
