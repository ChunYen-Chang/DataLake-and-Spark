# import necessary packages
import paramiko 


class SSHtoEMRCluster():
    def connect_server(self, pem_file, EMR_master_dns, EMR_user_name = 'ubuntu', EMR_ssh_port = 22):
        """
        Description: Create a connection with EMR master node
        Parameters: pem_file - ssh pem file for logging in the EMR master node
                    EMR_master_dns - the dns of EMR master node
                    EMR_user_name - EMR cluster username, default is ubuntu
                    EMR_ssh_port - EMR ssh port, default is 22
        Return: client, which is a connection object
        """
        ssh_key = paramiko.RSAKey.from_private_key_file(pem_file)
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(hostname = EMR_master_dns, port = EMR_ssh_port, username = EMR_user_name, pkey = ssh_key)
        return client

    
    def upload_file(self, client, local_file_path, remote_path):
        """
        Description: Upload a file to EMR master node
        Parameters: client - the connection object created by connect_server function
                    local_file_path - the path of the file that you want to upload
                    remote_path - the path in EMR master node server you want the 
                                  uploaded file
        Return: None
        """
        sftp = client.open_sftp()
        sftp.put(local_file_path, remote_path)

        
    def execute_command(self, client, command):
        """
        Description: Execute commands in EMR master node 
        Parameters: client - the connection object created by connect_server function
                    command - commands you want to execute
        Return: None
        """
        client.exec_command(command)

        
    def close_connection(self, client):
        """
        Description: Close the connection with EMR cluster node
        Parameters: client - the connection object created by connect_server function
        Return: None
        """
        client.close()

    
