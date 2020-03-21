# import necessary packages
from simplecrypt import encrypt, decrypt
from base64 import b64encode, b64decode
from getpass import getpass  


class AWSEncryption():    
    def encrypt_AWSKEY(self, key_content):
        """
        Description: This function allows user to encrypt AWS KEY
        Parameters: key_content - AWS key information
        Return: None
        """
        # ask user type password
        password = getpass()

        # encrypt AWS key
        cipher = encrypt(password, key_content)
        encoded_cipher = b64encode(cipher)

        # convert bytes to string
        encoded_cipher = str(encoded_cipher, encoding = 'utf-8')
        return encoded_cipher


    def decrypt_AWSKEY(self, encrypt_content):
        """
        Description: This function allows user to decrypt the  encrypted AWS KEY
        Parameters: encrypt_content - encrypted AWS KEY
        Return: None
        """
        # ask user type password
        password = getpass()

        # convert string to bytes and do decrypt
        encrypt_content = bytes(encrypt_content, encoding = 'utf-8')
        cipher = b64decode(encrypt_content)
        key_content = decrypt(password, cipher)

        # convert bytes to string
        key_content = str(key_content, encoding = 'utf-8')
        return key_content
    
    