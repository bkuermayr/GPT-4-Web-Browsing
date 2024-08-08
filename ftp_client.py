import pysftp
import os

# SFTP server details
sftp_host = '87.76.21.34'
sftp_username = 'ae2932a1_1'
sftp_password = 'UrbanFiledMedalsChimes'
remote_directory = '/chroot/home/ae2932a1/8f93964a1a.nxcli.io/var/rza-weclapp-sync/import/'  # Remote directory to change to
local_directory = './import'  # Local directory to download files to

def sftp_operations():
    # Define the connection options
    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None  # Skip host key checking (for simplicity, consider enabling in production)

    try:
        # Connect to the SFTP server
        with pysftp.Connection(host=sftp_host, username=sftp_username, password=sftp_password, cnopts=cnopts) as sftp:
            print(f"Connected to SFTP server {sftp_host}")
            
            # Verify current directory
            current_directory = sftp.pwd
            print(f"Current directory: {current_directory}")

            # List directories at the root level to verify path
            root_files = sftp.listdir('/')
            print("Root directory files and directories:")
            for file in root_files:
                print(file)

            # Change to the specified directory
            try:
                sftp.cwd(remote_directory)
                print(f"Changed directory to {remote_directory}")
                
                # Ensure local directory exists
                if not os.path.exists(local_directory):
                    os.makedirs(local_directory)
                
                # Download the entire directory
                sftp.get_r('.', local_directory)
                print(f"Downloaded {remote_directory} to {local_directory}")
                    
            except Exception as e:
                print(f"Error changing directory or downloading files: {e}")

    except Exception as e:
        print(f"Error: {e}")

def upload_file(local_file_path, remote_file_path):
    # Define the connection options
    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None  # Skip host key checking (for simplicity, consider enabling in production)

    try:
        # Connect to the SFTP server
        with pysftp.Connection(host=sftp_host, username=sftp_username, password=sftp_password, cnopts=cnopts) as sftp:
            print(f"Connected to SFTP server {sftp_host}")
            
            # Upload the file
            sftp.put(local_file_path, remote_file_path)
            print(f"Uploaded {local_file_path} to {remote_file_path}")

    except Exception as e:
        print(f"Error: {e}")

# Execute the operations
if __name__ == '__main__':
    sftp_operations()
    # Example usage of upload_file
    upload_file('path/to/local/file.txt', remote_directory + 'file.txt')