# PowerShell-DatabaseBackupCompressSendAndEtract
Powershell script to backup MSSQL database, compress database into chunks, send database to hosts and decompress database on hosts.

###Dependencies:
1. 7Zip must be installed on the hosts and the origin server. Script is currently using 64 bit. If you are using 32bit, please modify the script file to reflect the path accordingly.

2. A folder (as per usage) must exists on all hosts and origin server. 

3. Elevated PowerShell Permissions may be required to run the script. 

4. In order to receive emails, please update the SMTP Server. 


###Usage of the script is as follows:

script.ps1 -db myDB -OriginServer myServer -Hosts "server1,server2,server3" -FileName File1 -ToFolder Folder1 -EmailAddress "name@company.com" -Chunk 256M
