# Usage:
# script.ps1 -db myDB -OriginServer myServer -Hosts "server1,server2,server3" -FileName File1 -ToFolder Folder1 -EmailAddress "name@company.com" -Chunk 256M

param (
    [string]$db = $(throw "db is required."),
	[string]$OriginServer = $(throw "OriginServer is required."),
	[string]$Hosts = $(throw "Hosts is required."),
	[string]$FileName = $(throw "FileName is required."),
	[string]$ToFolder = $(throw "ToFolder is required."),
	[string]$EmailAddress = $(throw "EmailAddress is required."),
	[string]$Chunk = "256M"
 )
 
Start-transcript

#######################################################################################
#Variables
#######################################################################################
$cmdTimeOut = 2 * 60 * 60
$sqlConnection = new-object System.Data.SqlClient.SqlConnection
$sqlCMD = new-object System.Data.SqlClient.SqlCommand


#SMTP Variables
$emailFrom = "ITINFO <info@noreply.com>"
$emailTo = $EmailAddress
$smtpServer = " "    #insert smtp server 

  
$subject = "Backup upgraded database, compress and distribute database across servers. "
$body = "1. Backup database 2.Compress database 3.Distribute compressed chuncks to servers. 4. Uncompress Chuncks"
$smtp = new-object Net.Mail.SmtpClient($smtpServer)
$smtp.Port = 25
#25252
#######################################################################################
#End of Variables
#######################################################################################

#######################################################################################
#Functions
#######################################################################################
function SendEmail([string]$Subject,[string]$Body)
{
   $smtp.Send($emailFrom, $emailTo, 'Migration: '+$Subject, $Body)
}

function UploadFileChunk ([string]$path)
{
    
    $file = Get-Item $path
    
    SendEmail ('Sending Chunk '+$file.name) ('Chunk '+$file.name+' being sent to servers.')
    Write-Host ([datetime]::Now) 'Uploading file '$file.name
    
    foreach ($DHost in $AllHosts)
	{
	trap [Exception]
        {
            SendEmail ('Failed sending '+$path) ('Failed sending chunk '+$path+' to '+$DHost)
            break
        }
        $file = Get-Item $path
        Copy-Item $path ('\\'+$DHost+'\C$\'+$ToFolder+'\'+$file.name)
    }
    
    SendEmail ('Chunk '+$file.name+' sent.') ('Chunk '+$file.name+' has been sent to servers.')
    Write-Host ([datetime]::Now) 'Finished uploading chunk ' $file.name
}

function BackupDatabase
{
    $sqlConnection.ConnectionString = "server="+$OriginServer+";integrated security=true;database="+$db
    Write-Host ([datetime]::Now) "Opening connection to "$OriginServer" Database."
    $sqlConnection.Open()
    #
    ## Create the Command Object
    #
    $sqlCMD.Connection = $sqlConnection
    $sqlCMD.CommandText = "BACKUP DATABASE "+$db+" TO DISK='C:\"+$ToFolder+"\"+$FileName+".bak' WITH COPY_ONLY"
    $sqlCMD.CommandTimeout = $cmdTimeOut
    #
    ## Execute the Backup command
    #
    $body = "Backing up Database."
    Write-Host ([datetime]::Now)  $body
    Remove-Item ('\\'+$OriginServer+'\C$\'+$ToFolder+'\'+$FileName+'.bak') -ErrorAction 'SilentlyContinue'
    $sqlCMD.ExecuteNonQuery()
    
    $body = "Closing Connection to "+$OriginServer
    Write-Host ([datetime]::Now) $body
    $sqlConnection.Close()
}

function CompressAndSend
{
    $body = "Compressing "+$OriginServer+" Database."
    Write-Host ([datetime]::Now) $body
    
    $body = 'Starting the compression of the database'
    SendEmail 'Compressing the database' $body

	$ScriptBlock ={
		param ([string]$Folder,[string]$File,[string]$chunk)
		Remove-Item ('C:\'+$Folder+'\'+$File+'.7z*') -ErrorAction "SilentlyContinue"
		& 'C:\Program Files\7-Zip\7z.exe' a -mx3 ('-v'+$chunk) ('C:\'+$Folder+'\'+$File+'.7z') ('C:\'+$Folder+'\'+$File+'.bak') | Out-Null
	}
	If ($env:COMPUTERNAME -eq $OriginServer) {
		$compressJob = Start-Job -scriptblock $ScriptBlock -ArgumentList $ToFolder, $FileName, $Chunk
	} else {
		$compressJob = Invoke-Command -ComputerName $OriginServer -scriptblock $ScriptBlock -ArgumentList $ToFolder, $FileName, $Chunk -AsJob
	}
    #
    ##Cause script to sleep to allow for compression to start
    #
    Start-Sleep -s 10

    $fileNumber = 1
    $filePath = ''
    $complete = 0
	$Size = Convert-ChunkSize ($Chunk+'b')
    do
    {
		$filePath = GetFilePath('\\'+$OriginServer+'\C$\'+$ToFolder+'\') $FileName $fileNumber
		while (-Not (Test-Path $filePath) -AND ($compressJob.state -eq 'Running')) {
			Start-Sleep -s 3
		}
        do
		{
			Start-Sleep -s 3
			$file = Get-Item $filePath
			
		}
		while ($file.Length -lt $Size -AND ($compressJob.state -eq 'Running'))
			
        UploadFileChunk $filePath
        $fileNumber++
        
        #
        ## Check if the following chunk is the last one
        $filePath = GetFilePath('\\'+$OriginServer+'\C$\'+$ToFolder+'\') $FileName $fileNumber
		if (-Not (Test-Path $filePath) -AND ($compressJob.state -eq 'Completed')) {
			$complete = 1
			SendEmail 'Compression Completed' ('The compression of the backup is complete.')
			break
		}
    }
    until($complete -eq 1)
    
}

function GetFilePath ([string] $base, [string] $filename,[int] $number)
{
    $filePath = ''
    if($number -lt 10)
    {
        $filePath = $base + $filename + '.7z.00' + $number
    }
    else 
    {
        if($fileNumber -lt 100)
        {
                $filePath = $base + $filename + '.7z.0' + $number
        }
        else
        {
                $filePath = $base + $filename + '.7z.' + $number
        }
    }
    return $filepath
}

function Convert-ChunkSize ( [string] $chunksize )
{
    $chunksize = $chunksize.ToUpper()
    $result = 0
    switch ($chunksize.ToUpper().Substring($chunksize.Length-2))
    {
        "KB" { $result = [Int64]::Parse($chunksize.Replace("KB",""))*1024 }
        "MB" { $result = [Int64]::Parse($chunksize.Replace("MB",""))*1024*1024 }
        "GB" { $result = [Int64]::Parse($chunksize.Replace("GB",""))*1024*1024*1024 }
        "TB" { $result = [Int64]::Parse($chunksize.Replace("TB",""))*1024*1024*1024*1024 }   
        default { return $chunksize }
    }
    return [int64] $result
}

function ExtractAtHosts
{

	$ScriptBlock ={
		param ([string]$Folder,[string]$File)
		Remove-Item ('C:\'+$Folder+'\'+$File+'.bak') -ErrorAction "SilentlyContinue"
		& 'C:\Program Files\7-Zip\7z.exe' x ('-oC:\'+$Folder+'\') ('C:\'+$Folder+'\'+$File+'.7z.001') |Out-Null
	}
	
	SendEmail 'Extraction Starting' ('Extraction starting on hosts ')
	$extractJob = Invoke-Command -ComputerName $AllHosts -scriptblock $ScriptBlock -ArgumentList $ToFolder, $FileName -AsJob
	
	Wait-Job $extractJob |Out-Null
	SendEmail 'Extraction complete' ('Extraction completed on all hosts')
}
#######################################################################################
#End of Functions
#######################################################################################

#######################################################################################
#Script Logic
#######################################################################################
$AllHosts = $Hosts.replace(' ','').split(',')

#Kick off the Database Backup
$body = 'Starting the backup of the database'
Write-Host ([datetime]::Now) $body
SendEmail 'Backing up database' $body

BackupDatabase

#
#
CompressAndSend

$body = "Compression and Distribution has completed."
Write-Host ([datetime]::Now) $body
SendEmail 'Compression and Distribution Completed' $body

#
##Extract the compressed files at the hosts.
#
$body = "Starting Remote Extraction at hosts."
Write-Host ([datetime]::Now) $body
SendEmail 'Starting Remote Extraction' $body
ExtractAtHosts

Start-Sleep -s 5
$body = "Remote Extraction Completed at hosts."
Write-Host ([datetime]::Now) $body
SendEmail 'Remote Extraction Completed' $body

#End of Script
Start-Sleep -s 5
$body = "End of database distribution script. "
Write-Host ([datetime]::Now) $body
SendEmail 'Migration Completed' $body

stop-transcript

Read-Host "Migration Completed, transcript at C:\Users\Administrator\Documents, hit enter to exit"
#######################################################################################
#End of Script Logic
#######################################################################################
