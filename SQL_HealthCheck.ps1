<#
    Script: SQL_HealthCheck
    Author: Michael Toombs
    Email: mtoombs@pioneeres.com
    Date: 9/26/2019

    Version History: 
        0.0.1 - 9.26.2019
            Initial Script Creation
        0.1.0 - 10.11.2019
            1st version of Script Completed
            --Known Issues--
                Script fails on following PES Servers:
                    PESSINERGY-01
                    PESSINERGYDEV01
                    PESTREND-01
                    PESUPDATEMGR-01
                    PESCITLICS-01
        0.2.0 - 11.25.2019
            Added additional 'Checks' to script
                Collect Server Specs
                Collect Drive Info
                Collect DB Info
                Collect DB Maintenance Plans Info
                Collect DB Backups Info
                Collect SQL Log Failures
    
    Purpose: 
#>

[CmdletBinding()]
Param(
    # Target SQL Server. Can be single server, or a collection of SQL Server Instances.
    [Parameter(Mandatory)]
    [string[]]
    $targetSQLsvr,
    # SQL User Credentials. PSCredentials object, containing user credentials that will workd accross ALL SQL Servers in list.
    [pscredential]
    $sqlCreds = (Get-Credential),
    # Central Collection Server. Server name for Central collection database.
    [Parameter(Mandatory)]
    [string]
    $centralSvr,
    # Central Database Name. Name of Central collection database. Defaults to: DBA_Tools
    [string]
    $centralDB = 'DBA_Tools',
    # Central DB Blitz Table Name. Name of sp_Blitz results Table. Defaults to: BlitzResults
    [string]
    $blitzTableName = 'BlitzResults',
    # Central DB ServerInfo Table Name. Name of Table to store SQL Server Specifications to. Defaults to: ServerInfo
    [string]
    $svrInfoTableName = 'ServerInfo',
    # Central DB DriveInfo Table Name. Name of Table to store Server Drive Info to. Defaults to: ServerDriveInfo
    [string]
    $svrDriveInfoTableName = 'ServerDriveInfo',
    # Central DB DBInfo Table Name. Name of Table to store SQL Database Info to. Defaults to: DBInfo
    [string]
    $dbInfoTableName = 'DBInfo',
    # Central DB SQL Log Results Table Name. Name of Table to store SQL Log Results to. Defaults to: DBLogInfo
    [string]
    $dbLogInfoTableName = 'DBLogInfo',
    # Central DB SQL Maintenance Plans Results Table Name. Name of Table to store SQL Maintenance Plans Results to. Defaults to: MaintenancePlanInfo
    [string]
    $dbMaintPlanTableName = 'MaintenancePlanInfo',
    # Central DB Schema Name. Name of DB Schema to use. Defualts to: DBO
    [string]
    $centralSchemaName = 'dbo',
    # Path to SQL FirstRespondersToolkit, if different from default
    [string]
    $FRTK_Path = "$PSScriptRoot\FirstResponderKit.zip"
)

#region Load Necessary Assemblies
if (! ((Get-Module).name -contains 'dbatools')) {
    if ((Get-Module -ListAvailable).name -contains 'dbatools') {
        Import-Module -Name 'dbatools'
    }
    else {
        Install-Module -Name 'dbatools' -Force -Scope CurrentUser
    }
}
#endregion 

#region Script Variables
# Get Date
$CheckDate = (Get-Date -Format "yyyy-MM-dd HH:mm:ss")
$query = 'sp_blitz'
$backupsQuery = 'sp_BlitzBackups'
$dbRPORTO_ID = 0
$dbBackupStats_ID = 1
$dbBackupFindings_ID = 2
$dbRPORTO_TableName = 'DB_RPO_RTO_Data'
$dbBackupStats_TableName = 'DB_BackupStats'
$dbBackupFindings_TableName = 'DB_BackupFindings'

$DBInfoQuery = @"
set nocount on;
if object_id('tempdb..#t', 'U') is not null
 drop table #t;
create table #t (
  ServerName varchar(128) default @@servername
, DBName varchar(128) default db_name()
, DBOwner varchar(128)
, CreateDate datetime2
, RecoveryModel varchar(12)
, StateDesc varchar(60)
, CompatibilityLevel int
, DataFileSizeMB int
, LogFileSizeMB int
, DataUsageMB int
, IndexUsageMB int
, SizeMB decimal(17,2)
, Collation varchar(60)
, UserCount int
, RoleCount int
, TableCount int
, SPCount int
, UDFCount int
, ViewCount int
, DMLTriggerCount int
, IsCaseSensitive bit
, IsTrustWorthy bit
, LastFullBackupDate datetime2
, LastDiffBackupDate datetime2
, LastLogBackupDate datetime2);

   insert into #t (DBName, DBOwner, CreateDate, RecoveryModel, StateDesc, CompatibilityLevel, IsCaseSensitive
   , IsTrustWorthy, Collation, LastFullBackupDate, LastDiffBackupDate, LastLogBackupDate)
   select name, suser_sname(owner_sid), create_date, recovery_model_desc, state_desc,compatibility_level
   , IsCaseSensitive=CAST(CHARINDEX(N'_CS_', collation_name) AS bit), is_trustworthy_on, Collation_Name
   , t.LastFullBackup, t.LastDiffBackup, t.LastLogBackup
   from master.sys.databases db
   outer apply ( SELECT
    MAX(CASE WHEN b.type = 'D' THEN b.backup_finish_date END) AS LastFullBackup,
    MAX(CASE WHEN b.type = 'I' THEN b.backup_finish_date END) AS LastDiffBackup,
    MAX(CASE WHEN b.type = 'L' THEN b.backup_finish_date END) AS LastLogBackup
    FROM msdb.dbo.backupset b
    where b.database_name = db.name
    ) t;

EXEC master.dbo.sp_msforeachdb 'use [?]
update t set SizeMB=(select sum(size)/128. from dbo.sysfiles)
, DataUsageMB=x.DataUsageMB, IndexUsageMB=x.IndexUsageMB
   , DataFileSizeMB = u.DBSize, LogFileSizeMB = u.LogSize
   , TableCount=y.TC, UDFCount=y.UC, SPCount = y.SC, ViewCount=y.VC
   , DMLTriggerCount=y.DC
   , UserCount = z.UC, RoleCount = z.RC
from #t t
   outer apply (
   SELECT SUM(case when df.type in (0,2,4) then df.size else 0 end)/128
   , SUM(case when df.type in (1,3) then df.size else 0 end)/128
   FROM sys.database_files df
   ) u(DBSize, LogSize)
   outer apply(select  DataUsageMB=sum(
    CASE
    When it.internal_type IN (202,204,207,211,212,213,214,215,216,221,222,236) Then 0
    When a.type <> 1 and p.index_id < 2 Then a.used_pages
    When p.index_id < 2 Then a.data_pages
    Else 0
    END)/128,
IndexUsageMB=(sum(a.used_pages)-sum(
    CASE
    When it.internal_type IN (202,204,207,211,212,213,214,215,216,221,222,236) Then 0
    When a.type <> 1 and p.index_id < 2 Then a.used_pages
    When p.index_id < 2 Then a.data_pages
    Else 0
    END
    ))/128
    from sys.partitions p join sys.allocation_units a on p.partition_id = a.container_id
    left join sys.internal_tables it on p.object_id = it.object_id
   ) x
   outer apply
   ( select SC=Sum(case Type when ''P'' then 1 else 0 end)
    , DC=Sum(case Type when ''TR'' then 1 else 0 end)
    , TC=Sum(case Type when ''U'' then 1 end)
    , UC= sum(case when Type in (''TF'', ''IF'', ''FN'') then 1 else 0 end)
    , VC=Sum(case Type when ''V'' then 1 else 0 end)
    from sys.objects where object_id > 1024
    and type in (''U'',''P'',''TR'',''V'',''TF'',''IF'',''FN'')
   ) y
   outer apply
   ( select UC = sum(case when [Type] in (''G'',''S'',''U'') then 1 else 0 end)
      , RC = sum(case when Type = ''R'' then 1 else 0 end)
      from sys.database_principals
      where principal_id > 4
   ) z where t.DBName=db_name();
'
SELECT * FROM #T
"@
$maintPlanQuery = @"
SELECT CAST(CAST(sp.[packagedata] AS VARBINARY(MAX)) AS XML) AS [maintenance_plan_xml],
	   Frequency = REPLACE(x.FreqDescription, '##ActiveStartTime##', x.ActiveStartTime),
	   x.JobName,
	   x.ScheduleName,
	   x.IsEnabled 
FROM msdb.dbo.sysssispackages AS sp
CROSS APPLY (
	SELECT JobName = j.[name],
		   ScheduleName = ss.[name],
		   IsEnabled = ss.[enabled],
		   FreqDescription = f.FrequencyDescription + (CASE WHEN (freq_recurrence_factor = 1 
															  OR (freq_type = 4 AND freq_interval = 1 AND freq_subday_type = 1))
															THEN ': At ##ActiveStartTime##.' 
															ELSE '.'
													   END),
		   ActiveStartTime = STUFF(STUFF(RIGHT('000000' + CAST(ss.active_start_time AS varchar(6)), 6), 3, 0, ':'), 6, 0, ':')
	FROM msdb.dbo.sysjobs AS j
	JOIN msdb.dbo.sysjobschedules AS sj
	ON j.job_id = sj.job_id
	JOIN msdb.dbo.sysschedules AS ss
	ON sj.schedule_id = ss.schedule_id
	LEFT JOIN (
		SELECT d.FrequencyType, 
			   d.FrequencyInterval, 
			   d.FrequencySubdayType, 
			   d.FrequencySubdayInterval, 
			   d.FrequencyRelativeInterval, 
			   d.FrequencyDescription
		FROM (
			VALUES
			(64, 0, 0, 0, 0, 'Runs when computer is idle'),
			(4, 1, 4, 5, 0, 'Daily: Every 5 minutes'),
			(4, 1, 4, 10, 0, 'Daily: Every 10 minutes'),
			(4, 1, 4, 15, 0, 'Daily: Every 15 minutes'),
			(4, 1, 4, 30, 0, 'Daily: Every 30 minutes'),
			(4, 1, 4, 60, 0, 'Daily: Every 60 minutes'),
			(8, 1, 1, 0, 0, 'Weekly: Sunday'),
			(32, 1, 1, 0, 1, 'Monthy: Sunday: First'),
			(4, 1, 8, 6, 0, 'Daily: Every 6 hours'),
			(4, 1, 1, 0, 0, 'Daily'),
			(4, 1, 4, 1, 0, 'Daily: Every 1 minute'),
			(4, 1, 8, 3, 0, 'Daily: Every 3 hours'),
			(4, 1, 8, 12, 0, 'Daily: Every 12 hours'),
			(4, 1, 8, 1, 0, 'Daily: Every 1 hour')
		) AS d(FrequencyType, FrequencyInterval, FrequencySubdayType, FrequencySubdayInterval, FrequencyRelativeInterval, FrequencyDescription)) f
	ON ss.freq_type = f.FrequencyType
   AND ss.freq_interval = f.FrequencyInterval
   AND ss.freq_subday_type = f.FrequencySubdayType
   AND ss.freq_subday_interval = f.FrequencySubdayInterval
   AND ss.freq_relative_interval = f.FrequencyRelativeInterval
	WHERE j.[name] LIKE '' + sp.[name] + '%') AS x
"@
#endregion

#region Functions
#endregion

#region Main Script 
#! Basic Script Logic
#* Loop through SQL Servers
foreach ($svr in $targetSQLsvr) {
    Write-Verbose "Collecting and Processing Data for Server: $svr"
    $sqlServer = ($svr.Split('\'))[0]
    
    #* Iteration Variables
    $svrSpecs = @()
    $svrDriveInfo = @()
    $dbInfo = @()
    $maintPlanInfo = @()
    $sqlLogFailures = @()

    #* Install/Update FirstResponderToolkit Scripts
    Write-Verbose "$svr - Install/Update FirstResponderToolkit Scripts"
    $null = Install-DbaFirstResponderKit -SqlInstance $svr -SqlCredential $sqlCreds -LocalFile $FRTK_Path

    #* Collect sp_Blitz results
    Write-Verbose "$svr - Collecting sp_Blitz Results..."
    $results = Invoke-DbaQuery -SqlInstance $svr -SqlCredential $sqlCreds -Database 'master' -Query $query -QueryTimeout 600 -As 'DataTable'
    $results.Columns.Add((New-Object System.Data.DataColumn 'ServerName', ([string]), "'$svr'"))
    $results.Columns.Add((New-Object System.Data.DataColumn 'CheckDate', ([DateTime]), "'$CheckDate'"))
    Write-DbaDbTableData -SqlInstance $centralSvr -SqlCredential $sqlCreds -Database $centralDB -Table $blitzTableName -Schema $centralSchemaName -InputObject $results -AutoCreateTable

    #* Collect Server Specs
    Write-Verbose "$sqlServer - Collecting Server Specs..."
    $CPUInfo = Get-WmiObject Win32_Processor -ComputerName $sqlServer #Get CPU Information 
    $OSInfo = Get-WmiObject Win32_OperatingSystem -ComputerName $sqlServer #Get OS Information 
    # Get Memory Information. The data will be shown in a table as MB, rounded to the nearest second decimal. 
    $OSTotalVirtualMemory = [math]::round($OSInfo.TotalVirtualMemorySize / 1MB, 2) 
    $OSTotalVisibleMemory = [math]::round(($OSInfo.TotalVisibleMemorySize  / 1MB), 2) 
    $PhysicalMemory = Get-WmiObject CIM_PhysicalMemory -ComputerName $sqlServer | Measure-Object -Property capacity -sum | ForEach-Object {[math]::round(($_.sum / 1GB),2)} 

    $object = New-Object -TypeName PSObject
    Add-Member -InputObject $object -MemberType NoteProperty -Name "ServerName" -Value $sqlServer
    Add-Member -InputObject $object -MemberType NoteProperty -Name "CheckDate" -Value $CheckDate
    Add-Member -InputObject $object -MemberType NoteProperty -Name "CPU_Name" -Value $CPUInfo[0].Name
    Add-Member -InputObject $object -MemberType NoteProperty -Name "CPU_Description" -Value $CPUInfo[0].Description
    Add-Member -inputObject $object -memberType NoteProperty -name "Manufacturer" -value $CPUInfo[0].Manufacturer
    Add-Member -inputObject $object -memberType NoteProperty -name "PhysicalCores" -value $CPUInfo[0].NumberOfCores
    Add-Member -inputObject $object -memberType NoteProperty -name "CPU_L2CacheSize" -value $CPUInfo[0].L2CacheSize
    Add-Member -inputObject $object -memberType NoteProperty -name "CPU_L3CacheSize" -value $CPUInfo[0].L3CacheSize
    Add-Member -inputObject $object -memberType NoteProperty -name "Sockets" -value $CPUInfo[0].SocketDesignation
    Add-Member -inputObject $object -memberType NoteProperty -name "LogicalCores" -value $CPUInfo[0].NumberOfLogicalProcessors
    Add-Member -inputObject $object -memberType NoteProperty -name "OS_Name" -value $OSInfo.Caption
    Add-Member -inputObject $object -memberType NoteProperty -name "OS_Version" -value $OSInfo.Version
    Add-Member -inputObject $object -memberType NoteProperty -name "TotalPhysical_Memory_GB" -value $PhysicalMemory
    Add-Member -inputObject $object -memberType NoteProperty -name "TotalVirtual_Memory_MB" -value $OSTotalVirtualMemory
    Add-Member -inputObject $object -memberType NoteProperty -name "TotalVisable_Memory_MB" -value $OSTotalVisibleMemory
    $svrSpecs += $object

    #* Write Collected Server Spec Data to DB
    Write-DbaDbTableData -SqlInstance $centralSvr -SqlCredential $sqlCreds -Database $centralDB -Table $svrInfoTableName -Schema $centralSchemaName -InputObject $svrSpecs -AutoCreateTable

    #* Collect Drive Info
    Write-Verbose "$sqlServer - Collecting Server Disk Info..."
    $DiskInfo = Get-WmiObject -Class Win32_LogicalDisk -Filter "DriveType=3" -ComputerName $sqlServer | Select-Object DeviceID,VolumeName,Size,FreeSpace

    foreach ($disk in $DiskInfo) {
        $object = New-Object -TypeName PSObject
        Add-Member -InputObject $object -MemberType NoteProperty -Name "ServerName" -Value $sqlServer
        Add-Member -InputObject $object -MemberType NoteProperty -Name "CheckDate" -Value $CheckDate
        Add-Member -InputObject $object -MemberType NoteProperty -Name "Drive_ID" -Value $disk.DeviceID
        Add-Member -InputObject $object -MemberType NoteProperty -Name "Disk_Size" -Value ($disk.Size/1GB)
        Add-Member -inputObject $object -memberType NoteProperty -name "FreeSpace" -value ($disk.FreeSpace/1GB)
        
        $svrDriveInfo += $object
    }

    #* Write Collected Drive Data to DB
    Write-DbaDbTableData -SqlInstance $centralSvr -SqlCredential $sqlCreds -Database $centralDB -Table $svrDriveInfoTableName -Schema $centralSchemaName -InputObject $svrDriveInfo -AutoCreateTable
        
    #* Collect DB Info
    Write-Verbose "$svr - Collecting DB Info..."
    $dbInfo = Invoke-DbaQuery -SqlInstance $svr -SqlCredential $sqlCreds -Database 'master' -Query $DBInfoQuery -QueryTimeout 600 -As DataTable
    $dbInfo.Columns.Add((New-Object System.Data.DataColumn 'CheckDate', ([DateTime]), "'$CheckDate'"))

    #* Write Collected DB Info Data to DB
    Write-DbaDbTableData -SqlInstance $centralSvr -SqlCredential $sqlCreds -Database $centralDB -Table $dbInfoTableName -Schema $centralSchemaName -InputObject $dbInfo -AutoCreateTable

    #* Collect DB Maintenance Plans Info
    Write-Verbose "$svr - Collecting DB Maint Plan Info..."
    $maintPlanInfo = Invoke-DbaQuery -SqlInstance $svr -SqlCredential $sqlCreds -Database 'master' -Query $maintPlanQuery -QueryTimeout 600 -As 'DataTable'
    $maintPlanInfo.Columns.Add((New-Object System.Data.DataColumn 'ServerName', ([string]), "'$svr'"))
    $maintPlanInfo.Columns.Add((New-Object System.Data.DataColumn 'CheckDate', ([DateTime]), "'$CheckDate'"))

    #* Write Collected DB Maintenance Plans Info to DB
    Write-DbaDbTableData -SqlInstance $centralSvr -SqlCredential $sqlCreds -Database $centralDB -Table $dbMaintPlanTableName -Schema $centralSchemaName -InputObject $maintPlanInfo -AutoCreateTable

    #* Collect DB Backups Info
    Write-Verbose "$svr - Collecting DB Backups Info..."
    $results = Invoke-DbaQuery -SqlInstance $svr -SqlCredential $sqlCreds -Database 'master' -Query $backupsQuery -QueryTimeout 600 -As 'DataTable'

    foreach ($table in $results){
        #! $table.tablename
        $table.Columns.Add((New-Object System.Data.DataColumn 'ServerName', ([string]), "'$svr'"))
        $table.Columns.Add((New-Object System.Data.DataColumn 'CheckDate', ([DateTime]), "'$CheckDate'"))
    }
    #* Write collected Backups Data to DB
    Write-DbaDbTableData -SqlInstance $centralSvr -SqlCredential $sqlCreds -Database $centralDB -Table $dbRPORTO_TableName -Schema $centralSchemaName -InputObject $results[$dbRPORTO_ID] -AutoCreateTable
    Write-DbaDbTableData -SqlInstance $centralSvr -SqlCredential $sqlCreds -Database $centralDB -Table $dbBackupStats_TableName -Schema $centralSchemaName -InputObject $results[$dbBackupStats_ID] -AutoCreateTable
    Write-DbaDbTableData -SqlInstance $centralSvr -SqlCredential $sqlCreds -Database $centralDB -Table $dbBackupFindings_TableName -Schema $centralSchemaName -InputObject $results[$dbBackupFindings_ID] -AutoCreateTable

    #* Collect SQL Log Failures
    Write-Verbose "$svr - Collecting SQL Log Info..."
    $sqlLogFailures = Get-DbaErrorLog -SqlInstance $svr -SqlCredential $sqlCreds -Source 'Logon' -LogNumber 0
    #Todo: add code to add ServerName and CheckDate to this table...
    $results = @()
    foreach ($slf in $sqlLogFailures) {
        Add-Member -InputObject $slf -MemberType NoteProperty -Name "ServerName" -Value $svr
        Add-Member -InputObject $slf -MemberType NoteProperty -Name "CheckDate" -Value $CheckDate
        $results += $slf
    }

    #$sqlLogFailures.Columns.Add((New-Object System.Data.DataColumn 'ServerName', ([string]), "'$svr'"))
    #$sqlLogFailures.Columns.Add((New-Object System.Data.DataColumn 'CheckDate', ([DateTime]), "'$CheckDate'"))

    #* Write collected SQL Log Failures to DB
    Write-DbaDbTableData -SqlInstance $centralSvr -SqlCredential $sqlCreds -Database $centralDB -Table $dbLogInfoTableName -Schema $centralSchemaName -InputObject $slf[0..19] -AutoCreateTable
}
#endregion

#! Testing Area

#region Removed Code

<# $centralSvr = 'pessqlaus-03\sql2014'
$centralDB = 'DBA_Tools'
$centralSchemaName = 'dbo'


$CheckDate = (Get-Date -Format "yyyy-MM-dd HH:mm:ss")
$svr = 'pessqlsa-02'
$sqlCreds = (Get-Credential)
$backupsQuery = 'sp_BlitzBackups'
$dbRPORTO_ID = 0
$dbBackupStats_ID = 1
$dbBackupFindings_ID = 2

$dbRPORTO_TableName = 'DB_RPO_RTO_Data'
$dbBackupStats_TableName = 'DB_BackupStats'
$dbBackupFindings_TableName = 'DB_BackupFindings'


$results = Invoke-DbaQuery -SqlInstance $svr -SqlCredential $sqlCreds -Database 'master' -Query $backupsQuery -QueryTimeout 600 -As 'DataTable'

foreach ($table in $results){
    $table.tablename
    $table.Columns.Add((New-Object System.Data.DataColumn 'ServerName', ([string]), "'$svr'"))
    $table.Columns.Add((New-Object System.Data.DataColumn 'CheckDate', ([DateTime]), "'$CheckDate'"))
}

Write-DbaDbTableData -SqlInstance $centralSvr -SqlCredential $sqlCreds -Database $centralDB -Table $dbRPORTO_TableName -Schema $centralSchemaName -InputObject $results[$dbRPORTO_ID] -AutoCreateTable
Write-DbaDbTableData -SqlInstance $centralSvr -SqlCredential $sqlCreds -Database $centralDB -Table $dbBackupStats_TableName -Schema $centralSchemaName -InputObject $results[$dbBackupStats_ID] -AutoCreateTable
Write-DbaDbTableData -SqlInstance $centralSvr -SqlCredential $sqlCreds -Database $centralDB -Table $dbBackupFindings_TableName -Schema $centralSchemaName -InputObject $results[$dbBackupFindings_ID] -AutoCreateTable
 #>

#endregion