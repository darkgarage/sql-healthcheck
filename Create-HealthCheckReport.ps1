<#
    Script: Create-HealthCheckReport
    Author: Michael Toombs
    Email: mtoombs@pioneeres.com
    Date: 10.11.2019

    Version History: 
        0.0.1 -- 10.11.2019
            Initial script created

    Purpose: 
        To generate report of SQL Server Health data.
#>

[CmdletBinding()]
Param(
    # SQL User Credentials. PSCredentials object, containing user credentials that will work accross ALL SQL Servers in list.
    [pscredential]
    $sqlCreds = (Get-Credential),
    # Central Collection Server. Server name for Central collection database.
    [Parameter(Mandatory)]
    [string]
    $centralSvr,
    # Central Database Name. Name of Central collection database. Defaults to: DBA_Tools
    [string]
    $centralDB = 'DBA_Tools',
    # Central DB Table Name. Name of sp_Blitz results Table. Defaults to: BlitzResults
    [string]
    $centralTableName = 'BlitzResults',
    # OPTIONAL: Date of Health Check data to include in report. If not provided, will default to most recent dataset
    [datetime]
    $reportDate = (Get-Date)

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
if (! ((Get-Module).name -contains 'EnhancedHTML2')) {
    if ((Get-Module -ListAvailable).name -contains 'EnhancedHTML2') {
        Import-Module -Name 'EnhancedHTML2'
    }
    else {
        Install-Module -Name 'EnhancedHTML2' -Force -Scope CurrentUser
    }
}
#endregion 

#region Script Variables
$priorityCounts = @()
$blitzResults = @()
$filepath = ("$PSScriptRoot\{0}_{1}_SQLReport.html" -f (get-date).year, ((Get-Culture).DateTimeFormat.GetAbbreviatedMonthName((Get-Date).Month)))
$svrSpecs = @()
$svrDriveInfo = @()
$dbInfo = @()
$maintPlanInfo = @()
$dbBackupInfo = @()
$sqlLogFailures = @()
#endregion

#region Functions

#endregion

#region Main Script 
Write-Verbose "Server: $centralSvr"
Write-Verbose "DB: $centralDB"
Write-Verbose "Table: $centralTableName"



# Check date, and find CheckDate that is most recent, before given date.
$dateQuery = "select MAX([CheckDate]) from [dbo].[$centralTableName]"
$latestDate = Invoke-DbaQuery -SqlInstance $centralSvr -SqlCredential $sqlCreds -Database $centralDB -Query $dateQuery -QueryTimeout 600 -As SingleValue
if ($latestDate -lt $reportDate) {
    $healthCheckQuery = "select * from [dbo].[$centralTableName] where [checkdate] = '$latestDate' "
}
else {
    $dateQuery = "select MAX([CheckDate]) from [dbo].[$centralTableName] where [checkdate] < '$reportDate'"
    $latestDate = Invoke-DbaQuery -SqlInstance $centralSvr -SqlCredential $sqlCreds -Database $centralDB -Query $dateQuery -QueryTimeout 600
    $healthCheckQuery = "select * from [dbo].[$centralTableName] where [checkdate] = '$latestDate' "
}


# Collect HealthCheck Data from DB
$healthCheckData = Invoke-DbaQuery -SqlInstance $centralSvr -SqlCredential $sqlCreds -Database $centralDB -Query $healthCheckQuery -QueryTimeout 600 -As DataTable
#* Parse and Process SQL Health Data
# Get list of SQL Server Instances
$sqlInstances = ($healthCheckData.Rows | Sort-Object -Property 'ServerName' -Unique).ServerName
# Get Count of findings, by priority, for each SQL Instance.
foreach ($sqlInst in $sqlInstances) {
    #* Collect sp_Blitz Data from SQL DB
    #region sp_blitz Data
    
    #* Critical = P1; High = P5..P10; Medium = P20..50; Low/Info = 51..250
    $criticalFindingsCount = (($healthCheckData.Rows | Where-Object { $_.ServerName -eq $sqlInst}) | Where-Object { $_.Priority -eq 1 }).count
    $highFindingsCount = (($healthCheckData.Rows | Where-Object { $_.ServerName -eq $sqlInst}) | Where-Object { $_.Priority -in 5..10 }).count
    $mediumFindingsCount = (($healthCheckData.Rows | Where-Object { $_.ServerName -eq $sqlInst}) | Where-Object { $_.Priority -in 20..50 }).count
    $lowFindingsCount = (($healthCheckData.Rows | Where-Object { $_.ServerName -eq $sqlInst}) | Where-Object { $_.Priority -in 51..250 }).count
    $object = New-Object -TypeName PSObject
    Add-Member -InputObject $object -MemberType NoteProperty -Name ServerName -Value $sqlInst
    Add-Member -InputObject $object -MemberType NoteProperty -Name Critical -Value $criticalFindingsCount
    Add-Member -InputObject $object -MemberType NoteProperty -Name High -Value $highFindingsCount
    Add-Member -InputObject $object -MemberType NoteProperty -Name Medium -Value $mediumFindingsCount
    Add-Member -InputObject $object -MemberType NoteProperty -Name Low -Value $lowFindingsCount
    $priorityCounts += $object

    #* Split BlitsResults into seperate DataTables, by SQL Instance.  
    $instanceResults = (($healthCheckData.Rows | Where-Object { $_.ServerName -eq $sqlInst}) | Where-Object { $_.Priority -in 1..250 })
    $object = New-Object -TypeName PSObject
    Add-Member -InputObject $object -MemberType NoteProperty -Name ServerInstance -Value $sqlInst
    Add-Member -InputObject $object -MemberType NoteProperty -Name HCResults -Value $instanceResults
    $blitzResults += $object
    #endregion sp_Blitz Data

    #* Collect SQL Server Info from SQL DB
    #region SQL Server Info

    #endregion SQL Server Info

    #* Collect Drive Info from SQL DB
    #region Drive Info

    #endregion Drive Info

    #* DB Info from SQL DB
    #region DB Info

    #endregion DB Info

    #* DB Maintenance Plans Info from SQL DB
    #region DB Maintenance Plans

    #endregion DB Maintenance Plans

    #* DB Backups Info from SQL DB
    #region DB Backups

    #endregion DB Backups

    #* SQL Log Failures from SQL DB
    #region SQL Log Failures

    #endregion SQL Log Failures

}

#* Generate Report File

$style = Get-Content -Path 'style.css'

# Health Check Sumary Table 
$params = @{'As'='Table';
    'PreContent'='<h2>&diams; Summary</h2>';
    'EvenRowCssClass'='even';
    'OddRowCssClass'='odd';
    'TableCssClass'='grid';
    'Properties'=@{n='Instance';e={$_.ServerName}},
    @{n='Critical';e={$_.Critical}},
    @{n='High';e={$_.High}},
    @{n='Medium';e={$_.Medium}},
    @{n='Low';e={$_.Low}}
}
$html_PriorityCounts = $priorityCounts | ConvertTo-EnhancedHTMLFragment @params -ErrorAction SilentlyContinue

# Health Check Details Tables, by Server
$html_ReportDetails = ''

foreach ($bResult in $blitzResults) {
    $instance = $bResult.ServerInstance
    $params = @{'As'='Table';
        'PreContent'="<h2>&diams; SQL Health Check Details: $instance</h2>";
        'EvenRowCssClass'='even';
        'OddRowCssClass'='odd';
        'MakeTableDynamic'=$true;
        'TableCssClass'='grid';
        'Properties'=
        @{n='Priority';e={$_.Priority};css={if ($_.Priority -in 1..10) { 'red' }}},
        @{n='Findings Group';e={$_.FindingsGroup};css={if ($_.Priority -in 1..10) { 'red' }}},
        @{n='Database';e={$_.DatabaseName};css={if ($_.Priority -in 1..10) { 'red' }}},
        @{n='Finding';e={$_.Finding};css={if ($_.Priority -in 1..10) { 'red' }}},
        @{n='Details';e={$_.Details};css={if ($_.Priority -in 1..10) { 'red' }}}
    }
    $html_ReportDetails += $bResult.HCResults | ConvertTo-EnhancedHTMLFragment @params
}

# Generate Final report file
$params = @{'CssStyleSheet'=$style;
'Title'="SQL Server Health Report";
'PreContent'="<h1>PES SQL Server Health Report</h1>";
'HTMLFragments'=@($html_PriorityCounts,$html_ReportDetails)}
ConvertTo-EnhancedHTML @params | Out-File -FilePath $filepath

#endregion

#! Testing Area
#region Remove Before Execution

#endregion