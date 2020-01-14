param (
    # SQL Server to Querry, can contain an Array of SQL Instance names
    [string[]]
    $sqlserver,
    [pscredential]
    $sqlcreds = (get-Credential)
)

$query = Get-Content -Path "$Root\scripts\DetermineVersionOfSQLServer.sql" | Out-String
$serverInfo = @()

foreach ($svr in $sqlserver) {
    $results = ''
    $results = Invoke-DbaQuery -SqlInstance $svr -SqlCredential $sqlCreds -Query $query -MessagesToOutput
    $object = New-Object -TypeName PSObject
    Add-Member -InputObject $object -MemberType NoteProperty -Name "ServerName" -Value $svr
    Add-Member -InputObject $object -MemberType NoteProperty -Name "VersionInfo" -Value $results
    $serverInfo += $object
}

$serverInfo

foreach ($svr in $serverInfo) {
    Add-Content -Path 'C:\temp\sqlVersions.log' -Value '======================================================================'
    Add-Content -Path 'C:\temp\sqlVersions.log' -Value $svr.ServerName
    Add-Content -Path 'C:\temp\sqlVersions.log' -Value '======================================================================'
    Add-Content -Path 'C:\temp\sqlVersions.log' -Value $svr.VersionInfo
    Add-Content -Path 'C:\temp\sqlVersions.log' -Value '======================================================================'
    Add-Content -Path 'C:\temp\sqlVersions.log' -Value ' '

}

