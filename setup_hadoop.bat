@echo off
setlocal enabledelayedexpansion

:: Set Hadoop environment variables
SET HADOOP_HOME=%~dp0hadoop
SET PATH=%HADOOP_HOME%\bin;%PATH%

:: Create directories if they don't exist
if not exist "%HADOOP_HOME%\bin" (
    echo Creating Hadoop directories...
    mkdir "%HADOOP_HOME%\bin"
)

:: Download winutils from Apache archive
if not exist "%HADOOP_HOME%\bin\winutils.exe" (
    echo Downloading winutils.exe...
    powershell -Command "& {
        $url = 'https://archive.apache.org/dist/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz'
        $output = '%HADOOP_HOME%\hadoop.tar.gz'
        [Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12
        try {
            Invoke-WebRequest -Uri $url -OutFile $output
            Write-Host 'Download completed successfully'
        } catch {
            Write-Host 'Error downloading file: ' $_.Exception.Message
            exit 1
        }
    }"
    
    :: Extract winutils.exe from the archive
    echo Extracting winutils.exe...
    tar -xf "%HADOOP_HOME%\hadoop.tar.gz" --strip-components=2 hadoop-3.3.6/bin/winutils.exe -C "%HADOOP_HOME%\bin"
    
    :: Clean up
    del "%HADOOP_HOME%\hadoop.tar.gz"
)

:: Verify installation
if exist "%HADOOP_HOME%\bin\winutils.exe" (
    echo ✅ Hadoop environment configured successfully!
    echo HADOOP_HOME=%HADOOP_HOME%
) else (
    echo ❌ Failed to setup Hadoop environment
    exit /b 1
)

endlocal