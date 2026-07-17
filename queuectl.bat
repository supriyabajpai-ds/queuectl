@echo off
REM Launcher for Windows (cmd / PowerShell). Usage: queuectl <command> ...
set DIR=%~dp0
java -cp "%DIR%out;%DIR%lib\sqlite-jdbc-3.36.0.3.jar" com.flam.queuectl.cli.Main %*
