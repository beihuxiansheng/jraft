@echo off
set curdir=%~dp0
if "%*" == "" (
    echo server id is required
    goto exit
)

set curdir=%~dp0
if not exist "%curdir%\server%1" (
    echo server %1 is not found
    goto exit
)

start "server%1" /D "%curdir%\server%1" java -jar %curdir%\dmprinter.jar server "%curdir%\server%1"
@echo on