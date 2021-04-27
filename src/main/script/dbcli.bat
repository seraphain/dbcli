@echo off
@setlocal

set LAUNCHER=com.github.seraphain.dbcli.DatabaseCommandLine
set JVM_OPTS=

if not "%JAVA_HOME%"=="" goto OkJHome
for %%i in (java.exe) do set "JAVA_CMD=%%~$PATH:i"
goto checkJCmd

:OkJHome
set "JAVA_CMD=%JAVA_HOME%\bin\java.exe"

:checkJCmd
if exist "%JAVA_CMD%" goto chkBase

echo The JAVA_HOME environment variable is not defined correctly >&2
echo This environment variable is needed to run this program >&2
goto error

:chkBase
set "BASE_DIRECTORY=%~dp0"
if not "%BASE_DIRECTORY%"=="" goto stripBase
goto error

:stripBase
if not "_%BASE_DIRECTORY:~-1%"=="_\" goto setClasspath
set "BASE_DIRECTORY=%BASE_DIRECTORY:~0,-1%"
goto stripBase

:setClasspath
@setlocal EnableExtensions EnableDelayedExpansion
set CLASSPATH="%BASE_DIRECTORY%"
for %%i in ("%BASE_DIRECTORY%"\lib\*.jar) do set CLASSPATH=!CLASSPATH!;"%%i"
@endlocal & set CLASSPATH=%CLASSPATH%

pushd %BASE_DIRECTORY%
"%JAVA_CMD%" %JVM_OPTS% -cp %CLASSPATH% %LAUNCHER% %*
if ERRORLEVEL 1 goto error
goto end

:error
set ERROR_CODE=1

:end
popd
@endlocal & set ERROR_CODE=%ERROR_CODE%

cmd /C exit /B %ERROR_CODE%
