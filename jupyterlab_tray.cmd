@echo off
setlocal EnableDelayedExpansion

REM ==========================================
REM CONFIGURATION
REM ==========================================

REM 1: display help text (PowerShell only), 0: skip
set "displayHelp=1"

REM 1: Pause at end, 0: Close immediately
set "pauseAtEnd=1"

REM 1: Enforce Admin rights, 0: Ignore
set "RequiresAdmin=0"

REM Script Name (Leave empty to use this batch file's name)
set "scriptName="

REM Subdirectory (Leave empty for current dir)
set "scriptSubdir="

REM 1: Run from Script Dir, 0: Run from wherever this is called
set "runFromScriptDir=0"

REM Script Type: PS1 or PY
set "scriptType=PY"

REM DEFAULT ARGUMENTS
REM These are always passed to the script BEFORE your manual arguments.
REM Example: set "defaultArgs=--config default.json --verbose"
set "defaultArgs="

REM ==========================================
REM LOGIC
REM ==========================================

REM 1. Admin Check
if "%RequiresAdmin%"=="1" (
    net session >nul 2>&1
    if !errorlevel! neq 0 (
        echo [ERROR] This script requires administrative privileges.
        echo Please right-click and "Run as Administrator".
        goto :End
    )
)

REM 2. Resolve Paths
set "baseDir=%~dp0"

if not "%scriptSubdir%"=="" (
    set "targetDir=%baseDir%%scriptSubdir%\"
) else (
    set "targetDir=%baseDir%"
)

if "%scriptName%"=="" (
    set "scriptName=%~n0"
    if /I "%scriptType%"=="PS1" set "scriptName=!scriptName!.ps1"
    if /I "%scriptType%"=="PY"  set "scriptName=!scriptName!.py"
    if /I "%scriptType%"=="BAT" set "scriptName=!scriptName!.bat"
)

set "scriptFullName=%targetDir%%scriptName%"

REM 3. File Existence Check
if not exist "%scriptFullName%" (
    echo [ERROR] Target script not found:
    echo "%scriptFullName%"
    goto :End
)

REM 4. Working Directory
if "%runFromScriptDir%"=="1" (
    pushd "%targetDir%"
)

REM 5. Display Help (PS1 Only)
if "%displayHelp%"=="1" (
    if /I "%scriptType%"=="PS1" (
        echo Loading Help...
        powershell -NoProfile -ExecutionPolicy Bypass -Command "$h = Get-Help '%scriptFullName%'; Write-Host 'SYNOPSIS:'; Write-Host $h.synopsis; Write-Host 'DESCRIPTION:'; Write-Host $h.description"
        echo ----------------------------------------------------
    )
)

REM 6. Execution
echo Running: %scriptName%
echo Defaults: %defaultArgs%
echo UserArgs: %*
echo ----------------------------------------------------

if /I "%scriptType%"=="PS1" (
    powershell -NoProfile -ExecutionPolicy Bypass -File "%scriptFullName%" %defaultArgs% %*
) else if /I "%scriptType%"=="PY" (
    where py >nul 2>&1
    if !errorlevel! equ 0 (
        py "%scriptFullName%" %defaultArgs% %*
    ) else (
        python "%scriptFullName%" %defaultArgs% %*
    )
) else (
    echo [ERROR] Script type %scriptType% not implemented.
)

:End
if "%pauseAtEnd%"=="1" (
    pause
)
if "%runFromScriptDir%"=="1" popd