@echo off
setlocal EnableExtensions DisableDelayedExpansion
title Nightshift - Local Paper Desk
pushd "%~dp0" || exit /b 1
if not exist "compose.yaml" goto :missing_project
where.exe docker.exe >nul 2>&1
if errorlevel 1 goto :missing_docker
docker compose version >nul 2>&1
if errorlevel 1 goto :missing_docker
set "ENGINE_OS="
for /f "delims=" %%S in ('docker info --format "{{.OSType}}" 2^>nul') do set "ENGINE_OS=%%S"
if not defined ENGINE_OS goto :engine_off
if /i not "%ENGINE_OS%"=="linux" goto :wrong_engine

echo Starting Nightshift in PAPER mode. No wallet is needed.
echo The first build downloads the Linux image and Python packages.
docker compose up -d --build --wait --wait-timeout 180
if errorlevel 1 goto :failed
set "DESK_BIND="
for /f "delims=" %%B in ('docker compose port nightshift 8080') do set "DESK_BIND=%%B"
if not defined DESK_BIND goto :failed
if /i not "%DESK_BIND:~0,10%"=="127.0.0.1:" goto :unexpected_bind
echo Dashboard: http://%DESK_BIND%/
echo Keep Docker Desktop running and Windows awake for overnight scanning.
start "" "http://%DESK_BIND%/"
popd
exit /b 0

:missing_project
echo Please extract the whole ZIP first. Run this launcher beside compose.yaml.
goto :explain
:missing_docker
echo Install Docker Desktop for Windows with Docker Compose, then start it.
echo Official instructions: https://docs.docker.com/desktop/setup/install/windows-install/
goto :explain
:engine_off
echo Open Docker Desktop and wait until its engine is running. Then try again.
goto :explain
:wrong_engine
echo This desk needs Linux containers. Switch Docker Desktop to Linux containers.
goto :explain
:failed
echo The desk did not start cleanly. Check the messages above.
docker compose ps -a
docker compose logs --tail 20 nightshift
echo If port 8080 is already in use, see WINDOWS-START-HERE.txt.
goto :explain
:unexpected_bind
echo The published address is not the expected local loopback address.
echo Check the ports section of compose.yaml before opening the dashboard.
:explain
echo.
pause
popd
exit /b 1
