@echo off
set "BAT_DIR=%~dp0"
cd /d "%BAT_DIR%"

:: Go client
echo Building Go client...
go build -C client -o ../output/

::docker
echo Starting Docker containers...
docker compose up -d --force-recreate

::java
echo Packaging Java project...
cd .\processor
CALL mvn clean package -B

::Run client
echo Starting client
cd ..
start output\client.exe

:: Run the Flink job
echo Copying JAR file to Flink JobManager container...
cd .\processor\target
docker exec flink-jobmanager mkdir -p /opt/flink/usrlib/
docker cp .\processor-1.0-SNAPSHOT.jar flink-jobmanager:/opt/flink/usrlib/

echo Running job
docker exec flink-jobmanager /opt/flink/bin/flink run -d /opt/flink/usrlib/processor-1.0-SNAPSHOT.jar
