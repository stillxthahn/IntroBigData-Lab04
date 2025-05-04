@echo off

if "%~1"=="" (
    echo Please provide the name of the Spark script to submit.
    echo Usage: submit_spark_job.bat your_script.py
    exit /b 1
)

set SCRIPT_NAME=%1

echo ---Submitting spark job: %SCRIPT_NAME%
docker exec spark-worker /opt/bitnami/spark/bin/spark-submit ^
  --master spark://master:7077 ^
  --deploy-mode client ^
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 ^
  /opt/spark-scripts-transform/%SCRIPT_NAME%
