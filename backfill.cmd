@echo off
rem Runs the packaged CLI. Build it first: mvnw -DskipTests package
java -jar "%~dp0target\backfill.jar" %*
