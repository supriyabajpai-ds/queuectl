#!/usr/bin/env bash
# Build without Maven: compiles all sources into out/ against the SQLite JDBC jar.
# (pom.xml is also provided if you prefer `mvn package`.)
set -e
cd "$(dirname "$0")"

JAR_VERSION=3.36.0.3
JAR="lib/sqlite-jdbc-${JAR_VERSION}.jar"

if [ ! -f "$JAR" ]; then
  mkdir -p lib
  echo "Downloading sqlite-jdbc ${JAR_VERSION}..."
  curl -sL -o "$JAR" "https://github.com/xerial/sqlite-jdbc/releases/download/${JAR_VERSION}/sqlite-jdbc-${JAR_VERSION}.jar"
fi

rm -rf out
mkdir -p out
find src/main/java -name '*.java' > /tmp/queuectl-sources.txt
javac -cp "$JAR" -d out @/tmp/queuectl-sources.txt
echo "Build OK. Run with: ./queuectl help"
