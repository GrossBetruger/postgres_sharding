#/bin/bashs

docker build -t pgshard .

docker run -e POSTGRES_PASSWORD=password --name pgshard1 -p 5432:5432  pgshard
docker run -e POSTGRES_PASSWORD=password --name pgshard2 -p 5433:5432  pgshard
docker run -e POSTGRES_PASSWORD=password --name pgshard3 -p 5434:5432  pgshard


