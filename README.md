# Wiki-nematics 


## Install

### Docker
```
docker compose up -d
```
enter container
```
docker exec --workdir /opt/kafka/bin/ -it broker sh
```
more info : [Docker Kakfa Quick Start](https://hub.docker.com/r/apache/kafka#quick-start)
Connect to Job Dashboard : [localhost:8081](http://localhost:8081) 

### Go

```Go
go build -C client -o ../output/
```
### Maven

```
mvn clean package
```