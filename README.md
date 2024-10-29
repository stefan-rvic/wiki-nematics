# Wiki-nematics 


## Install

### Kafka
```
docker compose up -d
```
enter container
```
docker exec --workdir /opt/kafka/bin/ -it broker sh
```
more info : [Docker Kakfa Quick Start](https://hub.docker.com/r/apache/kafka#quick-start)

### Go

```Go
go build -C client -o ../output/
go build -C consumer -o ../output/
```