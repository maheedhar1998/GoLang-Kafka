# Kafka File Transfer GO
 
## Pre-reqs

- Docker
- Docker compose
- GO

## Initiating Kafka

- Linux (debian based)
-- `sudo docker-compose -f docker-compose.yml up`
- Linux (other)
-- `<Super user command> docker-compose -f docker-compose.yml up`

## Runnning Microservices

- `go run <micro service file>`

## REST Endpoints to search data

- Searching by Name -- `localhost:8180/search/ByName/<name>`
- Searching by Birth Year -- `localhost:8180/search/ByBirth/<year>`
- Searching by Death Year -- `localhost:8180/search/ByDeath/<year>`
- Searching by Profession -- `localhost:8180/search/ByProfession/<profession>`
