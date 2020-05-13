# Mongo Driver Test
Project's objetive is reproduce the faulty behaviour of some microservices at my job.

This behaviour comprises three components:
* Microservice' implementation: Covered by the test implementation, doing some random querys
* MongoDB' driver: Implementing connection's pool logic and all the necessary stuff to query datasource
* Db engine: Providing data's storage, access's security and db operations (external component) 

## Running test's scenarios
This project run a complete scenario, incluiding data generation if need.  
All the process, including connection pool, test data, query executions, and so on,
lives at stage execution and no one survives between executions.


Executing scenarios involve an HTTP Post:

URI: http://localhost:8090/api/v1/stages/

Payload:

```json
{
	"db_config":{
		"db_name": "stores",
		"collection_name": "stores",
		"conn_string":"mongodb://localhost:27017/stores",
		"min_pool_size": 30,
		"max_pool_size": 100,
		"idle_timeout": 60,
		"socket_timeout": 2
	},
	"stage_config":{
		"workers_count": 10,
		"workers_to_add": 45,
		"increment_load":2,
		"producers_count": 40,
		"msg_by_sec": 30,
		"time_to_sleep_secs": 30,
		"time_to_finish_secs": 20,
		"context_time_out_ms": 500,
		"query_timeout_ms": 500
	}
}
```
where important values are:
* context_time_out_ms: A posible cause of strange behaviour of driver connection pool
* query_timeout_ms: A best effort timeout for query
> first section of payload (db_config) configures the driver, 
> and second one (stage_config) configures the scenario

To run this locally just use a docker image of mongoDb as:
```shell script
docker run -d --name testDb -p 27017:27017 mongo:3.6.17-xenial
```
