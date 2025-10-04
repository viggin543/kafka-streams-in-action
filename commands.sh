#
docker-compose exec broker bash

# single partition
kafka-topics --create --topic first-topic --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1
kafka-topics --delete --topic first-topic --bootstrap-server localhost:9092
kafka-console-producer --topic first-topic --bootstrap-server localhost:9092 --property parse.key=true --property key.separator=":"
kafka-console-consumer --topic first-topic --bootstrap-server localhost:9092 --from-beginning --property print.key=true --property key.separator="-"


# multi partition
kafka-topics --create --topic second-topic --bootstrap-server localhost:9092 --replication-factor 1 --partitions 2
kafka-console-producer --topic second-topic --bootstrap-server localhost:9092 --property parse.key=true --property key.separator=":"
kafka-console-consumer --topic second-topic --bootstrap-server broker:9092  --property print.key=true  --property key.separator="-"  --partition 0
kafka-console-consumer --topic second-topic --bootstrap-server broker:9092  --property print.key=true  --property key.separator="-"  --partition 1
# without specifying a partition all records are consumed
kafka-console-consumer --topic second-topic --bootstrap-server broker:9092  --property print.key=true  --property key.separator="-"  --from-beginning

kafka-topics --list --bootstrap-server localhost:9092


# create avro schema
jq '. | {schema: tojson}' avenger.avcs | curl -s -X \
   POST http://localhost:8081/subjects/avro-avengers-value/versions\
   -H "Content-Type: application/vnd.schemaregistry.v1+json" \
   -d @-  \
   | jq
  # subjects/avro-avengers-value/, which specifies the subject name for the schema.
  # In this case, it’s avro-avengers-value, which means that values (in the key-value pairs)
  # going into the avro-avengers topic need to be in the format of the registered schema
  curl -s "http://localhost:8081/subjects" | jq
#[
 #  "avro-avengers-value"
 #]
# the schema registered for the `avro-avengers` topic.

curl -s "http://localhost:8081/subjects/avro-avengers-value/versions" | jq
curl -s "http://localhost:8081/subjects/avro-avengers-value/versions/1" | jq '.'
curl -s "http://localhost:8081/subjects/avro-avengers-value/versions/latest" | jq '.'

