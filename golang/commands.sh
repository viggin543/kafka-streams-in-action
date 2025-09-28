# one-time
go install github.com/actgardner/gogen-avro/v10/cmd/gogen-avro@latest

gogen-avro ./avro ./avenger.avsc



# create a proto schema
jq -Rs . proto/avenegers.proto >  proto/proto_string.json

curl -s -X POST "http://localhost:8081/subjects/grpc-avengers-value/versions" \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d "{\"schemaType\":\"PROTOBUF\",\"schema\":$(cat proto/proto_string.json)}"



go install google.golang.org/protobuf/cmd/protoc-gen-go@latest

protoc --go_out=. avenger.proto

# Terminal 1 – consumer
go run ./cmd/consumer

# Terminal 2 – producer
go run ./cmd/producer