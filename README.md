# Setup instructions

Start kafka
```
cd kafka-server
./start.sh
```

In another terminal
```
./topic.sh # creates topic called triff-ondc
./check-topic.sh # check if the topic is correctly created
```

Build producer and consumer
```
cd ../go-prod-cons/consumer/
go build -o out/consumer consumer.go
./out/consumer application.properties

# create producer

cd ../producer/
go build -o out/producer producer.go
./out/producer application.properties
```
