# stream_protobuf_example
Example of project reading protobuf stream from kafka

## Run project

checkout:

```
git clone https://github.com/bitquery/stream_protobuf_example.git
```

compile:

```
go build
```

copy config:

```
cp config_example.yml config.yml
```

then fill in variables ```<YOUR USERNAME>``` and ```<YOUR PASSWORD>``` in config.yml file

run it:

```
./stream_protobuf_example 
%4|1738918885.417|CONFWARN|rdkafka#producer-1| [thrd:app]: Configuration property group.id is a consumer property and will be ignored by this producer instance
%4|1738918885.417|CONFWARN|rdkafka#producer-1| [thrd:app]: Configuration property enable.auto.commit is a consumer property and will be ignored by this producer instance
Subscribing to topic solana.dextrades.proto 4 partitions: [{0 Success 3 [3] [3]} {1 Success 2 [2] [2]} {2 Success 2 [2] [2]} {3 Success 1 [1] [1]}]...
Assigned 4 consumers to solana.dextrades.proto topic
press Ctrl-C to exit
Running consumer rdkafka#consumer-2
Running consumer rdkafka#consumer-5
Running consumer rdkafka#consumer-3
Running consumer rdkafka#consumer-4
Received message with key 319040909 from partition 1[825631]
Received message with key 319040910 from partition 0[827071]
Received message with key 319040911 from partition 2[825942]

```