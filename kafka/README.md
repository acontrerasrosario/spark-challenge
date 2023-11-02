
In this setup, our Zookeeper server is listening on port=2181 for the kafka service, which is defined within the same 
container setup. However, for any client running on the host, it'll be exposed on port 22181.

Similarly, the kafka service is exposed to the host applications through port 29092, but it is actually advertised on 
port 9092 within the container environment configured by the KAFKA_ADVERTISED_LISTENERS property.

# Start Kafka Server

Let's start the Kafka server by spinning up the containers using the docker-compose command:
```shell
$ docker-compose up -d
Creating network "kafka_default" with the default driver
Creating kafka_zookeeper_1 ... done
Creating kafka_kafka_1     ... done
```

Now let's use the nc command to verify that both the servers are listening on the respective ports:

```shell
$ nc -z localhost 22181
Connection to localhost port 22181 [tcp/*] succeeded!
$ nc -z localhost 29092
Connection to localhost port 29092 [tcp/*] succeeded!
```

Additionally, we can also check the verbose logs while the containers are starting up and verify that the Kafka server 
is up:

```shell
$ docker-compose logs kafka | grep -i started
kafka_1      | [2021-04-10 22:57:40,413] DEBUG [ReplicaStateMachine controllerId=1] Started replica state machine with initial state -> HashMap() (kafka.controller.ZkReplicaStateMachine)
kafka_1      | [2021-04-10 22:57:40,418] DEBUG [PartitionStateMachine controllerId=1] Started partition state machine with initial state -> HashMap() (kafka.controller.ZkPartitionStateMachine)
kafka_1      | [2021-04-10 22:57:40,447] INFO [SocketServer brokerId=1] Started data-plane acceptor and processor(s) for endpoint : ListenerName(PLAINTEXT) (kafka.network.SocketServer)
kafka_1      | [2021-04-10 22:57:40,448] INFO [SocketServer brokerId=1] Started socket server acceptors and processors (kafka.network.SocketServer)
kafka_1      | [2021-04-10 22:57:40,458] INFO [KafkaServer id=1] started (kafka.server.KafkaServer)
```

With that, our Kafka setup is ready for use.

# Connection Using Kafka Tool

Finally, let's use the [Kafka Tool](https://kafkatool.com/download.html) GUI utility to establish a connection with 
our newly created Kafka server, and later, we'll visualize this setup:

