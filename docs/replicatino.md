# Replication

Welcome to the Replication extension!

In this extension, you'll extend your Redis server to support leader-follower replication. You'll be able to run multiple Redis servers with one acting as the "master" and the others as "replicas". Changes made to the master will be automatically replicated to replicas.


```plantuml
@startuml

title Replication

participant User

box "Master"
participant Master [
  =Master
  ----
  port 6379
]
participant Master_cb1
participant Master_cb2
end box

box "Slave 1"
participant Slave1R [
  =Slave1
  ----
  client
]
participant Slave1S [
  =Slave1
  ----
  port 6380
  replicaof "MASTER_IP 6379"
]
end box

Slave1S -> Slave1R ** : start client
Slave1R -> Master : connect
Master -> Master_cb1 ++ : client_connected_cb

group handshake
Slave1R -> Master_cb1 : PING
Master_cb1 -> Slave1R : PONG
Slave1R -> Master_cb1 : REPLCONF listening-port <PORT>
Master_cb1 -> Slave1R : OK
Slave1R -> Master_cb1 : REPLCONF capa eof capa psync2
Master_cb1 -> Slave1R : OK
Slave1R -> Master_cb1 : PSYNC ? -1
Master_cb1 -> Slave1R : +FULLRESYNC <REPL_ID> 0
Master_cb1 -> Master : read_db
Master -> Master_cb1 : <RDB>
Master_cb1 -> Slave1R : <RDB>
Master_cb1 -> Master : init_slave
end


User -> Master: connect
Master -> Master_cb2 ++ : client_connected_cb
User -> Master_cb2 : SET key value
Master_cb2 -> Master_cb2 : send_write [SET key value]
Master_cb2 -> Slave1R : SET key value

User -> Master_cb2 : WAIT 1 500
Master_cb2 -> Master_cb2 : wait_slaves

Master_cb2 -> Slave1R : REPLCONF GETACK *
Master_cb2 <- Slave1R : REPLCONF ACK <offset>

@enduml
```
