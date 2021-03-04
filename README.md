# Key Features
 * Interactive serializable distributed transaction using MVCC+OCC  
 * Parallel commit which was originally proposed by cockroachdb, the main difference is that spermwhale writes transaction record only during commit or rollback.
 * Async task scheduler supporting dependency management. E.g., a txn set(k1, v1) then set (k1, v2), then second write must be chained. If a txn set(k1, v1) and set (k2, v2) and then write a transaction record, then all the writes can be paralleled.  
 * Centralized physical timestamp oracle server (simplest implementation)
 * Topology management, support auto config reload at runtime using etcd's watch feature.
 * Default use memory db as backend but this can be changed to any kind of kv store.
 * Fault tolerant transaction model. If transaction manager is gone during committing, then other transaction may detect the failure and help resolve it in the future (either doing commit clearing job or rollback) this prevent the starvation problem of 2pc.
 
# Limitation
  * Single point oracle server (may change to HLCTimestamp in the future), but the benefit is that it provides the highest level of consistency --- linearizability.
  * Long transaction is not supported, default max transaction length is 5 seconds. The value could be increased through configuration, but it will have side effect. When a transaction server is down and dirty record remains in the system, the unavailable time may increase.
 
# Usage
Local file system coordinator (doesn't support cluster auto reconfiguration at runtime)  
 <pre> ./spwtablet -cluster-name spermwhale -coordinator fs -coordinator-addr /tmp -gid 0  -port 20000 2>&1 1>&sptablet-0.log &
 ./spwtablet -cluster-name spermwhale -coordinator fs -coordinator-addr /tmp -gid 1  -port 30000 2>&1 1>&sptablet-1.log &
 ./spworacle -cluster-name spermwhale -coordinator fs -coordinator-addr /tmp -port 6666 2>&1 1>&sporacle.log &
 ./spwgate   -cluster-name spermwhale -coordinator fs -coordinator-addr /tmp -port-txn 9999 -port-kv 10001 2>&1 1>&spgate-1.log &
 ./spwgate   -cluster-name spermwhale -coordinator fs -coordinator-addr /tmp -port-txn 19999 -port-kv 20001 2>&1 1>&spgate-2.log &
</pre>

etcd coordinator (support auto reconfiguration):
 <pre> ./spwtablet -cluster-name spermwhale -coordinator etcd -coordinator-addr 127.0.0.1:2379 -coordinator-auth "" -gid 0 -port 20000 2>&1 1>&sptablet-0.log &  
 ./spwtablet -cluster-name spermwhale -coordinator etcd -coordinator-addr 127.0.0.1:2379 -coordinator-auth "" -gid 1  -port 30000 2>&1 1>&sptablet-1.log &
 ./spworacle -cluster-name spermwhale -coordinator etcd -coordinator-addr 127.0.0.1:2379 -coordinator-auth "" -port 6666 2>&1 1>&spworacle.log &
 ./spwgate   -cluster-name spermwhale -coordinator etcd -coordinator-addr 127.0.0.1:2379 -coordinator-auth "" -port-txn 19999 -port-kv 20001 2>&1 1>&spwgate-1.log &
 ./spwgate   -cluster-name spermwhale -coordinator etcd -coordinator-addr 127.0.0.1:2379 -coordinator-auth "" -port-txn 9999 -port-kv 10001 2>&1 1>&spwgate-2.log &
</pre>

**Interactive transaction client**  
 <pre> ./spwclient  </pre>

**NOTE**:
For using the interactive client, you may need to increase the stale write period.  
E.g.:
<pre>./spwtablet -cluster-name spermwhale --db redis -redis-port 6379 -txn-stale-write-threshold 90s -test -coordinator fs -coordinator-addr /tmp -gid 0  -port 20000 2>&1 1>&sptablet-0.log &
sleep 1s
./spwtablet -cluster-name spermwhale --db redis -redis-port 16379 -txn-stale-write-threshold 90s -test -coordinator fs -coordinator-addr /tmp -gid 1  -port 30000 2>&1 1>&sptablet-1.log &
sleep 1s
./spworacle -cluster-name spermwhale --loose -coordinator fs -coordinator-addr /tmp -port 6666 2>&1 1>&sporacle.log &
sleep 1s
./spwgate   -cluster-name spermwhale -txn-stale-write-threshold 90s -coordinator fs -coordinator-addr /tmp -port-txn 9999 -port-kv 10001 2>&1 1>&spgate-1.log &
sleep 1s
./spwgate   -cluster-name spermwhale -txn-stale-write-threshold 90s -coordinator fs -coordinator-addr /tmp -port-txn 19999 -port-kv 20001 2>&1 1>&spgate-2.log &
</pre> 

# Client Library
Under txn/client.go  
see examples/begin.go.  
Also provide a smart client under txn/smart_txn_client which supports auto retry.
  
For more usage, see the test cases under txn/txn_test.go
