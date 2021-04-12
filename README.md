# Bref
  **SpermwhaleKV** is a distributed key-value store aims to add distributed transaction ability to most kinds of backend store, such as Mongodb, MySQL, HBase, etc.
# Key Features
 * **Interactive serializable distributed transaction** using **MVTO** (Multi Version Timestamp Ordering)  
 * Default isolation level is **Serializable**, but also support **Snapshot Isolation** for readonly transactions.
 * **Parallel Commit** which was originally proposed by cockroachdb, the main difference is that spermwhale writes transaction record only during commit or rollback.
 * **Read-modify-write Transaction Queue** which can minimize the retry times by put all read-modify-write transactions of the same key into a queue.
 * **Concurrent Write** of same key for direct-write transactions (write value without any previous read)
 * **Various Backend Store** preovided that the interface for them are implemented. Currently support 3 kinds of backend store: Memory, Redis(discareded), MongoDB. The defined interface required the backend support single line transaction and **floor** (max version below or equal to a required version) api.
 * **Async Task Scheduler** supporting dependency management. E.g., a txn set(k1, v1) then set (k1, v2), then second write must be chained. If a txn set(k1, v1) and set (k2, v2) and then write a transaction record, then all the writes can be paralleled.  
 * **Fault-tolerant Transaction Model** If transaction manager is gone during committing, then other transaction may detect the failure and help resolve it in the future (either doing commit clearing job or rollback) this prevent the starvation problem of 2pc.
 * Provide an option to wait transaction committed/aborted on tablets when read a record with write intent (dirty data). This can be used together with read-modify-write queue so that can have **pipelined** transaction execution which could minimize the wait time. It works as below: Suppoing 2 transactions, both read(k)->increment(k, 1)->write(k), T2 can read the value written by T1 just after T1 has finished the writing, and then wait T1 commit.
 * Centralized physical timestamp oracle server (simplest implementation)
 * Topology management, support auto config reload at runtime using etcd's watch feature.
 
# Limitation
  * Single point oracle server (may change to HLCTimestamp in the future), but the benefit is that it provides the highest level of consistency --- linearizability.
  * Long transaction is not supported, default max transaction length is 5 seconds. The value could be increased through configuration, but it will have side effect: tablets will have to wait longer before serving, this increase the unavailable time of the whole system.
  
# Transaction Design
  If a transaction sees a key with write intent during reading, it will try to find out the commit status of the transaction.  
   
  If the transaction record doesn't exist, it may prevent the transaction record from being written in the future using an atomic semantic (all transaction records needs write intent to proceed too, just like normal keys). This can be used to safely rollback a transaction without transaction record.   
  
  If transaction records exists, it will check the written keys of the transaction one-by-one and consider the transaction committed in one of the following 2 cases:
  1. one of the keys written by the transaction's write intent has been cleared.
  2. all keys written by the transaction exist and matches the interval version recorded in transaction record (in case of write same keys multiple times)
  
  Please refer to TxnStore::inferTransactionRecordWithRetry() in txn/txn_store.go and Txn::CheckCommitState and Txn::Commit in txn/txn.go  
 
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
<pre>./spwtablet -cluster-name spermwhale -logtostderr --db redis -redis-port 6379 -txn-stale-write-threshold 90s -test -coordinator fs -coordinator-addr /tmp -gid 0  -port 20000 2>&1 1>&sptablet-0.log &
sleep 1s
./spwtablet -cluster-name spermwhale -logtostderr --db redis -redis-port 16379 -txn-stale-write-threshold 90s -test -coordinator fs -coordinator-addr /tmp -gid 1  -port 30000 2>&1 1>&sptablet-1.log &
sleep 1s
./spworacle -cluster-name spermwhale -logtostderr --loose -coordinator fs -coordinator-addr /tmp -port 6666 2>&1 1>&sporacle.log &
sleep 1s
./spwgate   -cluster-name spermwhale -logtostderr -wound-uncommitted-txn-threshold 15s -coordinator fs -coordinator-addr /tmp -port-txn 9999 -port-kv 10001 2>&1 1>&spgate-1.log &
sleep 1s
./spwgate   -cluster-name spermwhale -logtostderr -wound-uncommitted-txn-threshold 15s -coordinator fs -coordinator-addr /tmp -port-txn 19999 -port-kv 20001 2>&1 1>&spgate-2.log &
</pre> 

# Client Library
Under txn/client.go  
see examples/begin.go.  
Also provide a smart client under txn/smart_txn_client which supports auto retry.
  
For more usage, see the test cases under txn/txn_test.go
