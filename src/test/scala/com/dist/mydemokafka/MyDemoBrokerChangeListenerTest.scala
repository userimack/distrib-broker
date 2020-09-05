package com.dist.mydemokafka

import com.dist.common.{TestUtils, ZookeeperTestHarness}
import com.dist.simplekafka.util.ZKStringSerializer
import com.dist.simplekafka.util.ZkUtils.Broker
import org.I0Itec.zkclient.ZkClient
//import com.dist.mydemokafka

class MyDemoBrokerChangeListenerTest extends ZookeeperTestHarness {
  test("should get notifies when new broker is registered") {
    val zkClient1 = new ZkClient(zkConnect, zkSessionTimeout, zkConnectionTimeout, ZKStringSerializer)
    val myZookeeperClient1 = new MyZookeeperClient(zkClient1)

    val listener = new MyDemoBrokerChangeListener(myZookeeperClient1)
    myZookeeperClient1.subscribeBrokerChangeListener(listener)
    myZookeeperClient1.registerBroker(Broker(0, "10.0.0.1", 8000))

    val zkClient2 = new ZkClient(zkConnect, zkSessionTimeout, zkConnectionTimeout, ZKStringSerializer)
    val myZookeeperClient2 = new MyZookeeperClient(zkClient2)
    myZookeeperClient2.registerBroker(Broker(1, "10.0.0.2", 8000))

    val zkClient3 = new ZkClient(zkConnect, zkSessionTimeout, zkConnectionTimeout, ZKStringSerializer)
    val myZookeeperClient3 = new MyZookeeperClient(zkClient3)
    myZookeeperClient3.registerBroker(Broker(2, "10.0.0.3", 8000))

    TestUtils.waitUntilTrue(() => {
      listener.liveBrokers.size == 3
    }, "Waiting for all the brokers to get added", 1000)

    zkClient2.close()
    zkClient3.close()

    TestUtils.waitUntilTrue(() => {
      listener.liveBrokers.size == 1
    }, "Waiting for all brokers to get added", 1000)
  }
}
