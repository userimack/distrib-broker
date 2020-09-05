package com.dist.mydemokafka

import com.dist.common.ZookeeperTestHarness
import com.dist.mydemokafka
import com.dist.simplekafka.util.ZkUtils.Broker
import org.I0Itec.zkclient.ZkClient

class MyZookeeperClientTest extends ZookeeperTestHarness {

  test("register broker with zookeeper") {

    val client = new MyZookeeperClient(zkClient)
    client.registerBroker(new Broker(1, "10.0.0.1", 80))

    assert(1 == client.getAllBrokers().size )
  }
}

