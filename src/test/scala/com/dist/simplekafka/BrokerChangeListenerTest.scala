package com.dist.simplekafka

import com.dist.common.{TestUtils, ZookeeperTestHarness}
import com.dist.simplekafka.server.Config
import com.dist.simplekafka.util.ZkUtils.Broker
import com.dist.util.Networks

class BrokerChangeListenerTest extends ZookeeperTestHarness {
  test("should add new broker information to controller on change") {
    val config = new Config(1, new Networks().hostname(), TestUtils.choosePort(), zkConnect, List(TestUtils.tempDir().getAbsolutePath))
    val zookeeperClient: ZookeeperClientImpl = new ZookeeperClientImpl(config)
    zookeeperClient.registerBroker(Broker(0, "10.10.10.10", 8000))

    val socketServer1 = new TestSocketServer(config)
    val controller = new ZkController(zookeeperClient, config.brokerId, socketServer1)
    controller.startup()

    zookeeperClient.registerBroker(Broker(1, "10.10.10.11", 8001))
    zookeeperClient.registerBroker(Broker(2, "10.10.10.12", 8002))


    TestUtils.waitUntilTrue(() => {
      controller.liveBrokers.size == 3
    }, "Waiting for all brokers to get added", 1000)

    assert(controller.liveBrokers.size == 3)
  }
}
