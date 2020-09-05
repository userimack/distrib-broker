package com.dist.mydemokafka

import com.dist.mykafka.MyBrokerChangeListener
import com.dist.simplekafka.common.JsonSerDes
import com.dist.simplekafka.util.ZkUtils.{Broker, createEphemeralPathExpectConflict}
import org.I0Itec.zkclient.{IZkChildListener, ZkClient}
import org.I0Itec.zkclient.exception.ZkNoNodeException

import scala.jdk.CollectionConverters.CollectionHasAsScala

class MyZookeeperClient(zkClient:ZkClient)  {
  def getBrokerInfo(brokerId: Int): Broker = {
    val data: String = zkClient.readData(getBrokerPath(brokerId.toInt))
    JsonSerDes.deserialize(data.getBytes, classOf[Broker])
  }

  def subscribeBrokerChangeListener(listener: MyBrokerChangeListener): Any = ???

  val BrokerTopicsPath = "/brokers/topics"
  val BrokerIdsPath = "/brokers/ids"
  val ControllerPath = "/controller"


  private def getBrokerPath(id: Int): String ={
    BrokerIdsPath + "/" + id
  }

  def subscribeBrokerChangeListener(listener: IZkChildListener): Option[List[String]] = {
    val result = zkClient.subscribeChildChanges(BrokerIdsPath, listener)
    Option(result).map(_.asScala.toList)
  }

  private def createParentPath(client: ZkClient, path: String): Unit = {
    val parentDir = path.substring(0, path.lastIndexOf("/"))
    if (parentDir.length != 0) {
      client.createPersistent(parentDir, true)
    }
  }

  def createEphemeralPath(client: ZkClient, path: String, data: String): Unit = {
    try {
      client.createEphemeral(path, data)
    } catch {
      case e: ZkNoNodeException => {
        createParentPath(client, path)
        client.createEphemeral(path, data)
      }
    }

  }

  def registerBroker(broker:Broker): Unit = {
    var brokerData = JsonSerDes.serialize(broker)
    var brokerPath = getBrokerPath(broker.id)
    createEphemeralPath(zkClient, brokerPath, brokerData)
  }

  def getAllBrokers(): Set[Broker] = {
    zkClient.getChildren(BrokerIdsPath).asScala.map(brokerId => {
      val data: String = zkClient.readData(getBrokerPath(brokerId.toInt))
      JsonSerDes.deserialize(data.getBytes, classOf[Broker])
    }).toSet
  }
}
