package com.dist.mydemokafka

import java.util

import com.dist.simplekafka.common.Logging
import com.dist.simplekafka.util.ZkUtils.Broker
import org.I0Itec.zkclient.IZkChildListener

class MyDemoBrokerChangeListener(zookeeperClient: MyZookeeperClient) extends IZkChildListener with Logging {
  var liveBrokers: Set[Broker] = Set()
  import scala.jdk.CollectionConverters._

  override def handleChildChange(parentPath: String, currentBrokerList: util.List[String]) = {
    try {
      val currentBrokerIds = currentBrokerList.asScala.map(_.toInt).toSet
      val newBrokerIds = currentBrokerIds -- liveBrokerIds
      val deadBrokerIds = liveBrokerIds -- currentBrokerIds
      val newBrokers = newBrokerIds.map(b => zookeeperClient.getBrokerInfo(b))

      addNewBrokers(newBrokers)
      removeDeadBrokers(deadBrokerIds)

    } catch {
      case e: Throwable => error("Error while handling broker changes", e)
    }
  }

  private def addNewBrokers(newBrokers: Set[Broker]) = {
    newBrokers.foreach((b => liveBrokers += b))
  }

  private def removeDeadBrokers(deadBrokerIds: Set[Int]) = {
    val deadBrokers = liveBrokers.filter(b => deadBrokerIds.contains(b.id))
    liveBrokers = liveBrokers -- deadBrokers
  }

  private def liveBrokerIds: Set[Int] = {
    liveBrokers.map(broker => broker.id)
  }

}
