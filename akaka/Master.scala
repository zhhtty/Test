import java.io.File
import java.util.concurrent.TimeUnit

import akka.actor.{Actor, ActorRef, ActorSelection, ActorSystem, Props}
import com.typesafe.config.ConfigFactory
import akka.pattern.ask
import akka.util.Timeout

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.language.postfixOps

class Master extends Actor {

  implicit val timeout = Timeout.apply(1L, TimeUnit.HOURS)

  val id2Workers = new mutable.HashMap[String, WorkerInfo]()
  val CHECK_INTERVAL = 15000

  val futures = new ArrayBuffer[Future[Any]]()

  val results = new ArrayBuffer[Result]()

  //Master创建之后就启动一个定时器，用来检测超时的Worker
  override def preStart(): Unit = {
    //导入隐式转换
    import context.dispatcher
    context.system.scheduler.schedule(0 millis, CHECK_INTERVAL millis, self, CheckTimeOutWorker)
  }

  override def receive: Receive = {
    case "hello" =>
      println("Master 已启动！")
    //Worker发送给Master的注册消息
    case RegisterWorker(workerId, host, port, memory, cores) =>
      //将注册消息保存起来
      println(workerId)
      val workerInfo = new WorkerInfo(workerId, host, port, memory, cores)
      //保存到集合
      id2Workers(workerId) = workerInfo
      //返回一个一个消息告诉Worker注册成功了
      sender() ! RegisteredWorker

      if (id2Workers.size == 2) {


        val file = new File("G:/test/")
        val files = file.listFiles()
        var arrWorkers = ArrayBuffer[ActorSelection]()
        for (id <- id2Workers) {
         println()
          var worker = context.actorSelection(s"akka.tcp://WorkerSystem@${id._2.host}:${id._2.port}/user/Worker")
          arrWorkers.append(worker)
        }

        for (f <- files if f.isFile) {
          //val worker01 = context.actorSelection(s"akka.tcp://WorkerSystem@${LoadProperties.getProp("worker01.host")}:${LoadProperties.getProp("worker01.port")}/user/Worker")
          //val worker02 = context.actorSelection(s"akka.tcp://WorkerSystem@${LoadProperties.getProp("worker02.host")}:${LoadProperties.getProp("worker02.port")}/user/Worker")
          if (f.hashCode() % 2 == 0) {
            val future: Future[Any] = arrWorkers(0) ? Task(f)
            futures += future
          } else {
            val future: Future[Any] = arrWorkers(1) ? Task(f)
            futures += future
          }

        }

        while (futures.size > 0) {
          val completeFuture = futures.filter(_.isCompleted)

          for (future <- completeFuture) {

            val result: Result = future.value.get.get.asInstanceOf[Result]

            results += result

            futures -= future

          }
          Thread.sleep(2000)

        }

        val finalResult = results.flatMap(_.result.toList).groupBy(_._1).mapValues(_.foldLeft(0)(_ + _._2))
          .toList.sortBy(_._2).reverse
        println(finalResult)
      }

    //Worker发送给Master的心跳信息
    case Heartbeat(workerId) =>
      //根据workerId到保存worker信息的map中查找
      if (id2Workers.contains(workerId)) {
        val workerInfo: WorkerInfo = id2Workers(workerId)
        //更新Worker的状态（上一次心跳的时间）
        val current = System.currentTimeMillis()
        workerInfo.lastHeartbeatTime = current
      }
    case CheckTimeOutWorker =>
      val current = System.currentTimeMillis()
      //过滤出超时的Worker
      val deadWorkers = id2Workers.values.filter(w => current - w.lastHeartbeatTime > CHECK_INTERVAL)
      //移除超时的worker
      deadWorkers.foreach(dw => {
        id2Workers -= dw.workerId
      })
      println("current works size : " + id2Workers.size)

  }
}

object Master {

  def main(args: Array[String]): Unit = {
    val host = LoadProperties.getProp("master.host")
    val port = LoadProperties.getProp("master.port").toInt

    val configStr =
      s"""
         |akka.actor.provider = "akka.remote.RemoteActorRefProvider"
         |akka.remote.netty.tcp.hostname = "$host"
         |akka.remote.netty.tcp.port = "$port"
    """.stripMargin

    println(configStr)

    val config = ConfigFactory.parseString(configStr)
    //actorSystem
    val actorSystem = ActorSystem("MasterSystem", config)
    val masterRef = actorSystem.actorOf(Props[Master], "Master")

    masterRef ! "hello"
    //结束
    Await.result(actorSystem.whenTerminated, Duration.Inf)


  }

}
