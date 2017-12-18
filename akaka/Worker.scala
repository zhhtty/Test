import java.util.UUID

import akka.actor.{Actor, ActorSelection, ActorSystem, Props}
import com.typesafe.config.ConfigFactory

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.io.Source

class Worker() extends Actor{
    val host = LoadProperties.getProp("worker01.host")
    val port = LoadProperties.getProp("worker01.port").toInt
    val masterHost = LoadProperties.getProp("master.host")
    val masterPort = LoadProperties.getProp("master.port").toInt
    val memory = LoadProperties.getProp("memery").toInt
    val cores = LoadProperties.getProp("cores").toInt
    var master: ActorSelection = _
    val WORKER_ID: String = UUID.randomUUID().toString
    override def preStart(): Unit = {
        master = context.actorSelection(s"akka.tcp://MasterSystem@$masterHost:$masterPort/user/Master")
        master ! RegisterWorker(WORKER_ID,host,port,memory,cores)
    }
    override def receive: Receive = {
        case RegisteredWorker =>
            //Worker启动一个定时器，定期向Master发送心跳
            //利用akka启动一个定时器,自己给自己发消息
            import context.dispatcher
            context.system.scheduler.schedule(0 millis, 10000 millis, self, SendHeartbeat)

        case SendHeartbeat =>
            //执行判断逻辑
            //向Master发送心跳
            println("Worker01 已启动！")
            master ! Heartbeat(WORKER_ID)
        case Task(file) =>
            val result:Map[String,Int] = Source.fromFile(file).getLines().toList.flatMap(_.split(" ")).map((_,1)).groupBy(_._1)
              .mapValues(_.length)
            println(result)
            sender() ! Result(result.toSeq)
    }
}
object Worker {

    def main(args: Array[String]): Unit = {
        val host = LoadProperties.getProp("worker01.host")
        val port = LoadProperties.getProp("worker01.port").toInt
        val configStr =
            s"""
               |akka.actor.provider = "akka.remote.RemoteActorRefProvider"
               |akka.remote.netty.tcp.hostname = "$host"
               |akka.remote.netty.tcp.port = "$port"
         """.stripMargin

        val config = ConfigFactory.parseString(configStr)
        //ActorSystem老大，辅助创建和监控下面的Actor，他是单例的
        val actorSystem = ActorSystem("WorkerSystem", config)
        //创建Actor
        actorSystem.actorOf(Props(new Worker()), "Worker")

        Await.result(actorSystem.whenTerminated, Duration.Inf)





    }

}
