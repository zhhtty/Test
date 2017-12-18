import java.io.File

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

//worker -> master的注册消息
case class RegisterWorker(workerId: String,host:String,port:Int, memory: Int, cores: Int) extends Serializable

//worker -> master的心跳信息
case class Heartbeat(workerId: String) extends Serializable


//master -> worker注册成功的消息
case object RegisteredWorker extends Serializable

//Worker -> self
case object SendHeartbeat

//Master -> self
case object CheckTimeOutWorker

case class Task(file:File)

case class Result(result:Seq[(String,Int)])