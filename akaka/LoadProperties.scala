import java.io.FileInputStream
import java.util.Properties

import scala.collection.mutable

object LoadProperties {
  def getProp(prop:String)={
    val properties = new Properties()
    val path = Thread.currentThread().getContextClassLoader.getResource("akka.properties").getPath //文件要放到resource文件夹下
    properties.load(new FileInputStream(path))
    properties.getProperty(prop)
  }
}
