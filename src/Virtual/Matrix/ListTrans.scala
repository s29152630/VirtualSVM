package Virtual.Matrix

object ListTrans {
  def TransListString(javalist: java.util.List[String]): List[String] = {
    scala.collection.JavaConversions.asScalaIterable(javalist).toList
  }
  def TransListInt(javalist: java.util.List[String]): List[Int] = {
    
    scala.collection.JavaConversions.asScalaIterable(javalist).toList.map(A => A.toInt)
  } 

}