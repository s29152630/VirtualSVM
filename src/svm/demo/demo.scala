package svm.demo
import svm.control.SVMEntity

object demo {
  def main(args: Array[String]) {
    val trainArgs = Array("SVM","-g", "2000", "-e", "0.001");
    val testTargetArray = Array(0, 1, 0, 1);
    val testArray = Array(
      Array(10.0, 20.0, 30.0, 40.0, 50.0),
      Array(11.1, 21.1, 31.1, 41.1, 51.1),
      Array(10.0, 20.0, 30.0, 40.0, 50.0),
      Array(11.1, 21.1, 31.1, 41.1, 51.1)
    );
    
    val demo = new SVMEntity(trainArgs, testTargetArray, testArray).run;

  }
}