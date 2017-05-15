package cn.kwong

import java.awt.event.{ActionEvent, ActionListener}
import javax.swing.{JButton, JFrame}

import scala.util.matching.Regex

class HelloWorld {

}
object HelloWorld{
  def main(args: Array[String]): Unit = {
    /*lesson1
    println("Hello World!")
    val pianoTeacher = new PianoTeacher
    pianoTeacher.teach
    val pianoTeacher2 = new Human with Teacher with PianoPlayer with Ironman{
      override def teach: Unit = println("I am a mix pianoTeacher")
    }
    pianoTeacher2.teach
    val tmp = new Superman { println("caonima")}*/
///////////////////////////////////////////////////////////////////////////////////////////
 /* lesson2
    var worker = new Worker with TAround
    worker.doAction*/
    ///////////////////////////////////////////////////////////////////////////////////////////
/*  lesson3
    print("please input something: ")
    val line = Console.readLine()
    println()
    println(line)*/
    ///////////////////////////////////////////////////////////////////////////////////////////
    //模式匹配 + 正则
 /* lesson4
    val regex="""([0-9]+) ([a-z]+)""".r //pattern
    val line = "520 china"
    line match { //matcher
      case  regex(num,item) => println(num + "\t" + item)
      case _ => println("no match")
    }
    val regex(num,item) =  "520 china" //pattern matcher
    //那上面的变量是什么类型？
    print(num)
    //上面的赋值语句应该相当于 val num ,item <- regex("520 chine")
    */
    ///////////////////////////////////////////////////////////////////////////////////////////
/*  lesson5
    var increase = (x: Int) => x+1
    var increase2 = (_: Int) +1
    var f = (_: Int) + (_: Int)

    //偏函数
    def sum(a:Int, b:Int, c:Int) = a + b + c
    val f_sum = sum _
    println(f_sum(1,2,3))
    val f2_sum = sum(1,_:Int ,3)
    println(f2_sum(2))
    val list = List(18,2, 0 -11,-2,25)
//    list.filter((_ : Int) > 0 ).foreach(println)//传入一个函数
    list.filter(_  > 0 ).foreach(println) //由于类型推断，还可以这样
*/
///////////////////////////////////////////////////////////////////////////////////////////
    //闭包 -》 处理数据的语言需要
/*    val data = List(3,4,2,6,1,9)
    var sum = 0
    data.foreach(sum += _) //也就是 (x:Int) => sum += x 这个匿名函数 访问了不不在函数域内的变量sum ->称之为 闭包
    println(sum) //25
    //上面是函数 访问 变量

    def add(more: Int) = (x: Int) => x + more
    //由于函数与其它变量的地位是一样的，可看作的add的作用域内 定义 本地匿名函数 ，这个函数访问了参数more(闭包)，并且将返回值作为add的返回值,
//    def add(more: Int) = { def tmp(x: Int) = x + more ;}
    // 那么问题是，这个匿名函数在什么时候被调用的
    //上面两行理解错误，应该是 函数add接收一个参数more，返回一个函数（而不是值）,在此印证了函数与其它变量地位一样，都是第一，既能作为参数（foreach、filter），也能作为返回值
    val a = add(1)
    val b = add(9999)
    println(b(10)) //10009
    println(a(10)) //11  10是 x

    def add2(more: Int) = (x: Int) => (y : Int)=> y + x + more
    val c = add2(1)
    val d = c(2)
    println(d(10)) //13
  //思考，这样的方式能提供怎样的方便
    //深度定制函数功能
    */

    //高阶函数 -》 函数作为参数, 处理数据集合的时候常用
    (1 to 9).map( "*" * _).foreach(println)

    //SAM转换 single abstract method 传递的参数是函数，指定具体的工作细节 途径是隐式转换
    var data = 0
    val frame = new JFrame("SAM testing")
    var jButton = new JButton("counter")
    jButton.addActionListener(new ActionListener {
      override def actionPerformed(e: ActionEvent): Unit = {
        data += 1
        println(data)
      }
    })
    //上面代码等价的 SAM 写法 如下
    implicit def convertedAction(action: (ActionEvent) =>Unit ) ={
      new ActionListener{
        override def actionPerformed(e: ActionEvent): Unit = {action(e)}
      }
    }
    jButton.addActionListener((event:ActionEvent) => {data += 1;println(data)}) //java8 并不需要上面的隐式转换，而scala需要,那为何有类型推测的scala不设计成酱紫

    frame.setContentPane(jButton)
    frame.pack()
    frame.setVisible(true)

    //Curring 库里化 接收多和参数的函数 转变成单个参数的函数。途径为 函数返回 一个接收除了第一个参数外其它参数的函数
    def multiple(x:Int, y:Int) = x * y
    def multipleOne(x:Int) = (y:Int) => x * y
    println(multipleOne(6)(7))

    def currying(x:Int)(y:Int) = x * y

    val a = Array("Hello","Spark")
    val b = Array("Hello","Spark")
    println(a.corresponds(b)(_.equalsIgnoreCase(_)))


    //模式匹配
    val data1 = 30
    data1 match {
      case 1 => println("First")
      case 2 => println("Second")
      case _ => println("Not Known Number")
    }
    val result = data match {
      case i if i == 1 => "The First"
      case number if number == 2 => "The Second"
      case _ => "Not Known Number"
    }
    println(result)

    "Spark !" foreach( c => println(
      c match {
        case ' ' => "space"
        case ch => "char: " + ch
      }
    ))

    def match_type(t:Any) = t match {
      case p:Int => println("It is Integer")
      case p:String => println("It is String")
      case m:Map[_,_] => m foreach(println)
      case _ => println("Unknown type")
    }
    match_type(2)
    match_type(Map("Scala" -> "Spark"))

    def match_array(arr :Any) = arr match {
      case Array(0) => println("Array:" + "0")
      case Array(x, y) => println("Array:"+ x + y)
      case Array(0, _*) =>println("Array:" + "0 ...")
      case _ => println("something else")
    }
    match_array(Array(0))
    match_array(Array(0,1))
    match_array(Array(0,1,2,3,4,5))

    def match_list(lst :Any) = lst match {
      case List(0) => println("List:" + "0")
      case x::y::Nil => println("List:"+ x + y)
      case 0:: tail =>println("List:" + "0 ...")
      case _ => println("something else")
    }
    match_list(List(0))
    match_list(List(0,1))
    match_list(List(0,1,2,3,4,5))
  }

  def match_tuple(tuple :Any) = tuple match {
    case (0,_) => println("tuple:" + "0")
    case (x,0) => println("tuple:"+ x )
    case _ => println("something else")
  }
  match_tuple((0,"scala"))
  match_tuple((2,0))
  match_tuple((0,1,2,3,4,5))


}

class Human{
  println("Human")
}
trait Teacher extends Human{
  println("Teacher")
  def teach
}
trait PianoPlayer extends Human{
  println("PianoPlayer")
  def playPiano = println("I'm playing piano")
}
class PianoTeacher extends Human with Teacher with PianoPlayer{//这里trait必须有相同的父类或者无父类
  override def teach: Unit = println("I'm training students")
}

trait Superman{
  println("Superman")
}
trait Ironman extends Superman{
  println("Ironman")
}
//AOP
trait Action{
  def doAction
}
trait TAround extends Action{
  abstract override def doAction = {
     println("before action in around")
     super.doAction
     println("after action in around")
   }
}
class Worker extends Action{
  override def doAction: Unit = {
    println("I am 搬砖ing")
  }
}
//情况：Action为abstract class && TAround 为trait && Worker extends Action with TAround ->编译不过
//情况2: Action 为abstract class && TAround 为 abstract class && Worker extends TAround ->编译不过
//情况3：Action 为trait && TAround 为trait && Worker extends Action with TAround ->编译通过，并成功执行AOP，但切面类中的doAction方法必须是abstract override的
//情况4：Action 为abstract class && TAround 为trait && Worker extends Action with TAround ->编译通过，并成功执行AOP，但切面类中的doAction方法必须是abstract override的
//总结：1、trait 和 abstract class 都可以有抽象方法；2、abstract override只能在 trait 中修饰方法；3、只有abstract override修饰的方法才能调用超类中的 抽象方法
//      4、构造器从左到右extends .. with ...
//思考：scala这种设计（让 trait 可以调用 超类的抽象方法 (而不让 abstract class 调用超类的抽象方法，但trait和abstract class都可以有方法实现)）的目的是什么
// 1、trait 主要满足多重继承，那么只要 with 的 trait 越多，则拥有越多的 trait 带来的“特性”；
// 2、trait 还可以 extends class ,但一旦继承某个类之后，其它类想要 extends 这个 trait 相当于 extends 了它的超类，从而无法 继承别的类 或者别的继承了非同一超类的 trait，
//    这与多重继承的本意相违背，限制了多个 trait 只能修饰同一种东西



//package
//类。对象。成员的 的作用域扩展
package spark{
  package navigation{

    import cn.kwong.spark.navigation

    private[spark] class Navigator{
      protected[navigation] def useStartChart(): Unit ={
        var visit = new legOfJourney
        visit.distance//ok
     //   visit.distance2//no ok
      }
     class legOfJourney{
        private[Navigator] val distance = 100
        private val distance2 = 100

      }
      private[this] var speed = 200
    }
  }

  package launch{
    import navigation._
    object Vehicle{
      private [launch] val guide = new Navigator

     // var host = new legOfJourney
    }
  }
}

//伴生类 与 伴生对象 可以互相访问 private



