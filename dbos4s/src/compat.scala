package dbos4s

import java.time.{Duration => JDuration}
import java.util.Optional
import scala.concurrent.duration.FiniteDuration

private[dbos4s] object Jdk {

  def toScala[A](o: Optional[A]): Option[A] =
    if (o.isPresent) Some(o.get) else None

  def toScala[A](xs: java.util.List[A]): List[A] = {
    val b  = List.newBuilder[A]
    val it = xs.iterator()
    while (it.hasNext) b += it.next()
    b.result()
  }

  def toScala[K, V](m: java.util.Map[K, V]): Map[K, V] = {
    val b  = Map.newBuilder[K, V]
    val it = m.entrySet().iterator()
    while (it.hasNext) { val e = it.next(); b += (e.getKey -> e.getValue) }
    b.result()
  }

  def toScala[A](it: java.util.Iterator[A]): Iterator[A] =
    new Iterator[A] {
      def hasNext: Boolean = it.hasNext
      def next(): A        = it.next()
    }

  def toJava[A](xs: Seq[A]): java.util.List[A] = {
    val out = new java.util.ArrayList[A](xs.size)
    xs.foreach(out.add)
    out
  }

  def toJava(d: FiniteDuration): JDuration =
    JDuration.ofNanos(d.toNanos)
}
