package dbos4s.testing

import dbos4s._
import dev.dbos.transact.StartWorkflowOptions
import dev.dbos.transact.workflow.{Queue, SendMessage, StepOptions, WorkflowStatus}

import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable
import scala.concurrent.duration.FiniteDuration

/**
 * An in-memory [[Dbos]] for unit tests. It implements the same authoring surface
 * as the live transactor, but instead of a database it:
 *
 *   - runs each `step` body inline ... unless you stub it, exactly mirroring the
 *     Java guide's key fact that a *mocked* `runStep` never runs its lambda;
 *   - runs each `workflow` body synchronously on `start`, and is idempotent by
 *     id (re-starting a finished id replays the recorded result, body not re-run);
 *   - records every step, event, send and start so you can assert on them;
 *   - sets [[Dbos.workflowId]]/`stepId` around bodies, so context-derived inputs
 *     behave the same as in production.
 *
 * It does NOT exercise durability (checkpoint replay, crash recovery) ... that is
 * the live transactor's job, covered by integration tests against PostgreSQL.
 * Use this for the *logic*; use a real `Dbos` for the *durability*.
 *
 * @example
 * {{{
 * val dbos = TestDbos()
 *   .onStep("charge").returns(Receipt("ok"))   // stub the side-effecting step
 *
 * val checkout = dbos.workflow("checkout") {
 *   val r = dbos.step("charge")(realCharge())   // body skipped ... stub returned
 *   dbos.setEvent("phase", "charged")
 *   r
 * }
 *
 * assertEquals(checkout.start("order-1").result, Receipt("ok"))
 * assertEquals(dbos.steps, List("charge"))
 * assertEquals(dbos.events, List(("order-1", "phase", "charged")))
 * }}}
 */
final class TestDbos(val appName: String = "test") extends Dbos {

  private val stepLog  = mutable.ListBuffer.empty[String]
  private val eventLog = mutable.ListBuffer.empty[(String, String, Any)]
  private val sendLog  = mutable.ListBuffer.empty[(String, String, Any)]
  private val startLog = mutable.ListBuffer.empty[String]
  private val sleepLog = mutable.ListBuffer.empty[FiniteDuration]
  private val queueLog = mutable.ListBuffer.empty[String]
  private val results  = mutable.LinkedHashMap.empty[String, Any]
  private var launched = false
  private val stepSeq  = new AtomicInteger(0)

  private val streamLog =
    mutable.LinkedHashMap.empty[(String, String), mutable.ListBuffer[Any]]

  def steps: List[String] = stepLog.toList

  def events: List[(String, String, Any)] = eventLog.toList

  def sends: List[(String, String, Any)] = sendLog.toList

  def started: List[String] = startLog.toList

  def slept: List[FiniteDuration] = sleepLog.toList

  def queues: List[String] = queueLog.toList

  def streamWrites: List[(String, String, Any)] =
    streamLog.iterator.flatMap { case ((id, key), vs) =>
      vs.map(v => (id, key, v))
    }.toList

  def isLaunched: Boolean = launched

  private val stepStubs  = mutable.Map.empty[String, () => Any]
  private val eventSeeds = mutable.Map.empty[(String, String), Any]
  private val inbox      = mutable.Map.empty[String, mutable.Queue[Any]]

  def onStep(name: String): TestDbos.StepStub = new TestDbos.StepStub(this, name)

  private[testing] def stubStep(name: String, thunk: () => Any): this.type = {
    stepStubs(name) = thunk
    this
  }

  def seedEvent(targetId: String, key: String, value: Any): this.type = {
    eventSeeds((targetId, key)) = value
    this
  }

  def feed(topic: String, message: Any): this.type = {
    inbox.getOrElseUpdate(topic, mutable.Queue.empty).enqueue(message)
    this
  }

  def underlying =
    throw new UnsupportedOperationException(
      "TestDbos has no underlying DBOS ... it is an in-memory double. " +
        "Use a live Dbos for anything that needs the raw transactor."
    )

  def launch(): Unit   = launched = true
  def shutdown(): Unit = ()
  def close(): Unit    = ()

  def workflow[R](name: String)(body: => R): Workflow[R] =
    new Workflow[R] {
      def start(workflowId: String): Handle[R]            = run(workflowId)
      def start(options: StartWorkflowOptions): Handle[R] = run(options.workflowId())

      private def run(id: String): Handle[R] = {
        startLog += id
        val value = results.getOrElseUpdate(id, DbosContext.withWorkflow(id)(body))
        new TestHandle[R](id, value.asInstanceOf[R])
      }
    }

  def status(id: String): Option[WorkflowStatus] = None

  def result[R](id: String): R =
    results
      .getOrElse(id, throw new NoSuchElementException(s"no completed workflow '$id'"))
      .asInstanceOf[R]

  def step[T](opts: StepOptions)(body: => T): T = runStep(opts.name())(body)

  private def runStep[T](name: String)(body: => T): T = {
    stepLog += name
    stepStubs.get(name) match {
      case Some(thunk) => thunk().asInstanceOf[T]
      case None        => DbosContext.withStep(stepSeq.incrementAndGet())(body)
    }
  }

  def setEvent(key: String, value: Any): Unit =
    eventLog += ((currentWorkflowId, key, value))

  def event[A](targetId: String, key: String, timeout: FiniteDuration): Option[A] =
    eventSeeds
      .get((targetId, key))
      .orElse(eventLog.reverseIterator.collectFirst { case (`targetId`, `key`, v) => v })
      .map(_.asInstanceOf[A])

  def send(targetId: String, message: Any, topic: String): Unit = {
    sendLog += ((targetId, topic, message))
    inbox.getOrElseUpdate(topic, mutable.Queue.empty).enqueue(message)
  }

  def recv[A](topic: String, timeout: FiniteDuration): Option[A] =
    inbox.get(topic).filter(_.nonEmpty).map(_.dequeue().asInstanceOf[A])

  def sleep(d: FiniteDuration): Unit = sleepLog += d

  def registerQueue(queue: Queue): Unit = queueLog += queue.name()

  def sendBulk(messages: Seq[SendMessage]): Unit =
    messages.foreach(m => send(m.destinationId, m.message, m.topic))

  def events(targetId: String): Map[String, Any] = {
    val logged = eventLog.collect { case (`targetId`, k, v) => k -> v }.toMap
    val seeded = eventSeeds.collect { case ((`targetId`, k), v) => k -> v }.toMap
    logged ++ seeded
  }

  def writeStream(key: String, value: Any): Unit =
    streamLog.getOrElseUpdate((currentWorkflowId, key), mutable.ListBuffer.empty) += value

  private def currentWorkflowId: String = DbosContext.current.fold("")(_.workflowId)

  def closeStream(key: String): Unit = ()

  def readStream[A](workflowId: String, key: String): Iterator[A] =
    streamLog
      .getOrElse((workflowId, key), mutable.ListBuffer.empty)
      .iterator
      .map(_.asInstanceOf[A])
}

object TestDbos {

  def apply(appName: String = "test"): TestDbos = new TestDbos(appName)

  final class StepStub private[testing] (dbos: TestDbos, name: String) {

    def returns(value: Any): TestDbos = dbos.stubStep(name, () => value)

    def throws(t: Throwable): TestDbos = dbos.stubStep(name, () => throw t)
  }
}

final class TestHandle[R] private[testing] (val id: String, val result: R) extends Handle[R] {
  def status: WorkflowStatus =
    throw new UnsupportedOperationException("TestDbos does not model WorkflowStatus")
}
