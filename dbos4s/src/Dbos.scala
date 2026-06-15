package dbos4s

import dev.dbos.transact.{DBOS, StartWorkflowOptions}
import dev.dbos.transact.config.DBOSConfig
import dev.dbos.transact.execution.ThrowingSupplier
import dev.dbos.transact.workflow.{
  ForkOptions,
  ListWorkflowsInput,
  Queue,
  QueueOptions,
  ScheduleStatus,
  SendMessage,
  StepInfo,
  StepOptions,
  WorkflowHandle => JWorkflowHandle,
  WorkflowSchedule,
  WorkflowStatus
}

import java.time.Instant
import scala.concurrent.duration.{Duration, DurationDouble, FiniteDuration}

trait Dbos extends AutoCloseable {

  def underlying: DBOS

  /* --- LIFECYCLE --- */

  /** Connect to the database and begin executing workflows. Call once, after
   *  all workflows and queues are registered.
   */
  def launch(): Unit

  /** Gracefully stop the executor. */
  def shutdown(): Unit

  /** Alias for [[shutdown]], so `Dbos` works in `Using`/try-with-resources. */
  def close(): Unit

  /* --- WORKFLOWS --- */

  /**
   * Register a durable, no-argument workflow under `name`. Must be called
   * before [[launch]].
   *
   * @param name a stable name ... DBOS uses it to recover in-flight runs across
   *             restarts, so keep it constant across deploys.
   * @param body the workflow body, evaluated on every (re)execution.
   * @return a [[Workflow]] you can `start` as many times as you like.
   */
  def workflow[R](name: String)(body: => R): Workflow[R]

  def status(id: String): Option[WorkflowStatus]

  def result[R](id: String): R

  /* --- STEPS --- */

  /**
   * Run `body` as a checkpointed step. `maxAttempts` is the total number of
   * tries (1 = no retry); on failure DBOS waits `retryInterval`, growing it by
   * `backoff` each attempt. Defaults mirror `dev.dbos:transact`.
   */
  final def step[T](
    name: String,
    maxAttempts: Int = 1,
    retryInterval: FiniteDuration = StepOptions.DEFAULT_INTERVAL_SECONDS.seconds,
    backoff: Double = StepOptions.DEFAULT_BACKOFF
  )(body: => T): T =
    step(
      new StepOptions(name)
        .withMaxAttempts(maxAttempts)
        .withRetryInterval(Jdk.toJava(retryInterval))
        .withBackoffRate(backoff)
    )(body)

  def step[T](opts: StepOptions)(body: => T): T

  /* --- MESSAGING --- */

  def setEvent(key: String, value: Any): Unit

  def event[A](targetId: String, key: String, timeout: FiniteDuration): Option[A]

  final def event[A](targetId: String, key: String): Option[A] =
    event(targetId, key, Duration.Zero)

  def send(targetId: String, message: Any, topic: String): Unit

  def recv[A](topic: String, timeout: FiniteDuration): Option[A]

  def sleep(d: FiniteDuration): Unit

  /* --- QUEUES --- */

  def registerQueue(queue: Queue): Unit

  /* --- WORKFLOW MANAGEMENT --- */

  def cancel(id: String): Unit = underlying.cancelWorkflow(id)

  def resume[R](id: String): Handle[R] =
    new LiveHandle[R](underlying.resumeWorkflow[AnyRef, Exception](id))

  def fork[R](id: String, startStep: Int): Handle[R] =
    new LiveHandle[R](underlying.forkWorkflow[AnyRef, Exception](id, startStep))

  def fork[R](id: String, startStep: Int, opts: ForkOptions): Handle[R] =
    new LiveHandle[R](underlying.forkWorkflow[AnyRef, Exception](id, startStep, opts))

  def retrieve[R](id: String): Handle[R] =
    new LiveHandle[R](underlying.retrieveWorkflow[AnyRef, Exception](id))

  def delete(id: String): Unit = underlying.deleteWorkflow(id)

  def listWorkflows(input: ListWorkflowsInput): List[WorkflowStatus] =
    Jdk.toScala(underlying.listWorkflows(input))

  def listWorkflows(configure: ListWorkflowsInput => ListWorkflowsInput): List[WorkflowStatus] =
    listWorkflows(configure(new ListWorkflowsInput()))

  def listWorkflows(): List[WorkflowStatus] =
    listWorkflows(new ListWorkflowsInput())

  def listSteps(id: String): List[StepInfo] =
    Jdk.toScala(underlying.listWorkflowSteps(id))

  /* --- QUEUE MANAGEMENT --- */

  def updateQueue(name: String, opts: QueueOptions): Unit =
    underlying.updateQueue(name, opts)

  def findQueue(name: String): Option[Queue] =
    Jdk.toScala(underlying.findQueue(name))

  def getQueue(name: String): Option[Queue] =
    Jdk.toScala(underlying.getQueue(name))

  def deleteQueue(name: String): Boolean =
    underlying.deleteQueue(name)

  def listQueues(): List[Queue] =
    Jdk.toScala(underlying.listQueues())

  def createSchedule(s: WorkflowSchedule): Unit =
    underlying.createSchedule(s)

  def schedule(name: String): Option[WorkflowSchedule] =
    Jdk.toScala(underlying.getSchedule(name))

  def listSchedules(): List[WorkflowSchedule] =
    listSchedules(Nil, Nil, Nil)

  def listSchedules(
    statuses: Seq[ScheduleStatus],
    workflowNames: Seq[String],
    scheduleNamePrefixes: Seq[String]
  ): List[WorkflowSchedule] =
    Jdk.toScala(
      underlying.listSchedules(
        Jdk.toJava(statuses),
        Jdk.toJava(workflowNames),
        Jdk.toJava(scheduleNamePrefixes)
      )
    )

  def deleteSchedule(name: String): Unit = underlying.deleteSchedule(name)

  def pauseSchedule(name: String): Unit = underlying.pauseSchedule(name)

  def resumeSchedule(name: String): Unit = underlying.resumeSchedule(name)

  def triggerSchedule[R](name: String): Handle[R] =
    new LiveHandle[R](underlying.triggerSchedule[AnyRef, Exception](name))

  def backfillSchedule(name: String, from: Instant, to: Instant): List[Handle[Any]] =
    Jdk
      .toScala(underlying.backfillSchedule(name, from, to))
      .map(h => new LiveHandle[Any](h))

  /* --- STREAM + BULK --- */

  def sendBulk(messages: Seq[SendMessage]): Unit

  def events(targetId: String): Map[String, Any]

  def writeStream(key: String, value: Any): Unit

  def closeStream(key: String): Unit

  def readStream[A](workflowId: String, key: String): Iterator[A]
}

/**
 * Live [[Dbos]] ... just a thin wrapper over `dev.dbos.transact.DBOS`
 */
final class LiveDbos private[dbos4s] (val underlying: DBOS) extends Dbos {

  def launch(): Unit   = underlying.launch()
  def shutdown(): Unit = underlying.shutdown()
  def close(): Unit    = underlying.close()

  def workflow[R](name: String)(body: => R): Workflow[R] = {
    /* DBOS invokes the registered method reflectively with a serialized arg
     * array. We register a Scala Function0 and hand DBOS its `apply` method.
     * Unit bodies return `null` rather than scala.runtime.BoxedUnit, which
     * Jackson cannot serialize.
     */
    val fn: Function0[AnyRef] = () => box(body)
    val method                = classOf[Function0[_]].getMethod("apply")
    val registered            = underlying
      .integration()
      .registerWorkflow(name, fn.getClass.getName, null, fn, method, null, null)
    new LiveWorkflow[R](underlying, registered)
  }

  def status(id: String): Option[WorkflowStatus] =
    Jdk.toScala(underlying.getWorkflowStatus(id))

  def result[R](id: String): R =
    underlying.getResult[R, Exception](id)

  def step[T](opts: StepOptions)(body: => T): T = {
    val supplier: ThrowingSupplier[AnyRef, Exception] = () => box(body)
    underlying.runStep(supplier, opts).asInstanceOf[T]
  }

  def setEvent(key: String, value: Any): Unit =
    underlying.setEvent(key, value.asInstanceOf[AnyRef])

  def event[A](targetId: String, key: String, timeout: FiniteDuration): Option[A] =
    Jdk.toScala(underlying.getEvent[A](targetId, key, Jdk.toJava(timeout)))

  def send(targetId: String, message: Any, topic: String): Unit =
    underlying.send(targetId, message.asInstanceOf[AnyRef], topic)

  def recv[A](topic: String, timeout: FiniteDuration): Option[A] =
    Jdk.toScala(underlying.recv[A](topic, Jdk.toJava(timeout)))

  def sleep(d: FiniteDuration): Unit = underlying.sleep(Jdk.toJava(d))

  def registerQueue(queue: Queue): Unit = underlying.registerQueue(queue)

  def sendBulk(messages: Seq[SendMessage]): Unit =
    underlying.sendBulk(Jdk.toJava(messages))

  def events(targetId: String): Map[String, Any] =
    Jdk.toScala(underlying.getAllEvents(targetId))

  def writeStream(key: String, value: Any): Unit =
    underlying.writeStream(key, value.asInstanceOf[AnyRef])

  def closeStream(key: String): Unit = underlying.closeStream(key)

  def readStream[A](workflowId: String, key: String): Iterator[A] =
    Jdk.toScala(underlying.readStream(workflowId, key)).map(_.asInstanceOf[A])

  /** Box a step/workflow result for DBOS's Jackson serializer, mapping the
   *  Scala `Unit` value to `null` to dodge BoxedUnit...
   */
  private def box(value: => Any): AnyRef =
    value match {
      case ()    => null
      case other => other.asInstanceOf[AnyRef]
    }
}

object Dbos {

  def apply(config: DBOSConfig): LiveDbos = new LiveDbos(
    new DBOS(config)
  ) /* Wrap already bult `DBOSConfig` */

  def apply(dbos: DBOS): LiveDbos = new LiveDbos(dbos) /* Wrap existing raw `DBOS` instance */

  /**
   * Build a live [[Dbos]] from sensible defaults for `appName`...
   *
   * @example
   * {{{
   * val dbos = Dbos("payments") {
   *   _.withDatabaseUrl("jdbc:postgresql://localhost:5432/payments")
   *    .withDbUser("postgres")
   *    .withDbPassword("dbos")
   * }
   * }}}
   */
  def apply(appName: String)(configure: DBOSConfig => DBOSConfig): LiveDbos =
    apply(configure(DBOSConfig.defaults(appName)))

  /*
   * Execution context accessors ...
   */

  def workflowId: Option[String] =
    DbosContext.current.map(_.workflowId).orElse(Option(DBOS.workflowId()))

  def stepId: Option[Int] =
    DbosContext.current.flatMap(_.stepId).orElse(Option(DBOS.stepId()).map(_.intValue))

  def inWorkflow: Boolean = DbosContext.current.isDefined || DBOS.inWorkflow()

  def inStep: Boolean =
    DbosContext.current.exists(_.stepId.isDefined) || DBOS.inStep()

  /** The DBOS library version. */
  def version: String = DBOS.version()
}

trait Workflow[R] {

  def start(workflowId: String): Handle[R]

  def start(options: StartWorkflowOptions): Handle[R]

  /** Sugar for [[start]] with an id: `checkout("order-42")`. */
  def apply(workflowId: String): Handle[R] = start(workflowId)

  /** Enqueue this workflow onto the named queue (registered via
   *  [[Dbos.registerQueue]]) instead of running it inline. The queue's
   *  concurrency and rate limits then govern when it executes.
   */
  final def enqueue(queue: String, workflowId: String): Handle[R] =
    start(new StartWorkflowOptions(workflowId).withQueue(queue))

  final def enqueue(queue: Queue, workflowId: String): Handle[R] =
    start(new StartWorkflowOptions(workflowId).withQueue(queue))
}

/** A live handle to a started workflow. */
trait Handle[R] {

  def id: String

  def result: R

  def status: WorkflowStatus
}

/** The live [[Workflow]] ... wraps a DBOS `RegisteredWorkflow`. */
final class LiveWorkflow[R] private[dbos4s] (
  underlying: DBOS,
  registered: dev.dbos.transact.execution.RegisteredWorkflow
) extends Workflow[R] {

  def start(workflowId: String): Handle[R] =
    start(new StartWorkflowOptions(workflowId))

  def start(options: StartWorkflowOptions): Handle[R] = {
    val handle = underlying
      .integration()
      .startRegisteredWorkflow(registered, Array.empty[AnyRef], options)
    new LiveHandle[R](handle)
  }
}

/** The live [[Handle]] ... wraps a DBOS `WorkflowHandle`. */
final class LiveHandle[R] private[dbos4s] (val underlying: JWorkflowHandle[_, _])
    extends Handle[R] {
  def id: String             = underlying.workflowId()
  def result: R              = underlying.getResult().asInstanceOf[R]
  def status: WorkflowStatus = underlying.getStatus()
}
