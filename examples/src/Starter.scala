package example

import dbos4s.*
import org.slf4j.LoggerFactory
import scala.concurrent.{ExecutionContext, Future}

object Starter extends cask.MainRoutes {
  override def port = 7070
  override def host = "0.0.0.0"

  private val logger             = LoggerFactory.getLogger("dbos-starter-scala")
  private val StepsEvent         = "steps_event"
  private given ExecutionContext = ExecutionContext.global

  private val dbos = Dbos("dbos-starter-scala") {
    _.withDatabaseUrl(
      env("DBOS_SYSTEM_JDBC_URL", "jdbc:postgresql://localhost:5432/dbos_starter_scala")
    ).withDbUser(env("PGUSER", "postgres"))
      .withDbPassword(env("PGPASSWORD", "dbos"))
      .withAppVersion("0.1.0")
  }

  private def work(n: Int): Unit = {
    Thread.sleep(5000)
    logger.info("Workflow {} step {} completed!", Dbos.workflowId.orNull, n)
  }

  private val exampleWorkflow = dbos.workflow("exampleWorkflow") {
    for (i <- 1 to 3) {
      dbos.step(s"step$i")(work(i))
      dbos.setEvent(StepsEvent, i)
    }
  }

  @cask.get("/")
  def index() =
    cask.Response(
      scala.io.Source.fromResource("index.html").mkString,
      headers = Seq("Content-Type" -> "text/html")
    )

  @cask.get("/workflow/:taskId")
  def workflow(taskId: String) = {
    Future(exampleWorkflow.start(taskId))
    ""
  }

  @cask.get("/last_step/:taskId")
  def lastStep(taskId: String) =
    dbos.event[Int](taskId, StepsEvent).getOrElse(0).toString

  @cask.post("/crash")
  def crash() = {
    logger.warn("Crash endpoint called - terminating application")
    Runtime.getRuntime.halt(0)
    ""
  }

  initialize()

  override def main(args: Array[String]): Unit = {
    super.main(args)
    dbos.launch()
    Runtime.getRuntime.addShutdownHook(Thread(() => dbos.shutdown()))
    logger.info("Server started on http://localhost:7070")
    new java.util.concurrent.CountDownLatch(1).await()
  }

  private def env(key: String, default: String): String =
    Option(System.getenv(key)).filter(_.nonEmpty).getOrElse(default)
}
