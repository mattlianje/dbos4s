package dbos4s

import java.util.UUID
import scala.sys.process._
import scala.util.Try

/**
 * A PostgreSQL instance an integration test can run against, plus the means to
 * dispose of it. Acquired either from an externally-supplied JDBC URL or from a
 * throwaway Docker container that this object starts and stops.
 */
final class PostgresContainer private (
  val jdbcUrl: String,
  val user: String,
  val password: String,
  dispose: () => Unit
) {
  def close(): Unit = dispose()
}

object PostgresContainer {

  private val PgDb    = "dbos4s_test"
  private val PgUser  = sys.env.get("PGUSER").filter(_.nonEmpty).getOrElse("postgres")
  private val PgPass  = sys.env.get("PGPASSWORD").filter(_.nonEmpty).getOrElse("dbos")
  private val PgImage =
    sys.env.get("DBOS_PG_IMAGE").filter(_.nonEmpty).getOrElse("postgres:16-alpine")

  /**
   * A ready-to-use Postgres, or `None` if neither an external URL nor a usable
   * Docker daemon is available (the suite then self-skips)
   */
  def acquire(): Option[PostgresContainer] = external().orElse(docker())

  /** Uses an externally-provided database when DBOS_SYSTEM_JDBC_URL is set .. */
  private def external(): Option[PostgresContainer] =
    sys.env.get("DBOS_SYSTEM_JDBC_URL").filter(_.nonEmpty).flatMap { url =>
      if (waitReady(url, PgUser, PgPass, timeoutMillis = 5000))
        Some(new PostgresContainer(url, PgUser, PgPass, () => ()))
      else None
    }

  /* Start a throwaway Postgres in Docker and tear it down on close... */
  private def docker(): Option[PostgresContainer] = {
    if (!dockerAvailable) return None

    val name           = s"dbos4s-it-${UUID.randomUUID().toString.take(8)}"
    val (startCode, _) = run(
      Seq(
        "docker",
        "run",
        "-d",
        "--rm",
        "--name",
        name,
        "-e",
        s"POSTGRES_USER=$PgUser",
        "-e",
        s"POSTGRES_PASSWORD=$PgPass",
        "-e",
        s"POSTGRES_DB=$PgDb",
        "-p",
        "5432",
        PgImage
      )
    )
    if (startCode != 0) return None

    hostPort(name, 5432) match {
      case Some(port) =>
        val url = s"jdbc:postgresql://localhost:$port/$PgDb"
        if (waitReady(url, PgUser, PgPass, timeoutMillis = 60000))
          Some(new PostgresContainer(url, PgUser, PgPass, () => remove(name)))
        else { remove(name); None }
      case None =>
        remove(name); None
    }
  }

  private def dockerAvailable: Boolean =
    Try(run(Seq("docker", "version", "--format", "{{.Server.Version}}"))._1 == 0)
      .getOrElse(false)

  private def hostPort(name: String, containerPort: Int): Option[String] = {
    val (code, out) = run(Seq("docker", "port", name, s"$containerPort/tcp"))
    if (code != 0) None
    else
      out
        .split("\n")
        .iterator
        .map(_.trim)
        .filter(_.nonEmpty)
        .map(_.split(":").last.trim)
        .find(_.nonEmpty)
  }

  private def remove(name: String): Unit = { run(Seq("docker", "rm", "-f", name)); () }

  /* Poll until a JDBC connection succeeds or the deadline passes. */
  private def waitReady(url: String, user: String, pass: String, timeoutMillis: Long): Boolean = {
    java.sql.DriverManager.setLoginTimeout(2)
    val deadline = System.currentTimeMillis() + timeoutMillis
    var ok       = false
    while (!ok && System.currentTimeMillis() < deadline) {
      ok = Try {
        val c = java.sql.DriverManager.getConnection(url, user, pass)
        try c.isValid(2)
        finally c.close()
      }.getOrElse(false)
      if (!ok) Thread.sleep(1000)
    }
    ok
  }

  /* Runs a command, and returns exit code + combined stdout + stderr */
  private def run(cmd: Seq[String]): (Int, String) = {
    val out  = new StringBuilder
    val sink = ProcessLogger(
      line => out.append(line).append('\n'),
      line => out.append(line).append('\n')
    )
    val code = Try(cmd ! sink).getOrElse(-1)
    (code, out.toString.trim)
  }
}
