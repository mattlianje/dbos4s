package dbos4s

import dev.dbos.transact.workflow.Queue

import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger

/**
 * Self-contained DBOS integration test
 *
 * Just need a running Docker daemon
 *
 * To run against an existing Postgres instead, set `DBOS_SYSTEM_JDBC_URL`
 * (and optionally `PGUSER` / `PGPASSWORD`); Docker is then skipped entirely:
 *
 * {{{
 * DBOS_SYSTEM_JDBC_URL=jdbc:postgresql://localhost:5432/dbos4s_test \
 * PGUSER=postgres PGPASSWORD=dbos  ./mill dbos4s.test
 * }}}
 *
 * If neither a JDBC URL nor a usable Docker daemon is available, the whole
 * suite self-skips.
 */
class Dbos4sIntegrationSpec extends munit.FunSuite {

  private val pg        = PostgresContainer.acquire()
  private val available = pg.isDefined

  private val charges = new AtomicInteger(0)

  private lazy val dbos = {
    val c = pg.get
    Dbos("dbos4s-it") {
      _.withDatabaseUrl(c.jdbcUrl).withDbUser(c.user).withDbPassword(c.password)
    }
  }

  private lazy val checkout = dbos.workflow("checkout") {
    val amount = dbos.step("charge") { charges.incrementAndGet(); 4200 }
    dbos.setEvent("phase", "charged")
    amount
  }

  private lazy val emails = new Queue("emails")

  override def beforeAll(): Unit = if (available) {
    dbos.registerQueue(emails)
    checkout /* Force registration of the workflow before launch */
    dbos.launch()
  }

  override def afterAll(): Unit = if (available) {
    try dbos.shutdown()
    catch { case _: Throwable => () }
    pg.foreach(_.close())
  }

  private def requirePostgres(): Unit =
    assume(
      available,
      "no Postgres available (set DBOS_SYSTEM_JDBC_URL or start Docker) — skipping DBOS integration tests"
    )

  test("a durable workflow runs its steps, fires its event, and returns a result") {
    requirePostgres()
    val id      = s"checkout-${UUID.randomUUID()}"
    val before  = charges.get()
    val receipt = checkout.start(id).result
    assertEquals(receipt, 4200)
    assertEquals(charges.get(), before + 1, "the charge step ran exactly once")
    assertEquals(dbos.event[String](id, "phase"), Some("charged"))
  }

  test("re-starting the same workflow id is idempotent — steps do not re-run") {
    requirePostgres()
    val id     = s"checkout-${UUID.randomUUID()}"
    val first  = checkout.start(id).result
    val after  = charges.get()
    val second = checkout.start(id).result
    assertEquals(first, second)
    assertEquals(charges.get(), after, "the step did not re-execute on the second start")
  }

  test("a completed workflow's result is retrievable by id") {
    requirePostgres()
    val id = s"checkout-${UUID.randomUUID()}"
    checkout.start(id)
    assertEquals(dbos.result[Int](id), 4200)
  }

  test("a workflow enqueued onto a registered queue runs and returns its result") {
    requirePostgres()
    val id     = s"checkout-${UUID.randomUUID()}"
    val handle = checkout.enqueue(emails, id)
    assertEquals(handle.id, id)
    assertEquals(handle.result, 4200)
  }

  test("cancel and listWorkflows operate on a started workflow") {
    requirePostgres()
    val id = s"checkout-${UUID.randomUUID()}"
    checkout.start(id).result

    val all = dbos.listWorkflows()
    assert(all.nonEmpty, "listWorkflows() should see at least the started workflow")
    assert(all.exists(_.workflowId() == id), s"listWorkflows() should contain $id")

    val filtered = dbos.listWorkflows(_.withWorkflowIds(id))
    assert(filtered.exists(_.workflowId() == id), s"filtered listWorkflows should contain $id")

    dbos.cancel(id)
  }

  test("getQueue sees the statically registered queue") {
    requirePostgres()
    assert(dbos.getQueue("emails").isDefined, "the registered queue should be findable")
    assertEquals(dbos.getQueue("emails").get.name(), "emails")
    assert(dbos.getQueue("does-not-exist").isEmpty, "unknown queue should not be found")
  }
}
