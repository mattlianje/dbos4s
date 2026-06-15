package dbos4s.testing

import dbos4s._
import dev.dbos.transact.workflow.SendMessage
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.duration._

class TestDbosSpec extends munit.FunSuite {

  test("steps run inline by default; the workflow returns its body's value") {
    val dbos = TestDbos()
    val wf   = dbos.workflow("checkout") {
      val cents = dbos.step("price")(4000 + 200)
      dbos.setEvent("phase", "priced")
      cents
    }
    assertEquals(wf.start("order-1").result, 4200)
    assertEquals(dbos.steps, List("price"))
    assertEquals(dbos.events, List(("order-1", "phase", "priced")))
    assertEquals(dbos.started, List("order-1"))
  }

  test("a stubbed step returns its stub and never runs its body") {
    val ran  = new AtomicInteger(0)
    val dbos = TestDbos().onStep("charge").returns(999)
    val wf   = dbos.workflow("checkout") {
      dbos.step("charge") { ran.incrementAndGet(); 1 }
    }
    assertEquals(wf.start("order-1").result, 999)
    assertEquals(ran.get(), 0, "the stubbed step body was skipped")
    assertEquals(dbos.steps, List("charge"))
  }

  test("Dbos.workflowId is visible inside the body, as in production") {
    val dbos                 = TestDbos()
    var seen: Option[String] = None
    val wf                   = dbos.workflow("w") {
      dbos.step("peek") { seen = Dbos.workflowId; assert(Dbos.inStep); 0 }
    }
    wf.start("abc").result
    assertEquals(seen, Some("abc"))
    assert(!Dbos.inWorkflow, "context is cleared after the run")
  }

  test("re-starting the same id is idempotent — the body runs once") {
    val runs = new AtomicInteger(0)
    val dbos = TestDbos()
    val wf   = dbos.workflow("w") { dbos.step("s") { runs.incrementAndGet(); 42 } }
    assertEquals(wf.start("id").result, 42)
    assertEquals(wf.start("id").result, 42)
    assertEquals(runs.get(), 1)
    assertEquals(dbos.started, List("id", "id"))
  }

  test("send/recv round-trips through the in-memory inbox") {
    val dbos = TestDbos()
    dbos.send("other-wf", "ping", "greetings")
    assertEquals(dbos.recv[String]("greetings", 0.seconds), Some("ping"))
    assertEquals(dbos.recv[String]("greetings", 0.seconds), None)
    assertEquals(dbos.sends, List(("other-wf", "greetings", "ping")))
  }

  test("event() reads back what a workflow set, and pre-seeded upstream events") {
    val dbos = TestDbos().seedEvent("upstream", "ready", 7)
    val wf   = dbos.workflow("w") { dbos.setEvent("phase", "done"); () }
    wf.start("w1").result
    assertEquals(dbos.event[String]("w1", "phase"), Some("done"))
    assertEquals(dbos.event[Int]("upstream", "ready"), Some(7))
    assertEquals(dbos.event[String]("w1", "missing"), None)
  }

  test("a step stubbed to throw fails the workflow") {
    val dbos = TestDbos().onStep("charge").throws(new RuntimeException("declined"))
    val wf   = dbos.workflow("w") { dbos.step("charge")(1) }
    val ex   = intercept[RuntimeException](wf.start("x").result)
    assertEquals(ex.getMessage, "declined")
  }

  test("sendBulk delivers each message to its topic and records every send") {
    val dbos     = TestDbos()
    val messages = Seq(
      new SendMessage("wf-a", "hello", "greetings"),
      new SendMessage("wf-b", "go", "signals"),
      new SendMessage("wf-c", "bye", "farewells")
    )
    dbos.sendBulk(messages)
    assertEquals(
      dbos.sends,
      List(
        ("wf-a", "greetings", "hello"),
        ("wf-b", "signals", "go"),
        ("wf-c", "farewells", "bye")
      )
    )
    assertEquals(dbos.recv[String]("greetings", 0.seconds), Some("hello"))
    assertEquals(dbos.recv[String]("signals", 0.seconds), Some("go"))
    assertEquals(dbos.recv[String]("farewells", 0.seconds), Some("bye"))
  }

  test("events(targetId) returns every event a workflow published, keyed by name") {
    val dbos = TestDbos().seedEvent("upstream", "ready", "yes")
    val wf   = dbos.workflow("order") {
      dbos.setEvent("phase", "charged")
      dbos.setEvent("amount", "4200")
      ()
    }
    wf.start("order-1").result
    assertEquals(dbos.events("order-1"), Map("phase" -> "charged", "amount" -> "4200"))
    assertEquals(dbos.events("upstream"), Map("ready" -> "yes"))
  }

  test("writeStream / readStream round-trips through the in-memory stream buffer") {
    val dbos = TestDbos()
    val wf   = dbos.workflow("w") {
      dbos.writeStream("rows", "a")
      dbos.writeStream("rows", "b")
      dbos.closeStream("rows")
      ()
    }
    wf.start("w1").result
    assertEquals(dbos.readStream[String]("w1", "rows").toList, List("a", "b"))
    assertEquals(
      dbos.streamWrites,
      List(("w1", "rows", "a"), ("w1", "rows", "b"))
    )
  }

  test("an operational method has no in-memory model and says so") {
    val dbos = TestDbos()
    val ex   = intercept[UnsupportedOperationException](dbos.cancel("anything"))
    assert(ex.getMessage.startsWith("TestDbos has no underlying DBOS"))
  }
}
