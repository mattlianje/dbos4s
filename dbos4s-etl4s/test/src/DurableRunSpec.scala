package dbos4s.etl4s

import _root_.etl4s._
import dbos4s._
import dbos4s.testing.TestDbos

class DurableRunSpec extends munit.FunSuite {

  test(".step lifts each node into an in-order durable step") {
    implicit val dbos: TestDbos = TestDbos()

    val incr   = Transform[Int, Int](_ + 1)
    val double = Transform[Int, Int](_ * 2)

    val pipeline = Extract(10) ~> incr.step("incr") ~> double.step("double")
    val workflow = pipeline.asWorkflow("calc")

    dbos.launch()
    assertEquals(workflow.start("run-1").result, 22)
    assertEquals(dbos.steps, List("incr", "double"))
    assertEquals(dbos.started, List("run-1"))
  }

  test("asWorkflow is idempotent by id — the body runs at most once") {
    implicit val dbos: TestDbos = TestDbos()

    val pipeline = Extract(1) ~> Transform[Int, Int](_ + 1).step("bump")
    val workflow = pipeline.asWorkflow("calc")

    assertEquals(workflow.start("run-1").result, 2)
    assertEquals(workflow.start("run-1").result, 2)
    assertEquals(dbos.steps, List("bump"))
  }

  test("@step lifts annotated nodes into in-order durable steps") {
    import AnnotatedSteps._
    dbos.launch()
    assertEquals(workflow.start("run-1").result, 22)
    assertEquals(dbos.steps, List("incr", "double"))
  }

  test("relineage carries the original node's lineage onto the lifted node") {
    val orig   = Node[Int, Int](_ + 1).lineageName("increment")
    val lifted = Node[Int, Int](_ + 1)
    assertEquals(relineage(orig, lifted).getLineage.map(_.name), Some("increment"))
  }
}

object AnnotatedSteps {
  implicit val dbos: TestDbos = TestDbos()

  @step("incr") val incr: Transform[Int, Int]     = Transform[Int, Int](_ + 1)
  @step("double") val double: Transform[Int, Int] = Transform[Int, Int](_ * 2)

  val workflow: Workflow[Int] = (Extract(10) ~> incr ~> double).asWorkflow("calc")
}
