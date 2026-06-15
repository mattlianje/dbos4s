package dbos4s.etl4s

import _root_.etl4s.*
import _root_.etl4s.given
import scala.language.implicitConversions
import dbos4s._
import dbos4s.testing.TestDbos

class FunctionStepSpec extends munit.FunSuite {

  test("@step lifts a plain Function1 into an in-order durable step") {
    import FunctionSteps._
    dbos.launch()
    assertEquals(workflow.start("run-1").result, 21)
    assertEquals(dbos.steps, List("incr", "triple"))
  }
}

object FunctionSteps {
  implicit val dbos: TestDbos = TestDbos()

  @step("incr") val incr: Int => Int     = _ + 1
  @step("triple") val triple: Int => Int = _ * 3

  val workflow: Workflow[Int] = (Extract(6) ~> incr ~> triple).asWorkflow("calc")
}
