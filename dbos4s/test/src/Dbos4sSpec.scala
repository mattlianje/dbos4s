package dbos4s

class Dbos4sSpec extends munit.FunSuite {

  test("version is reported from the underlying transactor") {
    assert(Dbos.version.nonEmpty, "expected a non-empty DBOS version string")
  }

  test("context accessors are empty outside a workflow") {
    assertEquals(Dbos.workflowId, None)
    assertEquals(Dbos.stepId, None)
    assert(!Dbos.inWorkflow)
    assert(!Dbos.inStep)
  }
}
