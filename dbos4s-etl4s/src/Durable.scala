package dbos4s

import _root_.etl4s._
import dev.dbos.transact.workflow.StepOptions

/**
 * etl4s x DBOS bridge exntension methods
 *
 * {{{
 * import etl4s._
 * import dbos4s._
 * import dbos4s.etl4s._
 *
 * implicit val dbos: Dbos = ...
 *
 * val pipeline: Pipeline[Any, Receipt] =
 *   fetch.step("fetch")       ~>
 *   validate.step("validate") ~>
 *   charge.step("charge")
 *
 * val checkout = pipeline.asWorkflow("checkout")
 * dbos.launch()
 * checkout.start("order-42").result
 * }}}
 */
package object etl4s {

  /** Preserve the original node's lineage on the lifted node.
   *  TODO: Maybe a little useless ... revisit 
   */
  private[etl4s] def relineage[A, B](orig: Node[A, B], lifted: Node[A, B]): Node[A, B] =
    orig.getLineage.fold(lifted)(lifted.withLineage)

  /** Lift an etl4s [[Node]] into a durable, checkpointed DBOS step. */
  implicit final class DurableNodeOps[A, B](private val node: Node[A, B]) extends AnyVal {

    def step(name: String)(implicit dbos: Dbos): Node[A, B] =
      relineage(node, Node((a: A) => dbos.step(name)(node(a))))

    def step(opts: StepOptions)(implicit dbos: Dbos): Node[A, B] =
      relineage(node, Node((a: A) => dbos.step(opts)(node(a))))

    def retryStep(name: String, maxAttempts: Int)(implicit dbos: Dbos): Node[A, B] =
      step(new StepOptions(name).withMaxAttempts(maxAttempts))
  }

  /** Register a fully-lifted pipeline as a durable DBOS workflow. */
  implicit final class DurablePipelineOps[B](private val pipeline: Node[Any, B]) extends AnyVal {

    def asWorkflow(name: String)(implicit dbos: Dbos): Workflow[B] =
      dbos.workflow(name)(pipeline.unsafeRun())
  }

  /** Lift a context-dependent stage (`Reader[R, Node[A, B]]`) into a step. */
  implicit final class DurableReaderStageOps[R, A, B](private val reader: Reader[R, Node[A, B]])
      extends AnyVal {

    def step(name: String)(implicit dbos: Dbos): Reader[R, Node[A, B]] =
      reader.map(_.step(name))

    def step(opts: StepOptions)(implicit dbos: Dbos): Reader[R, Node[A, B]] =
      reader.map(_.step(opts))

    def retryStep(name: String, maxAttempts: Int)(implicit dbos: Dbos): Reader[R, Node[A, B]] =
      reader.map(_.retryStep(name, maxAttempts))
  }

  /** Provide context and register a lifted context-dependent pipeline. */
  implicit final class DurableReaderPipelineOps[R, B](private val reader: Reader[R, Node[Any, B]])
      extends AnyVal {

    def asWorkflow(name: String, context: R)(implicit dbos: Dbos): Workflow[B] =
      reader.provide(context).asWorkflow(name)
  }
}
