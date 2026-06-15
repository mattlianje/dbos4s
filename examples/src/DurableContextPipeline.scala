package example

import _root_.etl4s.*
import dbos4s.*
import dbos4s.etl4s.*

object DurableContextPipeline {

  final case class Config(taxRate: Double)
  final case class Order(id: String, cents: Int)
  final case class Receipt(orderId: String, charged: Long)

  given dbos: Dbos = Dbos("etl4s-durable-ctx") {
    _.withDatabaseUrl(
      Option(System.getenv("DBOS_SYSTEM_JDBC_URL"))
        .filter(_.nonEmpty)
        .getOrElse("jdbc:postgresql://localhost:5432/etl4s_durable_ctx")
    ).withDbUser(Option(System.getenv("PGUSER")).getOrElse("postgres"))
      .withDbPassword(Option(System.getenv("PGPASSWORD")).getOrElse("dbos"))
  }

  val fetch: Extract[Any, Order] =
    Extract { Order(Dbos.workflowId.getOrElse("unknown"), 4200) }

  val applyTax: Reader[Config, Node[Order, Receipt]] =
    Node.requires[Config, Order, Receipt] { cfg => o =>
      Receipt(o.id, math.round(o.cents * (1 + cfg.taxRate)))
    }

  val pipeline: Reader[Config, Node[Any, Receipt]] =
    fetch.step("fetch") ~> applyTax.step("applyTax")

  val checkout: Workflow[Receipt] =
    pipeline.asWorkflow("checkout", Config(taxRate = 0.08))

  def main(args: Array[String]): Unit = {
    dbos.launch()
    val receipt = checkout.start("order-42").result
    println(s"Durable context pipeline produced: $receipt")
    dbos.shutdown()
  }
}
