package example

import _root_.etl4s.*
import dbos4s.*
import dbos4s.etl4s.*

object DurablePipeline {

  final case class Order(id: String, cents: Int)
  final case class Receipt(orderId: String, charged: Int)

  given dbos: Dbos = Dbos("etl4s-durable") {
    _.withDatabaseUrl(
      Option(System.getenv("DBOS_SYSTEM_JDBC_URL"))
        .filter(_.nonEmpty)
        .getOrElse("jdbc:postgresql://localhost:5432/etl4s_durable")
    ).withDbUser(Option(System.getenv("PGUSER")).getOrElse("postgres"))
      .withDbPassword(Option(System.getenv("PGPASSWORD")).getOrElse("dbos"))
  }

  val fetch: Extract[Any, Order] =
    Extract { Order(Dbos.workflowId.getOrElse("unknown"), 4200) }

  val validate: Transform[Order, Order] =
    Transform[Order, Order] { o =>
      if (o.cents > 0) o else sys.error(s"invalid order ${o.id}")
    }

  val charge: Load[Order, Receipt] =
    Load[Order, Receipt](o => Receipt(o.id, o.cents))

  val pipeline: Pipeline[Any, Receipt] =
    fetch.step("fetch") ~> validate.step("validate") ~> charge.step("charge")

  val checkout: Workflow[Receipt] = pipeline.asWorkflow("checkout")

  def main(args: Array[String]): Unit = {
    dbos.launch()
    val receipt = checkout.start("order-42").result
    println(s"Durable pipeline produced: $receipt")
    dbos.shutdown()
  }
}
