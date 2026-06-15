package dbos4s

private[dbos4s] object DbosContext {

  final case class Ctx(workflowId: String, stepId: Option[Int])

  private val tl: ThreadLocal[Option[Ctx]] =
    ThreadLocal.withInitial(() => Option.empty[Ctx])

  def current: Option[Ctx] = tl.get

  def withWorkflow[A](id: String)(body: => A): A =
    swap(Some(Ctx(id, None)))(body)

  def withStep[A](sid: Int)(body: => A): A =
    swap(current.map(_.copy(stepId = Some(sid))))(body)

  private def swap[A](next: Option[Ctx])(body: => A): A = {
    val prev = tl.get
    tl.set(next)
    try body
    finally tl.set(prev)
  }
}
