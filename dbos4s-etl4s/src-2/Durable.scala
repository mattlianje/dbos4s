package dbos4s.etl4s

import scala.annotation.StaticAnnotation
import scala.language.experimental.macros
import scala.reflect.macros.whitebox

/**
 * Make a `val` a durable DBOS step named `name`
 * counterpart to `.step("name")`. Works on a plain `Node[A, B]` or a
 * context-dependent `Reader[R, Node[A, B]]`, and needs an `implicit Dbos` in
 * scope. The body is rewritten to `<rhs>.step(name)`.
 *
 * NOTE: Macro-paradise or -Ymacro-annotations required
 *
 * {{{
 * implicit val dbos: Dbos = ...
 *
 * @step("fetch")    val fetch    = Extract { Order(id, 4200) }
 * @step("validate") val validate = Transform[Order, Order](identity)
 *
 * val pipeline = fetch ~> validate
 * val checkout = pipeline.asWorkflow("checkout")
 * }}}
 */
class step(name: String) extends StaticAnnotation {
  def macroTransform(annottees: Any*): Any = macro stepMacro.impl
}

private[etl4s] object stepMacro {

  def impl(c: whitebox.Context)(annottees: c.Expr[Any]*): c.Expr[Any] = {
    import c.universe._

    val name: Tree = c.prefix.tree match {
      case q"new step($n)" => n
      case _               =>
        c.abort(c.enclosingPosition, "@step requires a step name, e.g. @step(\"fetch\")")
    }

    val rewritten: Tree = annottees.map(_.tree).toList match {
      case q"$mods val $tname: $tpt = $rhs" :: Nil =>
        q"$mods val $tname: $tpt = $rhs.step($name)"
      case _ =>
        c.abort(c.enclosingPosition, "@step can only annotate a val")
    }

    c.Expr[Any](rewritten)
  }
}
