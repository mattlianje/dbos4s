package dbos4s.etl4s

import scala.annotation.{experimental, MacroAnnotation}
import scala.quoted.*
import _root_.etl4s.*
import dbos4s.*

@experimental
class step(name: String) extends MacroAnnotation {

  def transform(using
    Quotes
  )(
    definition: quotes.reflect.Definition,
    companion: Option[quotes.reflect.Definition]
  ): List[quotes.reflect.Definition] = {
    import quotes.reflect.*

    definition match {
      case vd @ ValDef(name, tpt, Some(rhs)) =>
        val dbos         = summonDbos(vd.pos)
        val stepName     = annotationName(vd.symbol, vd.pos)
        val lifted: Term =
          tpt.tpe.asType match {
            case '[Reader[r, Node[a, b]]] =>
              '{
                ${ rhs.asExprOf[Reader[r, Node[a, b]]] }.step($stepName)(using $dbos)
              }.asTerm
            case '[Node[a, b]] =>
              '{ ${ rhs.asExprOf[Node[a, b]] }.step($stepName)(using $dbos) }.asTerm
            case '[Function1[a, b]] =>
              '{
                val fn = ${ rhs.asExprOf[a => b] }
                (x: a) => $dbos.step($stepName)(fn(x))
              }.asTerm.changeOwner(vd.symbol)
            case _ =>
              report.errorAndAbort(
                "@step can only annotate a Node[_, _], Reader[_, Node[_, _]], or Function1[_, _] val",
                vd.pos
              )
          }
        List(ValDef.copy(vd)(name, tpt, Some(lifted)))

      case other =>
        report.errorAndAbort("@step can only annotate a val", other.pos)
    }
  }

  private def annotationName(using
    Quotes
  )(sym: quotes.reflect.Symbol, pos: quotes.reflect.Position): Expr[String] = {
    import quotes.reflect.*
    sym.annotations
      .collectFirst {
        case Apply(Select(New(tpt), _), List(arg)) if tpt.tpe =:= TypeRepr.of[step] =>
          arg.asExprOf[String]
      }
      .getOrElse {
        report.errorAndAbort("@step requires a step name, e.g. @step(\"fetch\")", pos)
      }
  }

  private def summonDbos(using Quotes)(pos: quotes.reflect.Position): Expr[Dbos] = {
    import quotes.reflect.*
    Implicits.search(TypeRepr.of[Dbos]) match {
      case ok: ImplicitSearchSuccess => ok.tree.asExprOf[Dbos]
      case _                         =>
        report.errorAndAbort("@step needs a `given Dbos` in scope", pos)
    }
  }
}
