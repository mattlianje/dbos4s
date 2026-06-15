package dbos4s

import java.time.{Duration => JDuration}
import scala.concurrent.duration.FiniteDuration

/**
 * Opt-in conveniences for when you drop down to the raw `dbos.underlying` and
 * a Java method wants a `java.time.Duration`.
 *
 * `import dbos4s.syntax.given` then write `dbos.underlying.sleep(5.seconds)`
 * using the usual `scala.concurrent.duration` helpers.
 */
object syntax {
  given finiteDurationToJava: Conversion[FiniteDuration, JDuration] = Jdk.toJava(_)
}
