package dbos4s

import java.time.{Duration => JDuration}
import scala.concurrent.duration._

class SyntaxSpec extends munit.FunSuite {

  test("FiniteDuration converts to java.time.Duration via syntax") {
    import dbos4s.syntax._
    val j: JDuration = 5.seconds
    assertEquals(j, JDuration.ofSeconds(5))
  }
}
