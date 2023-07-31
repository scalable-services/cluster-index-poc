package cluster

import org.scalatest.{Outcome, Succeeded}
import org.scalatest.flatspec.AnyFlatSpec

trait Repeatable extends AnyFlatSpec {

  val times: Int

  override def withFixture(test: NoArgTest) = {
    withFixture(test, 0)
  }

  def withFixture(test: NoArgTest, count: Int): Outcome = {
    val outcome = super.withFixture(test)
    outcome match {
      case Succeeded => if (count + 1 == times) outcome else withFixture(test, count + 1)
      case other => other
    }
  }

}
