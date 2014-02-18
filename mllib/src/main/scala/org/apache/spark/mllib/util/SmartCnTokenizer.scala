package org.apache.spark.mllib.util

/**
 * Created with IntelliJ IDEA.
 * User: sen
 * Date: 11/14/13
 * Time: 11:55 AM
 * To change this template use File | Settings | File Templates.
 */
import scala.collection.JavaConversions._

class SCTokenizer {
  def apply(s: String): Iterable[String] = SmartCnTokenizer.parseString(s)
}

object SCTokenizer extends SCTokenizer

