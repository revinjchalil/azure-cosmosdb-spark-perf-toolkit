package ms.common

import scala.collection.immutable.ListMap

/**
 * Case for config.
 *
 * @param spark
 * @param sql
 * @param hadoop
 */
case class SparkConf(spark: ListMap[Object, String],
                     sql: ListMap[Object, String],
                     hadoop: ListMap[Object, String])
