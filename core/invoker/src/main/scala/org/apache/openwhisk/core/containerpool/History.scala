package org.apache.openwhisk.core.containerpool

import scala.collection.immutable
import org.apache.openwhisk.core.entity._


class History(
    action: ExecutableWhiskAction
) {
    var userTimeHistory = immutable.Vector.empty[Double]
    var userMemoryHistory = immutable.Vector.empty[Double]
    var langTimeHistory = immutable.Vector.empty[Double]
    var langMemoryHistory = immutable.Vector.empty[Double]
    var bareTimeHistory = immutable.Vector.empty[Double]
    var bareMemoryHistory = immutable.Vector.empty[Double]

    def updateTime(userTime: Double, langTime: Double, bareTime: Double): Unit = {
        userTimeHistory = userTimeHistory :+ userTime
        langTimeHistory = langTimeHistory :+ langTime
        bareTimeHistory = bareTimeHistory :+ bareTime
    }

    def updateMemory(memory: Double, level: String): Unit = {
        level match {
            case "user" => userMemoryHistory = userMemoryHistory :+ memory
            case "lang" => langMemoryHistory = langMemoryHistory :+ memory
            case "bare" => bareMemoryHistory = bareMemoryHistory :+ (memory*1024*1024)
        }
    }

    def upperBound(alpha: Double, level: String, default: Int): Int = {
        level match {
            case "user" => {
                if (userTimeHistory.isEmpty || userMemoryHistory.isEmpty) {
                    default
                } else {
                    val avgTime: Double = (userTimeHistory.sum / userTimeHistory.length) / 1000 // second
                    val avgMemory: Double = (userMemoryHistory.sum / userMemoryHistory.length) / 1024 / 1024 // megabyte
                    (alpha*avgTime / ((1-alpha)*avgMemory)).ceil.toInt
                }
            }
            case "lang" => {
                if (langTimeHistory.isEmpty || langMemoryHistory.isEmpty) {
                    default
                } else {
                    val avgTime: Double = (langTimeHistory.sum / langTimeHistory.length) / 1000 // second
                    val avgMemory: Double = (langMemoryHistory.sum / langMemoryHistory.length) / 1024 / 1024 // megabyte
                    (alpha*avgTime / ((1-alpha)*avgMemory)).ceil.toInt
                }
            }
            case "bare" => {
                if (bareTimeHistory.isEmpty || bareMemoryHistory.isEmpty) {
                    default
                } else {
                    val avgTime: Double = (bareTimeHistory.sum / bareTimeHistory.length) / 1000 // second
                    val avgMemory: Double = (bareMemoryHistory.sum / bareMemoryHistory.length) / 1024 / 1024 // megabyte
                    (alpha*avgTime / ((1-alpha)*avgMemory)).ceil.toInt
                }
            }
        }
    }
}
