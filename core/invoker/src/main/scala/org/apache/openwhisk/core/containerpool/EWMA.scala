package org.apache.openwhisk.core.containerpool

import java.time.Instant
import scala.collection.immutable.Vector
import scala.math
import scala.annotation.tailrec


class EWMA(
    smoothing: Double,
    tdefault: Int,
    historyLimit: Int
) {
    var lastArrivalTime: Instant = Instant.EPOCH
    var history = Vector.empty[Double]

    def update(arrivalTime: Instant): Vector[Double] = {
        if (lastArrivalTime != Instant.EPOCH) {
            val iat: Double = (arrivalTime.toEpochMilli() - lastArrivalTime.toEpochMilli()).toDouble / 1000 // To second
            if (history.length == historyLimit) {
                history = history.drop(1)
            }
            history = history :+ iat
        }
        lastArrivalTime = arrivalTime
        history
    }

    // Reference: https://www.cnblogs.com/jiangxinyang/p/9705198.html
    @tailrec
    final def compute(index: Int, prev: Double): Double = {
        val cur = (smoothing * prev + (1 - smoothing) * history(index))
        val newIndex = index + 1
        if (newIndex == history.length) {
            cur / (1 - math.pow(smoothing, newIndex)) // Bias correction at the end
        } else {
            compute(newIndex, cur)
        }
    }

    def predict(): Int = if (history.isEmpty) tdefault else compute(index=0, prev=0).ceil.toInt
}
