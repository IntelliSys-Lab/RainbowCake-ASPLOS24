package org.apache.openwhisk.core.containerpool

import java.time.Instant
import scala.collection.immutable.Vector
import org.apache.commons.math3.distribution._


class Distribution(
    historyTimeout: Int,
    historyLength: Int
) {
    var history = Vector.empty[Long]

    def update(arrivalTime: Long): Vector[Long] = {
        history = history.filter(Instant.now.toEpochMilli() - _ < (historyTimeout * 1000).toLong)
        if (history.length == historyLength) {
            history = history.drop(1)
        }

        history = history :+ arrivalTime
        history
    }

    def predictPrewarm(quantile: Double, min: Int, max: Int): Int = {
        if (history.isEmpty) {
            min
        } else {
            val mean: Double = ((Instant.now.toEpochMilli().toDouble - history.head.toDouble) / 1000) / history.length
            if (mean > 0) {
                val exponential = new ExponentialDistribution(mean)
                // val prewarm: Int = exponential.inverseCumulativeProbability(quantile).ceil.toInt
                // if (prewarm < min) min else if (prewarm > max) max else prewarm
                exponential.inverseCumulativeProbability(quantile).ceil.toInt
            } else {
                min
            }
        }
    }

    def predictAlive(quantile: Double, min: Int, max: Int): Int = {
        if (history.isEmpty) {
            max
        } else {
            val mean: Double = ((Instant.now.toEpochMilli().toDouble - history.head.toDouble) / 1000) / history.length
            if (mean > 0) {
                val exponential = new ExponentialDistribution(mean)
                val alive: Int = exponential.inverseCumulativeProbability(quantile).ceil.toInt
                if (alive < min) min else if (alive > max) max else alive
            } else {
                max
            }
        }
    }
}
