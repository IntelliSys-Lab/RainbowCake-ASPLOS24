package org.apache.openwhisk.core.containerpool

import java.time.Instant
import org.apache.commons.math3.random._


class Histogram(
    historyTimeout: Int,
    historyLength: Int
) {
    var lastArrivalTime: Long = Instant.EPOCH.toEpochMilli
    var history = Array.empty[Double]
    val cvThreshold: Double = 2.0

    def update(arrivalTime: Long): Unit = {
        if (lastArrivalTime != Instant.EPOCH.toEpochMilli) {
            val iat: Double = ((arrivalTime - lastArrivalTime).toDouble / 1000 / 60).ceil // minute
            if (history.length == historyLength) {
                history = history.drop(1)
            }
            history = history :+ iat
        }
        lastArrivalTime = arrivalTime
    }

    def predictPrewarm(): Option[Int] = {
        if (history.isEmpty) {
            None
        } else {
            val dist = new EmpiricalDistribution(history.length)
            dist.load(history)
            val mean: Double = 	dist.getNumericalMean()
            val cv: Double = mean / math.sqrt(dist.getNumericalVariance())
            if (mean == 0.0 || cv < cvThreshold) {
                None
            } else {
                Some(
                    dist.inverseCumulativeProbability(0.05).ceil.toInt * 60 // second
                )
            }
        }
    }

    def predictAlive(default: Int): Int = {
        if (history.isEmpty) {
            default
        } else {
            val dist = new EmpiricalDistribution(history.length)
            dist.load(history)
            val mean: Double = 	dist.getNumericalMean()
            val cv: Double = mean / math.sqrt(dist.getNumericalVariance())
            if (mean == 0.0 || cv < cvThreshold) {
                default
            } else {
                (dist.inverseCumulativeProbability(0.99).ceil.toInt - dist.inverseCumulativeProbability(0.05).ceil.toInt) * 60 // second
            }
        }
    }

    def predictAlivePagurus(default: Int): Int = {
        if (history.length < 30) { // T_default
            default
        } else {
            val dist = new EmpiricalDistribution(history.length)
            dist.load(history)
            dist.inverseCumulativeProbability(0.95).ceil.toInt * 60 // T_0.95m, second
        }
    }
}
