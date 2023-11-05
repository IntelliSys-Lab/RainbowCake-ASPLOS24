package org.apache.openwhisk.core.containerpool

import java.time.Instant


//
// A tracker for each activation
//

sealed abstract class Level

case object UserLevel extends Level
case object LangLevel extends Level
case object BareLevel extends Level
case object ColdLevel extends Level

class ActivationTracker(job: Run, hitLevel: Level) {
  var userInterval = Interval.zeroWithEpoch
  var langInterval = Interval.zeroWithEpoch
  var bareInterval = Interval.zeroWithEpoch

  def userTime: Long = hitLevel match {
    case UserLevel => Interval.zeroWithEpoch.duration.length
    case _ => userInterval.duration.length
  }

  def langTime: Long = hitLevel match {
    case UserLevel => Interval.zeroWithEpoch.duration.length
    case LangLevel => Interval.zeroWithEpoch.duration.length
    // case _ => Interval(langInterval.start, userInterval.start).duration.length
    case _ => langInterval.duration.length
  }

  def bareTime: Long = hitLevel match {
    case ColdLevel => Interval(job.msg.transid.meta.start, langInterval.start).duration.length
    case _ => Interval.zeroWithEpoch.duration.length
  }

  def levelToString: String = hitLevel match {
    case UserLevel => "user"
    case LangLevel => "lang"
    case BareLevel => "bare"
    case ColdLevel => "cold"
  }
}

//
// Record wasted time of memory allocated to containers
//

class WastedMemoryTimeRecorder() {
  var userWastedMemoryTime: Double = 0.0
  var langWastedMemoryTime: Double = 0.0
  var bareWastedMemoryTime: Double = 0.0
  var start: Instant = Instant.EPOCH
  var end: Instant = Instant.EPOCH

  var userTimeline: String = ""
  var langTimeline: String = ""
  var bareTimeline: String = ""

  def setStart(t: Instant) = {
    start = t
  }

  def setEnd(t: Instant) = {
    end = t
  }
  
  def summary(memory: Double, level: String, status: String) = {
    if (start != Instant.EPOCH && end != Instant.EPOCH) {
      val interval = Interval(start, end)
      level match {
        case "user" => {
          val wastedMemory: Double  = memory / 1024 / 1024
          userWastedMemoryTime = userWastedMemoryTime + wastedMemory * (interval.duration.length.toDouble / 1000) // megabyte x second
          if (userTimeline == "") {
            userTimeline = s"${wastedMemory},${start.toEpochMilli},${end.toEpochMilli},${status}"
          } else {
            userTimeline = userTimeline + s" ${wastedMemory},${start.toEpochMilli},${end.toEpochMilli},${status}"
          }
        }
        case "lang" => {
          val wastedMemory: Double = memory / 1024 / 1024
          langWastedMemoryTime = langWastedMemoryTime + wastedMemory * (interval.duration.length.toDouble / 1000) // megabyte x second
          if (langTimeline == "") {
            langTimeline = s"${wastedMemory},${start.toEpochMilli},${end.toEpochMilli},${status}"
          } else {
            langTimeline = langTimeline + s" ${wastedMemory},${start.toEpochMilli},${end.toEpochMilli},${status}"
          }
        }
        case "bare" => {
          val wastedMemory: Double = memory
          bareWastedMemoryTime = bareWastedMemoryTime + wastedMemory * (interval.duration.length.toDouble / 1000) // megabyte x second
          if (bareTimeline == "") {
            bareTimeline = s"${wastedMemory},${start.toEpochMilli},${end.toEpochMilli},${status}"
          } else {
            bareTimeline = bareTimeline + s" ${wastedMemory},${start.toEpochMilli},${end.toEpochMilli},${status}"
          }
        }
      }
    }
    start = Instant.EPOCH
    end = Instant.EPOCH
  }

  def timeline: Option[String] = Some(s"${userTimeline};${langTimeline};${bareTimeline}")

  def total: Double = {userWastedMemoryTime + langWastedMemoryTime + bareWastedMemoryTime}
}
