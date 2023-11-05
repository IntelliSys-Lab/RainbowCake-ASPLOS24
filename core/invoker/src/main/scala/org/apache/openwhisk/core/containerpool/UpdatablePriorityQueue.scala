package org.apache.openwhisk.core.containerpool

class TrackedAction() {
    var lastcalled : Double = 0.0;
    var invocations : Long = 0;
    var coldTime : Long = 0;
    var warmTime : Long = 0;
    var memory : Double = 0.0;
    var active : Long = 0;

    def priority() : Double = {
        lastcalled + ((invocations * (coldTime - warmTime))/ memory)
    }

}

object ActionOrdering extends Ordering[TrackedAction] {
  def compare(a:TrackedAction, b:TrackedAction) = a.priority() compare b.priority()
}
