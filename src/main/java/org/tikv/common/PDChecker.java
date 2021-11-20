package org.tikv.common;

public enum PDChecker {
  Learner,
  Replica,
  Rule,
  Split,
  Merge,
  JointState,
  Priority;

  public String apiName() {
    switch (this) {
      case Learner:
        return "learner";
      case Replica:
        return "replica";
      case Rule:
        return "rule";
      case Split:
        return "split";
      case Merge:
        return "merge";
      case JointState:
        return "joint-state";
      case Priority:
        return "priority";
    }
    throw new IllegalArgumentException();
  }
}
