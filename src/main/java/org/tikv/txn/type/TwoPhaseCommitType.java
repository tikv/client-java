package org.tikv.txn.type;

public enum TwoPhaseCommitType {
    actionPrewrite,
    actionCommit,
    actionCleanup
}
