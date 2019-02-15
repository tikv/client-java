package org.tikv.txn.type;

/**
 * 2PC command types
 */
public enum TwoPhaseCommitType {
    actionPrewrite,
    actionCommit,
    actionCleanup
}
