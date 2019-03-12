package org.tikv.common.memdb.iterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.key.Key;

import java.util.Iterator;
import java.util.Map;

public class UnionStoreIterator implements Iterator {
    private final static Logger LOG = LoggerFactory.getLogger(UnionStoreIterator.class);

    private Iterator dirtyIt;

    private Iterator snapshotIt;

    private boolean dirtyValid = true;

    private boolean snapshotValid = true;

    private boolean currentIsDirty;

    private boolean isValid = true;

    private boolean reverse = false;

    private Object dirtyCurObject;

    private Object snapshotCurObject;

    public UnionStoreIterator(Iterator bufferIt, Iterator snapshotIt) {
        this(bufferIt, snapshotIt, false);
    }
    public UnionStoreIterator(Iterator bufferIt, Iterator snapshotIt, boolean isReverse) {
        this.dirtyIt = bufferIt;
        this.snapshotIt = snapshotIt;
        if(snapshotIt == null) {
            snapshotValid = false;
        }
        if(bufferIt == null) {
            dirtyValid = false;
        }
        if(!dirtyValid && !snapshotValid) {
            isValid = false;
        }
        this.reverse = isReverse;
    }

    @Override
    public boolean hasNext() {
        if(!dirtyValid && !snapshotValid) {
            return false;
        }
        if(dirtyIt.hasNext()) {
            dirtyValid = true;
        }
        if(snapshotIt.hasNext()) {
            snapshotValid = true;
        }
        return isValid = (dirtyValid || snapshotValid);
    }

    @Override
    public Object next() {
        Object result = null;
        if(!currentIsDirty) {
            result = this.snapshotNext();
        } else {
            result = this.dirtyNext();
        }
        this.updateCurrent();
        return result;
    }

    private Object snapshotNext() {
        if(!snapshotValid) {
            return null;
        }
        if(snapshotIt.hasNext()) {
            snapshotCurObject = snapshotIt.next();
        } else {
            snapshotValid = false;
            snapshotCurObject = null;
        }
        return snapshotCurObject;
    }

    private Object dirtyNext() {
        if(!dirtyValid) {
            return null;
        }
        if(dirtyIt.hasNext()) {
            dirtyCurObject = dirtyIt.hasNext();
        } else {
            dirtyValid = false;
            dirtyCurObject = null;
        }
        return dirtyCurObject;
    }

    private void updateCurrent() {
        this.isValid = true;
        for(;;) {
            if(!dirtyValid && !snapshotValid) {
                this.isValid = false;
            }
            if(!dirtyValid) {
                this.currentIsDirty = false;
                break;
            }
            if(!snapshotValid) {
                this.currentIsDirty = true;
                //if delete it
                Map.Entry<Key, Key> entry = (Map.Entry<Key, Key>) dirtyCurObject;
                if(length(entry.getValue()) == 0) {
                    this.dirtyNext();
                    continue;
                }
                break;
            }
            //both valid
            if(snapshotValid && dirtyValid) {
                Map.Entry<Key, Key> entryDirty = (Map.Entry<Key, Key>) dirtyCurObject;
                Map.Entry<Key, Key> entrySnapshot = (Map.Entry<Key, Key>) snapshotCurObject;
                Key dirtyKey = entryDirty.getKey();
                Key snapshotKey = entrySnapshot.getKey();
                Key dirtyValue = entryDirty.getValue();
                int cmp = dirtyKey.compareTo(snapshotKey);
                // if equal, means both have value
                if(cmp == 0) {
                    if(length(dirtyValue) == 0) {
                        //snapshot have a record, but txn says we have deleted it
                        //just go next
                        this.dirtyNext();
                        this.snapshotNext();
                        continue;
                    }
                    this.snapshotNext();
                    this.currentIsDirty = true;
                    break;
                } else if(cmp > 0) {
                    // record from snapshot comes first
                    this.currentIsDirty = false;
                    break;
                } else {
                    // record from dirty comes first
                    if(length(dirtyValue) == 0) {
                        LOG.warn("kv delete a record not exist, key={}", snapshotKey.toString());
                        // jump over this deletion
                        this.dirtyNext();
                        continue;
                    }
                    this.currentIsDirty = true;
                    break;
                }
            }
        }
    }

    private int length(Key object) {
        return object == null ? object.getBytes().length : 0;
    }
}
