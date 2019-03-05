

package org.tikv.common.memdb;

import org.tikv.common.key.Key;

import java.util.Iterator;

/**
 * lazyMemBuffer wraps a MemBuffer which is to be initialized when it is modified.
 */
public class LazyMemBuffer implements IMemBuffer{
    private IMemBuffer mb;
    private int cap;

    public LazyMemBuffer(int cap) {
        this.cap = cap;
    }
    @Override
    public int size() {
        if(mb == null) {
            return 0;
        }
        return this.mb.size();
    }

    @Override
    public int length() {
        if(mb == null) {
            return 0;
        }
        return this.mb.length();
    }

    @Override
    public void reset() {
        if(this.mb != null) {
            this.mb.reset();
        }
    }

    @Override
    public void setcap(int cap) {
        this.cap = cap;
    }

    @Override
    public boolean set(Key key, byte[] value) {
        if(this.mb == null) {
            this.mb = new MemDbBuffer(this.cap);
        }
        return this.mb.set(key, value);
    }

    @Override
    public boolean delete(Key key) {
        if(this.mb == null) {
            this.mb = new MemDbBuffer(this.cap);
        }
        return this.mb.delete(key);
    }

    @Override
    public byte[] get(Key key) {
        if(this.mb == null) {
            return null;
        }
        return this.mb.get(key);
    }

    @Override
    public Iterator iterator(Key key, Key upperBound) {
        if(this.mb == null) {
            //invalid iterator
            return null;
        }
        return this.mb.iterator(key, upperBound);
    }

    @Override
    public Iterator iteratorReverse(Key key) {
        if(this.mb == null) {
            //invalid iterator
            return null;
        }
        return this.mb.iteratorReverse(key);
    }
}
