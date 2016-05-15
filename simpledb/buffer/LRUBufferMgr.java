package simpledb.buffer;

import simpledb.file.FileMgr;
import simpledb.log.LogMgr;
import simpledb.server.SimpleDB;

/**
 * Created by Daniel on 11/05/16.
 */
class LRUBufferMgr extends AbstractBufferMgr {

    public LRUBufferMgr() {

    }
    /**
     * Creates a buffer manager having the specified number
     * of buffer slots.
     * This constructor depends on both the {@link FileMgr} and
     * {@link LogMgr LogMgr} objects
     * that it gets from the class
     * {@link SimpleDB}.
     * Those objects are created during system initialization.
     * Thus this constructor cannot be called until
     * {@link SimpleDB#initFileAndLogMgr(String)} or
     * is called first.
     *
     * @param numbuffs the number of buffer slots to allocate
     */
    public LRUBufferMgr(int numbuffs) {
        super(numbuffs);
    }

    Buffer chooseUnpinnedBuffer() {
        Buffer[] buff = getBufferPool();
        Buffer last = null;
        long lastUnpin = Long.MAX_VALUE;
        for (int i = 0; i < buff.length; i++) {
            if (!buff[i].isPinned() && lastUnpin > buff[i].getLastUnpinTimestamp()) {
                last = buff[i];
                lastUnpin = last.getLastUnpinTimestamp();
            }
        }
        return last;
    }

}
