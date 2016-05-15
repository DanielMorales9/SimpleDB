package simpledb.buffer;

import simpledb.file.FileMgr;
import simpledb.log.LogMgr;
import simpledb.server.SimpleDB;

/**
 * Created by Daniel on 11/05/16.
 */
class ClockBufferMgr extends AbstractBufferMgr {

    private int lastReplacedPageIndex;

    public ClockBufferMgr() {
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
    public ClockBufferMgr(int numbuffs) {
        super(numbuffs);
        this.lastReplacedPageIndex= -1;
    }


    Buffer chooseUnpinnedBuffer() {
        //System.out.println("Clock Strategy " + lastIndexBuffer);
        Buffer buff[] = this.getBufferPool();
        for (int i = lastReplacedPageIndex+1; i < buff.length; i++)
            if (!buff[i].isPinned()) {
                lastReplacedPageIndex= i;
                return buff[i];
            }
        for (int i = 0; i <= lastReplacedPageIndex; i++)
            if (!buff[i].isPinned()) {
                lastReplacedPageIndex = i;
                return buff[i];
            }
        return null;
    }

}
