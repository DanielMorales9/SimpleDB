package simpledb.buffer;

/**
 * Created by Daniel on 12/05/16.
 */
public class NaiveBufferMgr extends AbstractBufferMgr {

    public NaiveBufferMgr(){}

    public NaiveBufferMgr(int numbuffs) {
        super(numbuffs);
    }

    @Override
    Buffer chooseUnpinnedBuffer() {
        for (Buffer buff : getBufferPool())
            if (!buff.isPinned())
                return buff;
        return null;
    }
}
