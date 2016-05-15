package simpledb.stats;

/**
 * Created by Daniel on 12/05/16.
 */
public class BasicFileStats  {

    private int numBlockWritten;
    private int numBlockRead;

    public BasicFileStats(){
        numBlockRead=0;
        numBlockWritten=0;
    }


    public void updateRead() { this.numBlockRead++;}

    public void updateWrite() { this.numBlockWritten++;  }

    public int getNumBlockRead() {
        return numBlockRead;
    }

    public int getNumBlockWritten() {
        return numBlockWritten;
    }

    @Override
    public String toString() {
        return "Blocks Read: "+ this.numBlockRead+ " Blocks Written: "+this.numBlockWritten;
    }
}
