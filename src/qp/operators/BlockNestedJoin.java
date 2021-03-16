package qp.operators;

import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Tuple;

/**
 * Implements the block-based nested loop join algorithm.
 */
public class BlockNestedJoin extends Join {

    static int filenum = 0;         // To get unique filenum for this operation
    int batchsize;                  // Number of tuples per out batch
    int leftindex;                  // Indices of the join attributes in left table
    int rightindex;                 // Indices of the join attributes in right table
    String rfname;                  // The file name where the right table is materialized
    Batch outbatch;                 // Buffer page for output
    Batch[] leftbatches;            // Buffer pages for left input stream
    Batch rightbatch;               // Buffer page for right input stream
    ObjectInputStream in;           // File pointer to the right hand materialized file

    int lcurs;                      // Cursor for left side buffer
    int rcurs;                      // Cursor for right side buffer
    boolean eosl;                   // Whether end of stream (left table) is reached
    boolean eosr;                   // Whether end of stream (right table) is reached

    /**
     * Instantiates a new join operator using block-based nested loop algorithm.
     *
     * @param jn is the base join operator.
     */
    public BlockNestedJoin(Join jn) {
        super(jn.getLeft(), jn.getRight(), jn.getCondition(), jn.getOpType());
        schema = jn.getSchema();
        jointype = jn.getJoinType();
        numBuff = jn.getNumBuff();
    }

    /**
     * During open finds the index of the join attributes
     * * Materializes the right hand side into a file
     * * Opens the connections
     **/
    @Override
    public boolean open() {
        // Selects the number of tuples per page based tuple size.
        int tupleSize = schema.getTupleSize();
        batchsize = Batch.getPageSize() / tupleSize;

        // Gets the join attribute from left & right table.
        Attribute leftAttr = getCondition().getLhs();
        Attribute rightAttr = (Attribute) getCondition().getRhs();
        leftindex = left.getSchema().indexOf(leftAttr);
        rightindex = right.getSchema().indexOf(rightAttr);
        Batch rightPage;

        // Initializes the cursors of input buffers for both sides.
        lcurs = 0;
        rcurs = 0;
        eosl = false;
        // Right stream would be repetitively scanned. If it reaches the end, we have to start new scan.
        eosr = true;

        // Materializes the right table for the algorithm to perform.
        if (!right.open()) {
            return false;
        } else {
            /*
             * If the right operator is not a base table, then materializes the intermediate result
             * from right into a file.
             */
            // if(right.getCondType() != OpType.SCAN){
            filenum++;
            rfname = "BNJtemp-" + filenum;
            try {
                ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(rfname));
                rightPage = right.next();
                while (rightPage != null) {
                    out.writeObject(rightPage);
                    rightPage = right.next();
                }
                out.close();
            } catch (IOException io) {
                System.out.println("BlockNestedJoin: writing the temporary file error");
                return false;
            }
            // }

            if (!right.close()) {
                return false;
            }

        }
        return left.open();
    }

    /**
     * Selects tuples satisfying the join condition from input buffers and returns.
     *
     * @return the next page of output tuples.
     */
    @Override
    public Batch next() {
        // Returns empty if the left table reaches end-of-stream.
        if (eosl) {
            close();
            return null;
        }

        outbatch = new Batch(batchsize);
        while (!outbatch.isFull()) {
            // Checks whether we need to read a new block of pages from the left table.
            if (lcurs == 0 && eosr) {
                leftbatches = new Batch[numBuff - 2];
                leftbatches[0] = left.next();
                // Checks if there is no more pages from the left table.
                if (leftbatches[0] == null) {
                    eosl = true;
                    return outbatch;
                }
                for (int i = 1; i < leftbatches.length; i++) {
                    leftbatches[i] = left.next();
                    if (leftbatches[i] == null) {
                        break;
                    }
                }

                // Starts the scanning of right table whenever a new block of left pages comes.
                try {
                    in = new ObjectInputStream(new FileInputStream(rfname));
                    eosr = false;
                } catch (IOException io) {
                    System.err.println("BlockNestedJoin:error in reading the file");
                    System.exit(1);
                }
            }

            int numlefttuple = leftbatches[0].size();
            for (int i = 1; i < leftbatches.length; i++) {
                if (leftbatches[i] == null) {
                    break;
                }
                numlefttuple += leftbatches[i].size();
            }

            // Continuously probe the right table until we hit the end-of-stream.
            while (!eosr) {
                try {
                    if (lcurs == 0 && rcurs == 0) {
                        rightbatch = (Batch) in.readObject();
                    }

                    for (int i = lcurs; i < numlefttuple; i++) {
                        int leftBatchIndex = i / leftbatches[0].size();
                        int leftTupleIndex = i % leftbatches[0].size();
                        Tuple leftTuple = leftbatches[leftBatchIndex].get(leftTupleIndex);

                        for (int j = rcurs; j < rightbatch.size(); j++) {
                            Tuple righttuple = rightbatch.get(j);

                            // Adds the tuple if satisfying the join condition.
                            if (leftTuple.checkJoin(righttuple, leftindex, rightindex)) {
                                Tuple outtuple = leftTuple.joinWith(righttuple);
                                outbatch.add(outtuple);

                                // Checks whether the output buffer is full.
                                if (outbatch.isFull()) {
                                    if (i == numlefttuple - 1 && j == rightbatch.size() - 1) {
                                        lcurs = 0;
                                        rcurs = 0;
                                    } else if (i != numlefttuple - 1 && j == rightbatch.size() - 1) {
                                        lcurs = i + 1;
                                        rcurs = 0;
                                    } else {
                                        lcurs = i;
                                        rcurs = j + 1;
                                    }

                                    // Returns since we have already produced a complete page of matching tuples.
                                    return outbatch;
                                }
                            }
                        }
                        rcurs = 0;
                    }
                    lcurs = 0;
                } catch (EOFException e) {
                    try {
                        in.close();
                    } catch (IOException io) {
                        System.out.println("BlockNestedJoin: error in temporary file reading");
                    }
                    eosr = true;
                } catch (ClassNotFoundException c) {
                    System.out.println("BlockNestedJoin: some error in deserialization");
                    System.exit(1);
                } catch (IOException io) {
                    System.out.println("BlockNestedJoin: temporary file reading error");
                    System.exit(1);
                }
            }
        }
        return outbatch;
    }

    /**
     * Closes this operator by deleting the file generated.
     *
     * @return true if the operator is closed successfully.
     */
    @Override
    public boolean close() {
        File f = new File(rfname);
        f.delete();
        return true;
    }
}