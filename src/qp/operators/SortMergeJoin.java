package qp.operators;

import java.util.Vector;

import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Tuple;

public class SortMergeJoin extends Join {
    
    static int filenum = 0;                         // To get unique filenum for this operation
    int batchsize;                                  // Number of tuples per out batch
    int leftindex;                                  // Indices of the join attributes in left table
    int rightindex;                                 // Indices of the join attributes in right table
    String rfname;                                  // The file name where the right table is materialized
    Batch outbatch;                                 // Buffer page for output
    Batch leftbatch;                                // Buffer page for left input stream
    Batch rightbatch;                               // Buffer page for right input stream
    Tuple lefttuple = null;                         // The tuple that is currently being processed from left input batch
    Tuple righttuple = null;                        // The tuple that is currently being processed from right input batch
    Vector<Tuple> rightpartition = new Vector<>();  // The right partition that is currently being joined in
    int rightpartitionindex = 0;                    // The index of the tuple that is currently being processed in the current right partition (0-based)
    Tuple nextrighttuple = null;                    // The next right tuple (i.e., the first element of the next right partition)
    int attrType;                                   // Type of the join attribute  
    int lcurs;                                      // Cursor for left side buffer
    int rcurs;                                      // Cursor for right side buffer
    boolean eosl;                                   // Whether end of stream (left table) is reached
    boolean eosr;                                   // Whether end of stream (right table) is reached

    /**
     * Instantiates a new join operator using block-based nested loop algorithm.
     *
     * @param jn is the base join operator.
     */
    public SortMergeJoin(Join jn) {
        super(jn.getLeft(), jn.getRight(), jn.getCondition(), jn.getOpType());
        schema = jn.getSchema();
        jointype = jn.getJoinType();
        numBuff = jn.getNumBuff();
    }

    /**
     * Opens this operator by performing the following operations:
     * 1. Sorts the left & right relation with external sort;
     * 2. Stores the sorted relations from both sides into files;
     * 3. Opens the connections.
     *
     * @return true if the operator is opened successfully.
     */
    @Override
    public boolean open() {
        // Sort in done at RandomOptimizer class.
        left.open();
        right.open();

        // Selects the number of tuples per page based tuple size.
        int tuplesize = schema.getTupleSize();
        batchsize = Batch.getPageSize() / tuplesize;

        // Gets the join attribute from left & right table.
        Attribute leftattr = getCondition().getLhs();
        Attribute rightattr = (Attribute) getCondition().getRhs();
        leftindex = left.getSchema().indexOf(leftattr);
        rightindex = right.getSchema().indexOf(rightattr);

        // Gets the type of the join attribute.
        attrType = left.getSchema().typeOf(leftattr);

        return super.open();
    }

    /**
     * Selects tuples satisfying the join condition from input buffers and returns.
     *
     * @return the next page of output tuples.
     */
    @Override
    public Batch next() {
        // Returns empty if either left or right table reaches end-of-stream.
        if (eosl || eosr) {
            close();
            return null;
        }

        // To handle the 1st run.
        if (leftbatch == null) {
            leftbatch = left.next();
            if (leftbatch == null) {
                eosl = true;
                return null;
            }
            lefttuple = readNextLeftTuple();
            if (lefttuple == null) {
                eosl = true;
                return null;
            }
        }
        if (rightbatch == null) {
            rightbatch = right.next();
            if (rightbatch == null) {
                eosr = true;
                return null;
            }
            rightpartition = createNextRightPartition();
            if (rightpartition.isEmpty()) {
                eosr = true;
                return null;
            }
            rightpartitionindex = 0;
            righttuple = rightpartition.elementAt(rightpartitionindex);
        }

        // The output buffer.
        Batch outbatch = new Batch(batchsize);

        while (!outbatch.isFull()) {
            int comparison = compareTuples(lefttuple, righttuple, leftindex, rightindex);
            if (comparison == 0) {
                outbatch.add(lefttuple.joinWith(righttuple));

                // Left tuple remains unchanged if it has not attempted to match with all tuples in the current right partition.
                if (rightpartitionindex < rightpartition.size() - 1) {
                    rightpartitionindex++;
                    righttuple = rightpartition.elementAt(rightpartitionindex);
                } else {
                    Tuple nextlefttuple = readNextLeftTuple();
                    if (nextlefttuple == null) {
                        eosl = true;
                        break;
                    }
                    comparison = compareTuples(lefttuple, nextlefttuple, leftindex, leftindex);
                    lefttuple = nextlefttuple;

                    // Moves back to the beginning of right partition if the next left tuple remains the same value as the current one.
                    if (comparison == 0) {
                        rightpartitionindex = 0;
                        righttuple = rightpartition.elementAt(0);
                    } else {
                        // Proceeds and creates a new right partition otherwise.
                        rightpartition = createNextRightPartition();
                        if (rightpartition.isEmpty()) {
                            eosr = true;
                            break;
                        }

                        // Updates the right tuple.
                        rightpartitionindex = 0;
                        righttuple = rightpartition.elementAt(rightpartitionindex);
                    }
                }
            } else if (comparison < 0) {
                lefttuple = readNextLeftTuple();
                if (lefttuple == null) {
                    eosl = true;
                    break;
                }
            } else {
                rightpartition = createNextRightPartition();
                if (rightpartition.isEmpty()) {
                    eosr = true;
                    break;
                }

                rightpartitionindex = 0;
                righttuple = rightpartition.elementAt(rightpartitionindex);
            }
        }

        return outbatch;
    }

    /**
     * Creates the next partition from the right input batch based on the current right cursor value.
     *
     * @return a vector containing all tuples in the next right partition.
     */
    private Vector<Tuple> createNextRightPartition() {
        Vector<Tuple> partition = new Vector<>();
        int comparison = 0;
        if (nextrighttuple == null) {
            nextrighttuple = readNextRightTuple();
            if (nextrighttuple == null) {
                return partition;
            }
        }

        // Continues until the next tuple carries a different value.
        while (comparison == 0) {
            partition.add(nextrighttuple);

            nextrighttuple = readNextRightTuple();
            if (nextrighttuple == null) {
                break;
            }
            comparison = compareTuples(partition.elementAt(0), nextrighttuple, rightindex, rightindex);
        }

        return partition;
    }

    /**
     * Reads the next tuple from left input batch.
     *
     * @return the next tuple if available; null otherwise.
     */
    private Tuple readNextLeftTuple() {
        // Reads in another batch if necessary.
        if (leftbatch == null) {
            eosl = true;
            return null;
        } else if (lcurs == leftbatch.size()) {
            leftbatch = left.next();
            lcurs = 0;
        }

        // Checks whether the left batch still has tuples left.
        if (leftbatch == null || leftbatch.size() <= lcurs) {
            eosl = true;
            return null;
        }

        // Reads in the next tuple from left batch.
        Tuple nextlefttuple = leftbatch.get(lcurs);
        lcurs++;
        return nextlefttuple;
    }

    /**
     * Reads the next tuple from right input batch.
     *
     * @return the next tuple if available; null otherwise.
     */
    private Tuple readNextRightTuple() {
        // Reads another batch if necessary.
        if (rightbatch == null) {
            return null;
        } else if (rcurs == rightbatch.size()) {
            rightbatch = right.next();
            rcurs = 0;
        }

        // Checks whether the right batch still has tuples left.
        if (rightbatch == null || rightbatch.size() <= rcurs) {
            return null;
        }

        // Reads the next tuple.
        Tuple next = rightbatch.get(rcurs);
        rcurs++;
        return next;
    }

    /**
     * Compares two tuples based on the join attribute.
     *
     * @param tuple1 is the first tuple.
     * @param tuple2 is the second tuple.
     * @return an integer indicating the comparision result, compatible with the {@link java.util.Comparator} interface.
     */
    private int compareTuples(Tuple tuple1, Tuple tuple2, int index1, int index2) {
        Object value1 = tuple1.dataAt(index1);
        Object value2 = tuple2.dataAt(index2);

        switch (attrType) {
            case Attribute.INT:
                return Integer.compare((int) value1, (int) value2);
            case Attribute.STRING:
                return ((String) value1).compareTo((String) value2);
            case Attribute.REAL:
                return Float.compare((float) value1, (float) value2);
            default:
                return 0;
        }
    }

    /**
     * Closes this operator.
     *
     * @return true if the operator is closed successfully.
     */
    @Override
    public boolean close() {
        left.close();
        right.close();
        return super.close();
    }
}