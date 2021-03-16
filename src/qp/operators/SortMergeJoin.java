package qp.operators;

import java.util.HashMap;

import qp.utils.Batch;
import qp.utils.Tuple;
import qp.operators.nestedjoin.Parameters;
import qp.operators.nestedjoin.ReadNextTuple;
//import qp.operators.TupleComparator; 


public class SortMergeJoin extends Join {

    private Parameters parameters;
    private static final String header = "SMJtemp-";

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
        // Sorts the left relation.
        left.open();
        right.open();

        parameters = new Parameters();
        // Initializes the cursors of input buffers for both sides.
        parameters.setLeftCursor(0);
        parameters.setRightCursor(0);
        // Right stream would be repetitively scanned. If it reaches the end, we have to start new scan.
        parameters.setEosLeft(false);
        parameters.setEosRight(false);
        parameters.setBatchSize(Batch.getPageSize() / schema.getTupleSize());

        // Gets the join attribute from left & right table.
        HashMap<JoinAttributeAssigner.AttributeKey,Integer> joinAttributes =
                JoinAttributeAssigner.getJoinAttributes(getCondition(), left, right);
        parameters.setLeftIndex(joinAttributes.get(JoinAttributeAssigner.AttributeKey.LEFT));
        parameters.setRightIndex(joinAttributes.get(JoinAttributeAssigner.AttributeKey.RIGHT));
        parameters.setAttributeType(left.getSchema().typeOf(getCondition().getLhs()));
        
        return TableGenerator.createTable(header, left, right);
    }

    /**
     * Selects tuples satisfying the join condition from input buffers and returns.
     *
     * @return the next page of output tuples.
     */
    @Override
    public Batch next() {
        // Returns empty if either left or right table reaches end-of-stream.
        if (parameters.getEosLeft() || parameters.getEosRight()) {
            close();
            return null;
        }

        // To handle the 1st run.
        if (parameters.getLeftBatch() == null) {
            Batch leftBatch = left.next();
            if (leftBatch == null) {
                parameters.setEosLeft(true);
                return null;
            }
            parameters.setLeftBatch(leftBatch);
            
            Tuple leftTuple = ReadNextTuple.readNextLeftTuple(parameters, left);
            parameters.setLeftTuple(leftTuple);
            if (leftTuple == null) {
                parameters.setEosLeft(true);
                return null;
            }
            parameters.setNextLeftTuple(leftTuple);
        }
        if (parameters.getRightBatch() == null) {
            Batch rightBatch = right.next();
            if (rightBatch == null) {
                parameters.setEosRight(true);
                return null;
            }
            parameters.setRightBatch(rightBatch);

            Partition.createNextRightPartition(parameters, () -> ReadNextTuple.readNextRightTuple(parameters, right));
            if (parameters.getRightPartition().isEmpty()) {
                parameters.setEosRight(true);
                return null;
            }
            parameters.setRightPartitionIndex(0);
            Tuple rightTuple = parameters.getRightPartition().elementAt(0);
            parameters.setRightTuple(rightTuple);
        }

        // The output buffer.
        Batch outBatch = new Batch(parameters.getBatchSize());

        while (!outBatch.isFull()) {
            int comparisionResult = Tuple.compareTuples(
                //parameters.getAttributeType(), 
                parameters.getLeftTuple(), 
                parameters.getRightTuple(), 
                parameters.getLeftIndex(), 
                parameters.getRightIndex()
            );
            if (comparisionResult == 0) {
                outBatch.add(parameters.getLeftTuple().joinWith(parameters.getRightTuple()));

                // Left tuple remains unchanged if it has not attempted to match with all tuples in the current right partition.
                if (parameters.getRightPartitionIndex() < parameters.getRightPartition().size() - 1) {
                    parameters.setRightPartitionIndex(parameters.getRightPartitionIndex() + 1);
                    Tuple rightTuple = parameters.getRightPartition().elementAt(parameters.getRightPartitionIndex());
                    parameters.setRightTuple(rightTuple);
                } else {
                    Tuple nextLeftTuple = ReadNextTuple.readNextLeftTuple(parameters, left);
                    if (nextLeftTuple == null) {
                        parameters.setEosLeft(true);
                        break;
                    }
                    comparisionResult = Tuple.compareTuples(
                        //parameters.getAttributeType(), 
                        parameters.getLeftTuple(), 
                        parameters.getNextLeftTuple(), 
                        parameters.getLeftIndex(), 
                        parameters.getLeftIndex()
                    );
                    parameters.setLeftTuple(parameters.getNextLeftTuple());

                    // Moves back to the beginning of right partition if the next left tuple remains the same value as the current one.
                    if (comparisionResult == 0) {
                        parameters.setRightPartitionIndex(0);
                        Tuple rightTuple = parameters.getRightPartition().elementAt(0);
                        parameters.setRightTuple(rightTuple);
                    } else {
                        // Proceeds and creates a new right partition otherwise.
                        Partition.createNextRightPartition(parameters, () -> ReadNextTuple.readNextRightTuple(parameters, right));
                        if (parameters.getRightPartition().isEmpty()) {
                            parameters.setEosRight(true);
                            break;
                        }

                        // Updates the right tuple.
                        parameters.setRightPartitionIndex(0);
                        Tuple rightTuple = parameters.getRightPartition().elementAt(0);
                        parameters.setRightTuple(rightTuple);
                    }
                }
            } else if (comparisionResult < 0) {
                Tuple leftTuple = ReadNextTuple.readNextLeftTuple(parameters, left);
                parameters.setLeftTuple(leftTuple);
                if (leftTuple == null) {
                    parameters.setEosLeft(true);
                    break;
                }
            } else {
                Partition.createNextRightPartition(parameters, () -> ReadNextTuple.readNextRightTuple(parameters, right));
                if (parameters.getRightPartition().isEmpty()) {
                    parameters.setEosRight(true);
                    break;
                }

                parameters.setRightPartitionIndex(0);
                Tuple rightTuple = parameters.getRightPartition().elementAt(0);
                parameters.setRightTuple(rightTuple);
            }
        }
        return outBatch;
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
