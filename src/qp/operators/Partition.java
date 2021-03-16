package qp.operators;

import java.util.Vector;
import java.util.function.Supplier;

import qp.operators.nestedjoin.Parameters;
import qp.utils.Tuple;

public class Partition {

    /**
     * Creates the next partition from the right input batch based on the current right cursor value.
     *
     * @return a vector containing all tuples in the next right partition.
     */
    public static void createNextRightPartition(Parameters params, Supplier<Tuple> readNextRightTuple) {
        Vector<Tuple> partition = new Vector<>();
        int comparisionResult = 0;
        Tuple nextRightTuple = params.getNextRightTuple();
        if (nextRightTuple == null) {
            params.setNextRightTuple(readNextRightTuple.get());
            if (nextRightTuple == null) {
                params.setRightPartition(partition);
                return;
            }
        }

        // Continues until the next tuple carries a different value.
        while (comparisionResult == 0) {
            partition.add(nextRightTuple);
            params.setNextRightTuple(readNextRightTuple.get());
            if (nextRightTuple == null) {
                break;
            }
            comparisionResult = Tuple.compareTuples(
                //params.getAttributeType(),
                partition.elementAt(0),
                nextRightTuple,
                params.getLeftIndex(),
                params.getRightIndex()
            );
        }

        params.setRightPartition(partition);
    }
}