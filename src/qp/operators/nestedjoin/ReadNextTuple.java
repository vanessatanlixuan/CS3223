package qp.operators.nestedjoin;

import qp.utils.Tuple;
import qp.operators.Operator;

public class ReadNextTuple {
    
    /**
     * Reads the next tuple from left input batch.
     *
     * @return the next tuple if available; null otherwise.
     */
    public static Tuple readNextLeftTuple(Parameters parameters, Operator left) {
        // Reads in another batch if necessary.
        if (parameters.getLeftBatch() == null) {
            parameters.setEosLeft(true);
            return null;
        } else if (parameters.getLeftCursor() == parameters.getLeftBatch().size()) {
            parameters.setLeftBatch(left.next());
            parameters.setLeftCursor(0);
        }

        // Checks whether the left batch still has tuples left.
        if (parameters.getLeftBatch() == null || parameters.getLeftBatch().size() <= parameters.getLeftCursor()) {
            parameters.setEosLeft(true);
            return null;
        }

        // Reads in the next tuple from left batch.
        Tuple nextLeftTuple = parameters.getLeftBatch().get(parameters.getLeftCursor());
        parameters.setLeftCursor(parameters.getLeftCursor() + 1);
        return nextLeftTuple;
    }

    /**
     * Reads the next tuple from right input batch.
     *
     * @return the next tuple if available; null otherwise.
     */
    public static Tuple readNextRightTuple(Parameters parameters, Operator right) {
        // Reads another batch if necessary.
        if (parameters.getRightBatch() == null) {
            return null;
        } else if (parameters.getRightCursor() == parameters.getRightBatch().size()) {
            parameters.setRightBatch(right.next());
            parameters.setRightCursor(0);
        }

        // Checks whether the right batch still has tuples left.
        if (parameters.getRightBatch() == null || parameters.getRightBatch().size() <= parameters.getRightCursor()) {
            return null;
        }

        // Reads the next tuple.
        Tuple next = parameters.getRightBatch().get(parameters.getRightCursor());
        parameters.setRightCursor(parameters.getRightCursor() + 1);
        return next;
    }
}
