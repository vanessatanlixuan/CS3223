package qp.operators;

import java.io.File;
import java.util.HashMap;

import qp.operators.blocknestedjoin.Parameters;
import qp.operators.blocknestedjoin.TupleSelector;
import qp.utils.Batch;

public class BlockNestedJoin extends Join {

    private Parameters parameters;
    private static final String header = "BNJtemp-";

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
     * Opens this operator by performing the following operations:
     * 1. Finds the index of the join attributes;
     * 2. Materializes the right hand side into a file;
     * 3. Opens the connections.
     *
     * @return true if the operator is opened successfully.
     */
    @Override
    public boolean open() {
        parameters = new Parameters();
        // Initializes the cursors of input buffers for both sides.
        parameters.setLeftCursor(0);
        parameters.setRightCursor(0);
        // Right stream would be repetitively scanned. If it reaches the end, we have to start new scan.
        parameters.setEosLeft(false);
        parameters.setEosRight(true);

        parameters.setBatchSize(Batch.getPageSize() / schema.getTupleSize());

        // Gets the join attribute from left & right table.
        HashMap<JoinAttributeAssigner.AttributeKey,Integer> joinAttributes =
                JoinAttributeAssigner.getJoinAttributes(getCondition(), left, right);
        parameters.setLeftIndex(joinAttributes.get(JoinAttributeAssigner.AttributeKey.LEFT));
        parameters.setRightIndex(joinAttributes.get(JoinAttributeAssigner.AttributeKey.RIGHT));

        return TableGenerator.createTable(header, left, right);
    }

    /**
     * Selects tuples satisfying the join condition from input buffers and returns.
     *
     * @return the next page of output tuples.
     */
    @Override
    public Batch next() {
        // Returns empty if the left table reaches end-of-stream.
        if (parameters.getEosLeft()) {
            close();
            return null;
        }

        return TupleSelector.getTuplesMatchingCondition(numBuff, left, parameters);
    }

    /**
     * Closes this operator by deleting the file generated.
     *
     * @return true if the operator is closed successfully.
     */
    @Override
    public boolean close() {
        File f = new File(TableGenerator.getTableName());
        return f.delete();
    }
}