package qp.operators;

import java.io.File;
import java.util.HashMap;

import qp.operators.blocknestedjoin.JoinAttributeAssigner;
import qp.operators.blocknestedjoin.RightTableGenerator;
import qp.operators.blocknestedjoin.TupleSelector;
import qp.utils.Batch;

public class BlockNestedJoin extends Join {

    private int batchSize;
    private int leftIndex, rightIndex;
    private int leftCursor, rightCursor;
    private boolean eosLeft, eosRight;

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

    private void init() {
        // Initializes the cursors of input buffers for both sides.
        leftCursor = 0;
        rightCursor = 0;
        eosLeft = false;
        // Right stream would be repetitively scanned. If it reaches the end, we have to start new scan.
        eosRight = true;
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

        batchSize = Batch.getPageSize() / schema.getTupleSize();

        // Gets the join attribute from left & right table.
        HashMap<JoinAttributeAssigner.AttributeKey,Integer> joinAttributes =
                JoinAttributeAssigner.getJoinAttributes(getCondition(), left, right);
        leftIndex = joinAttributes.get(JoinAttributeAssigner.AttributeKey.LEFT);
        rightIndex = joinAttributes.get(JoinAttributeAssigner.AttributeKey.RIGHT);

        init();

        return RightTableGenerator.createRightTable(left, right);
    }

    /**
     * Selects tuples satisfying the join condition from input buffers and returns.
     *
     * @return the next page of output tuples.
     */
    @Override
    public Batch next() {
        // Returns empty if the left table reaches end-of-stream.
        if (eosLeft) {
            close();
            return null;
        }

        return TupleSelector.getTuplesMatchingCondition(
                batchSize, numBuff,
                leftCursor, rightCursor,
                eosLeft, eosRight,
                leftIndex, rightIndex, left
        );
    }

    /**
     * Closes this operator by deleting the file generated.
     *
     * @return true if the operator is closed successfully.
     */
    @Override
    public boolean close() {
        File f = new File(RightTableGenerator.getTableName());
        return f.delete();
    }
}