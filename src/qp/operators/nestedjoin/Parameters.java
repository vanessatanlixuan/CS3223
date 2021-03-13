package qp.operators.nestedjoin;

import java.util.HashMap;
import java.util.Vector;

import qp.utils.Batch;
import qp.utils.Tuple;

public class Parameters {

    public enum Params {
        BATCH_SIZE, LEFT_INDEX, RIGHT_INDEX,
        LEFT_CURSOR, RIGHT_CURSOR, EOS_LEFT, EOS_RIGHT,
        RIGHT_PARTITION, RIGHT_PARTITION_INDEX, NEXT_RIGHT_TUPLE,
        LEFT_BATCH, RIGHT_BATCH, ATTRIBUTE_TYPE,
        LEFT_PARTITION, LEFT_PARTITION_INDEX, NEXT_LEFT_TUPLE,
        LEFT_TUPLE, RIGHT_TUPLE
    }

    private static HashMap<Params, Object> params;

    public Parameters() {
        params = new HashMap<>();
    }

    public void setAttributeType(int type) {params.put(Params.ATTRIBUTE_TYPE, type);}

    public int getAttributeType() {return (int) params.get(Params.ATTRIBUTE_TYPE);}

    public int getBatchSize() { return (int) params.get(Params.BATCH_SIZE); }

    public void setBatchSize(int batchSize) { params.put(Params.BATCH_SIZE, batchSize); }

    public int getLeftIndex() { return (int) params.get(Params.LEFT_INDEX); }

    public void setLeftIndex(int leftIndex) { params.put(Params.LEFT_INDEX, leftIndex); }

    public int getRightIndex() { return (int) params.get(Params.RIGHT_INDEX); }

    public void setRightIndex(int rightIndex) { params.put(Params.RIGHT_INDEX, rightIndex); }

    public int getLeftCursor() { return (int) params.get(Params.LEFT_CURSOR); }

    public void setLeftCursor(int leftCursor) { params.put(Params.LEFT_CURSOR, leftCursor); }

    public int getRightCursor() { return (int) params.get(Params.RIGHT_CURSOR); }

    public void setRightCursor(int rightCursor) { params.put(Params.RIGHT_CURSOR, rightCursor); }

    public boolean getEosLeft() { return (boolean) params.get(Params.EOS_LEFT); }

    public void setEosLeft(boolean eos) { params.put(Params.EOS_LEFT, eos); }

    public boolean getEosRight() { return (boolean) params.get(Params.EOS_RIGHT); }

    public void setEosRight(boolean eos) { params.put(Params.EOS_RIGHT, eos); }

    public void setRightPartition(Vector<Tuple> rightPartition) { params.put(Params.RIGHT_PARTITION, rightPartition); }

    public Vector<Tuple> getRightPartition() {
        @SuppressWarnings("unchecked")
        Vector<Tuple> partition = (Vector<Tuple>) params.get(Params.RIGHT_PARTITION);
        return partition;
    }

    public void setRightPartitionIndex(int index) { params.put(Params.RIGHT_PARTITION_INDEX, index); }

    public int getRightPartitionIndex() {return (int) params.get(Params.RIGHT_PARTITION_INDEX);}

    public void setNextRightTuple(Tuple rightTuple) { params.put(Params.NEXT_RIGHT_TUPLE, rightTuple);}

    public Tuple getNextRightTuple() { return (Tuple) params.get(Params.NEXT_RIGHT_TUPLE);}

    public void setLeftPartition(Vector<Tuple> leftPartition) { params.put(Params.LEFT_PARTITION, leftPartition); }

    public Vector<Tuple> getLeftPartition() {
        @SuppressWarnings("unchecked")
        Vector<Tuple> partition = (Vector<Tuple>) params.get(Params.LEFT_PARTITION); 
        return partition;
    }

    public void setLeftPartitionIndex(int index) { params.put(Params.LEFT_PARTITION_INDEX, index); }

    public int getLeftPartitionIndex() {return (int) params.get(Params.LEFT_PARTITION_INDEX);}

    public void setNextLeftTuple(Tuple leftTuple) { params.put(Params.NEXT_LEFT_TUPLE, leftTuple);}

    public Tuple getNextLeftTuple() { return (Tuple) params.get(Params.NEXT_LEFT_TUPLE);}

    public void setRightBatch(Batch batch) { params.put(Params.RIGHT_BATCH, batch);}

    public Batch getRightBatch() { return (Batch) params.get(Params.RIGHT_BATCH);}

    public void setLeftBatch(Batch batch) { params.put(Params.LEFT_BATCH, batch);}

    public Batch getLeftBatch() { return (Batch) params.get(Params.LEFT_BATCH);}

    public void setLeftTuple(Tuple leftTuple) { params.put(Params.LEFT_TUPLE, leftTuple);}

    public Tuple getLeftTuple() { return (Tuple) params.get(Params.LEFT_TUPLE);}

    public void setRightTuple(Tuple rightTuple) { params.put(Params.RIGHT_TUPLE, rightTuple);}

    public Tuple getRightTuple() { return (Tuple) params.get(Params.RIGHT_TUPLE);}
}