package qp.operators.blocknestedjoin;

import java.util.HashMap;

public class Parameters {

    public enum Params {
        BATCH_SIZE, LEFT_INDEX, RIGHT_INDEX,
        LEFT_CURSOR, RIGHT_CURSOR, EOS_LEFT, EOS_RIGHT
    }

    private static HashMap<Params, Object> params;

    public Parameters() {
        params = new HashMap<>();
    }

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
}
