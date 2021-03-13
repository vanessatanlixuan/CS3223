package qp.operators.nestedjoin;

import java.io.EOFException;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Objects;

import qp.operators.Operator;
import qp.operators.TableGenerator;
import qp.utils.Batch;
import qp.utils.Tuple;

public class TupleSelector {

    public static Batch getTuplesMatchingCondition(int numberOfBuffer, Operator left, Parameters parameters) {

        ObjectInputStream fileInputStream = null;
        Batch[] leftBatches = new Batch[0];
        Batch rightBatch = null;

        Batch outBatch = new Batch(parameters.getBatchSize());

        while (!outBatch.isFull()) {
            // Checks whether we need to read a new block of pages from the left table.
            if (parameters.getLeftCursor() == 0 && parameters.getEosRight()) {
                leftBatches = new Batch[numberOfBuffer - 2];
                leftBatches[0] = left.next();
                // Checks if there is no more pages from the left table.
                if (leftBatches[0] == null) {
                    parameters.setEosLeft(true);
                    return outBatch;
                }
                for (int i = 1; i < leftBatches.length; i++) {
                    leftBatches[i] = left.next();
                    if (leftBatches[i] == null) {
                        break;
                    }
                }

                // Starts the scanning of right table whenever a new block of left pages comes.
                try {
                    fileInputStream = new ObjectInputStream(
                            new FileInputStream(TableGenerator.getTableName())
                    );
                    parameters.setEosRight(false);
                } catch (IOException io) {
                    System.err.println("BlockNestedJoin:error in reading the file");
                    System.exit(1);
                }
            }

            int numOfLeftTuple = leftBatches[0].size();
            for (int i = 1; i < leftBatches.length; i++) {
                if (leftBatches[i] == null) {
                    break;
                }
                numOfLeftTuple += leftBatches[i].size();
            }

            // Continuously probe the right table until we hit the end-of-stream.
            while (!parameters.getEosRight()) {
                try {
                    if (parameters.getRightCursor() == 0) {
                        rightBatch = (Batch) fileInputStream.readObject();
                    }

                    for (int i = parameters.getLeftCursor(); i < numOfLeftTuple; i++) {
                        int leftBatchIndex = i / leftBatches[0].size();
                        int leftTupleIndex = i % leftBatches[0].size();
                        Tuple leftTuple = leftBatches[leftBatchIndex].get(leftTupleIndex);

                        for (int j = parameters.getRightCursor(); j < Objects.requireNonNull(rightBatch).size(); j++) {
                            Tuple rightTuple = rightBatch.get(j);

                            // Adds the tuple if satisfying the join condition.
                            if (leftTuple.checkJoin(rightTuple, parameters.getLeftIndex(), parameters.getRightIndex())) {
                                Tuple outTuple = leftTuple.joinWith(rightTuple);
                                outBatch.add(outTuple);

                                // Checks whether the output buffer is full.
                                if (outBatch.isFull()) {
                                    if (i == numOfLeftTuple - 1 && j == rightBatch.size() - 1) {
                                        parameters.setRightCursor(0);
                                    } else if (i != numOfLeftTuple - 1 && j == rightBatch.size() - 1) {
                                        parameters.setLeftCursor(i + 1);
                                        parameters.setRightCursor(0);
                                    } else {
                                        parameters.setLeftCursor(i);
                                        parameters.setRightCursor(j + 1);
                                    }

                                    // Returns since we have already produced a complete page of matching tuples.
                                    return outBatch;
                                }
                            }
                        }
                        parameters.setRightCursor(0);
                    }
                    parameters.setLeftCursor(0);
                } catch (EOFException e) {
                    try {
                        fileInputStream.close();
                    } catch (IOException io) {
                        System.out.println("BlockNestedJoin: error in temporary file reading");
                    }
                    parameters.setEosRight(true);
                } catch (ClassNotFoundException c) {
                    System.out.println("BlockNestedJoin: some error in deserialization");
                    System.exit(1);
                } catch (IOException io) {
                    System.out.println("BlockNestedJoin: temporary file reading error");
                    System.exit(1);
                }
            }
        }
        return outBatch;
    }
}