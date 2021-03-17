package qp.operators;

import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Tuple;

import java.io.*;
import java.util.*;

import static java.lang.Math.min;

public class ExternalSort extends Operator{
    private Operator base;
    public int bufferNum;
    private int filenum;
    private int batchSize; // num of tuples in a single batch.
    private Comparator<Tuple> comparator;
    private List<File> sortedRunsFile;
    private ObjectInputStream resultStream;
    private ArrayList<Integer> attrIndex;
    private String identifier;


    public ExternalSort(Operator base, int bufferNum, int opType) {
        super(opType);
//        System.out.print("External sort schema");
        this.base = base;
        this.bufferNum = bufferNum;
        this.identifier = "";
    }

    public ExternalSort(Operator base, int buffernum, ArrayList<Integer> attrIndex, String identifier, int opType) {
        super(opType);
//        System.out.print("External sort schema");
//        Debug.PPrint(base.getSchema());
        this.base = base;
        this.bufferNum = buffernum;
        this.attrIndex = attrIndex;
        this.identifier = identifier;
    }

    public Operator getBase() {
        return base;
    }

    public void setBase(Operator base) {
        this.base = base;
    }

    @Override
    // open() for pre-processing.
    public boolean open() {
        if (!base.open()) {
            System.out.println("(External Sort) Failed to open External sort");
            return false;
        }

        this.filenum = 0;
        this.sortedRunsFile = new ArrayList<>();
        this.comparator = new TupleSortComparator(getAttributeList());
        this.batchSize = Batch.getPageSize() / this.base.getSchema().getTupleSize();

        generateSortedRuns();
        mergeRuns();

        // At the end, after the merging process, we should only have 1 run left.
        if (sortedRunsFile.size() != 1) {
            return false;
        }

        try {
            resultStream = new ObjectInputStream(new FileInputStream(sortedRunsFile.get(0)));
        } catch (IOException e) {
            System.out.println("IO Error when writing sorted file onto stream");
            return false;
        }
        return true;
    }

    public ArrayList<Integer> getAttributeList() {
        if (this.attrIndex != null) {
            return attrIndex;
        } else {
            ArrayList<Attribute> attrSet = base.getSchema().getAttList();
            ArrayList<Integer> result = new ArrayList<>();
            for (Attribute attr : attrSet) {
                result.add(base.getSchema().indexOf(attr));
            }
            return result;
        }
    }

    @Override
    public Batch next() {
        return nextBatchFromStream(resultStream);
    }

    @Override
    public boolean close() {
        try {
            for (File file : sortedRunsFile) {
                file.delete();
            }
            resultStream.close();
        } catch (IOException e) {
            System.out.println("Error in closing result file stream.");
        }
        return true;
    }

    private File writeFile(List<Batch> batchesToWrite) {
        try {
            File tempBatchFile = new File("ExternalSort" + "-" + this.filenum  + identifier);

            ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(tempBatchFile));
            for (Batch batch : batchesToWrite) {
                out.writeObject(batch);
            }
            this.filenum++;
            // initialize files for temp batches
            out.close();
            return tempBatchFile;
        } catch(IOException e) {
            System.out.println("Error in writing external sort batches to files");
        }
        return null;
    }

    private void generateSortedRuns() {
        // current batch
        Batch currentBatch = this.base.next();
        while (currentBatch != null) {
            // Initialize the buffer according to the amount of buffer we have.
            ArrayList<Batch> run = new ArrayList<>();
            for (int i = 0; i < this.bufferNum; i++) {
                if (currentBatch == null) {
                    break;
                } else {
                    run.add(currentBatch);
                    currentBatch = this.base.next();
                }
            }

            List<Tuple> tuples = new ArrayList<>();
            for (Batch batch : run) {
                // for each batch, append tuples to a list of tuples.
                for (int j = 0; j < batch.size(); j++) {
                    tuples.add(batch.get(j));
                }
            }
            Collections.sort(tuples, this.comparator);

            // after sorting, append back to the batches.
            List<Batch> batchesFromBuffer = new ArrayList<>();
            Batch newCurrentBatch = new Batch(this.batchSize);
            for (Tuple tuple : tuples) {
                newCurrentBatch.add(tuple);
                if (newCurrentBatch.isFull()) {
                    batchesFromBuffer.add(newCurrentBatch);
                    newCurrentBatch = new Batch(this.batchSize);
                }
            }

            // last page may not always be full.
            if (!newCurrentBatch.isEmpty()) {
                batchesFromBuffer.add(newCurrentBatch);
            }

            // write sorted runs (NewCurrentBatch) to temp file.
            if (batchesFromBuffer.size() < 1) {
//                System.out.println("NOT writing files");
                return;
            } else {
                File tempBatchFile = writeFile(batchesFromBuffer);
                this.sortedRunsFile.add(tempBatchFile);
            }
        }
    }

    private void mergeRuns() {
        int AvailableBuffers = this.bufferNum - 1;
        int numOfMergeRuns = 0;

        while (this.sortedRunsFile.size() > 1) {
            List<File> sortedRunsThisRound = new ArrayList<>();
            for (int numOfMerges = 0; numOfMerges * AvailableBuffers < this.sortedRunsFile.size(); numOfMerges++) {
                int end = min((numOfMerges + 1) * AvailableBuffers, sortedRunsFile.size());
                List<File> extractRuns = this.sortedRunsFile.subList(numOfMerges * AvailableBuffers, end);
                File resultantRun = mergeSortedRuns(extractRuns, numOfMergeRuns, numOfMerges);
                sortedRunsThisRound.add(resultantRun);
            }
            for (File file : this.sortedRunsFile) {
                file.delete();
            }

            numOfMergeRuns++;
            this.sortedRunsFile = sortedRunsThisRound;
        }
    }

    // input: files of sorted runs, each with a certain number of batches.
    // output: one single file of merged runs.
    private File mergeSortedRuns(List<File> sortedRuns, int numOfMergeRuns, int numOfMerges) {
        int numOfInputBuff = this.bufferNum - 1;
        if (sortedRuns.isEmpty()) {
            System.out.println("Sorted run is empty, nothing to sort here.");
            return null;
        }
        if (sortedRuns.size() > numOfInputBuff) {
            System.out.println("Number of sorted runs must be less than or equal to number of buffer - 1");
            return null;
        }

        ArrayList<Batch> inputBatches = new ArrayList<>();
        List<ObjectInputStream> inputs = new ArrayList<>();

        // Generated and input stream of sorted runs.
        for (File file : sortedRuns) {
            try {
                ObjectInputStream input = new ObjectInputStream(new FileInputStream(file));
                inputs.add(input);
            } catch (IOException e) {
                System.out.println("Error reading file into input stream.");
            }
        }

        // Feed in new batch into inputBatches.
        for (int sortedRunNum = 0; sortedRunNum < sortedRuns.size(); sortedRunNum++) {
            Batch nextBatch = nextBatchFromStream(inputs.get(sortedRunNum));
            while (nextBatch != null) {
                inputBatches.add(nextBatch);
                nextBatch = nextBatchFromStream(inputs.get(sortedRunNum));
            }
        }

        // Write all the tuples in inputBatches into the array of tuples. At the end, clear the tuples from
        // inputBatches.
        ArrayList<Tuple> inputTuples = new ArrayList<>();
        for (Batch batch : inputBatches) {
            if (batch == null) {
                continue;
            }
            while (!batch.isEmpty()) {
                int k = batch.size() - 1; 
                Tuple tuple = batch.get(k);
                inputTuples.add(tuple);
                batch.remove(-1);
                
            }
        }

        // Sort the array of tuples in desc order.
        inputTuples.sort(this.comparator.reversed());


        // A single output buffer to store the sorted tuples. When it is full, we will spill it over to file.
        Batch outputBuffer = new Batch(this.batchSize);
        // The result file to store the merged sorted runs.
        File resultFile = new File("ExternalSort_sortedRuns" + "_" + numOfMergeRuns + "_" + numOfMerges + identifier);
        ObjectOutputStream resultFileStream;
        try {
            resultFileStream = new ObjectOutputStream(new FileOutputStream(resultFile, true));
        } catch (FileNotFoundException e) {
            System.out.println("Unable to find file for output stream.");
            return null;
        } catch (IOException e) {
            System.out.println("IO error occurred while creating output stream.");
            return null;
        }

        // In each iteration we pop out the smallest element and add it into the output buffer
        // Once the buffer is filled write it into disk.
        while (!inputTuples.isEmpty()) {
            Tuple currentTuple = inputTuples.remove(inputTuples.size() - 1);
            outputBuffer.add(currentTuple);
            if (outputBuffer.isFull()) {
                try {
                    resultFileStream.writeObject(outputBuffer);
                    resultFileStream.reset();
                } catch (IOException e) {
                    System.out.println("Error in writing to output file during merging.");
                    return null;
                }
                outputBuffer.clear();
            }
        }

        if (!outputBuffer.isEmpty()) {
            try {
                resultFileStream.writeObject(outputBuffer);
                resultFileStream.reset();
            } catch (IOException e) {
                System.out.println("Error in writing to output file during merging.");
                return null;
            }
        }
        return resultFile;
    }

    protected Batch nextBatchFromStream(ObjectInputStream stream) {
        try {
            Batch batch = (Batch) stream.readObject();
            if (batch.isEmpty()) {
                return null;
            }
            return batch;
        } catch (ClassNotFoundException e) {
            System.out.println("Unable to serialize the read object.");
            return null;
        } catch (IOException e) {
            return null;
        }
    }

    static class TupleSortComparator implements Comparator<Tuple>{
        private ArrayList<Integer> index_sort;
        TupleSortComparator(ArrayList<Integer> index_sort_in) {
            this.index_sort = index_sort_in;
        }

        @Override
        public int compare(Tuple firstTuple, Tuple secondTuple) {
            for (int index : this.index_sort) {
                int compareValue = Tuple.compareTuples(firstTuple, secondTuple, index);
                if (compareValue != 0) {
                    return compareValue;
                }
            }
            return 0;
        }
    }
}