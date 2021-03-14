/*
to perform external sort on a relation table
*/

package qp.operators;

import qp.utils.*;

import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Collections;
import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.HashMap;

public class ExternalSort extends Operator {
    
    Operator base; //the relation (table) that we need to sort
    ArrayList<Attribute> attrset; //the attributes that we are sorting on
    int batchSize; //the size of every output page
    int numBuff; //number of buffer pages
    int numRuns; //number of sorted runs
    //Batch inBatch; //the page containing the input
    Batch outBatch; //the page containing the output
    int lastRoundIndex;
    int lastSortedPage;
    int nextOutputPage = 0;
    boolean allSorted = false;
    //lastPageIndexInEachRun stores the number of pages in each sorted run of round 0

    HashMap<Integer, Integer> lastPageIndexInEachRun = new HashMap<Integer, Integer>();
    /**
     * index of the attributes in the base operator(relation/table)
     * * that are to be sorted
     **/
    //int[] attrIndex;
    ArrayList<Integer> attrIndex;


    public ExternalSort(Operator base, ArrayList<Attribute> as, int type) {
        super(type);
        this.base = base;
        this.attrset = as;
    }

    public Operator getBase() { //obtain the relation
        return base;
    }

    public void setBase(Operator base) { //set the relation
        this.base = base;
    }

    public ArrayList<Attribute> getSortAttr() { //get the attributes that we are sorting on
        return attrset;
    }

    public int getNumBuff() {
        return numBuff;
    }

    public void setNumBuff(int num) {
        this.numBuff = num;
    }


    /*
    * open external sort operator
    */
    @Override 
    public boolean open() {
        
        if (!base.open()) return false; //if table is not open, return false --> cant perform sorting on unopened table
        Schema baseSchema = base.getSchema();
        /** set number of tuples per page(batch) **/
        int tuplesize = baseSchema.getTupleSize(); // get size (num bytes) of tuple of the relation
        batchSize = Batch.getPageSize() / tuplesize; 
        //batchsize is the number of tuples in a page.


        //create an int array consisting of indices of req'd attributes to sort by
        attrIndex = new ArrayList<Integer>(); 
        //for loop to obtain indices of attributes
        for (int i = 0; i < attrset.size(); i++) {
            Attribute attr = attrset.get(i);
            int index = baseSchema.indexOf(attr.getBaseAttribute()); //get the index of the attribute
            attrIndex.add(index);
        }
        
        //1. load tuples of schema into pages
            // need to know how many tuples can one buffer page hold 
            // need to consider max num of buffer pages that system can take
            // need to calc how many rounds of internal sort needed (n)
        //2. perform internal sort on the pages for n times
        //3. write out to disk each time
        ////////////// stage 2 of sort ////////////////////
        //4. assign one buffer page for output, rest of the buffer pages for input
        //5. while no active input pages is empty, find min entry across the input buffer pages
        //   cut and paste that entry to the output buffer
        //   if there is empty input buffer page, load one more page into memory
        //6. when buffer page for output is full, write out to disk
        //7. 
        generateSortedRuns();
        recursivelyMerge(0, lastPageIndexInEachRun);
        return this.allSorted;
    }

    public void generateSortedRuns() { //returns number of sorted runs
        boolean eos = false; //have we reached the end of the table yet?
        int index = 0; //index for first filename
        //numRounds = number of internal sort rounds needed 
        //1. load as many batches as possible into buffer
        ArrayList<Batch> pagesInBuffer = new ArrayList<Batch>();
        
        Batch incomingPage;

        int counter = 0;

        while (eos == false){ //while there are still pages in table(base)
            incomingPage = base.next();
            // if the page being read is null aka no more pages
            if(incomingPage == null){
                
                //sort the pages already in Buffer + write out
                internalSort(pagesInBuffer, index); //counter = num of pages that buffer need to output
                //clear the pages in buffer
                index ++;
                pagesInBuffer.clear();
                eos = true;
                break;
            }
            if(counter < numBuff){
                pagesInBuffer.add(incomingPage);
                counter++;
            }
            else{
                //sort the pagesInBuffer + write out
                internalSort(pagesInBuffer, index);
                index++;
                //set counter back to 0
                counter = 0;
                //clear the pages in buffer
                pagesInBuffer.clear();
            }
        }
    }

    public void internalSort(ArrayList<Batch> pagesInBuffer, int fileIndex){
        //input arraylist of pages in the buffer
        //output: write out pages of sorted tuples
        if(lastPageIndexInEachRun.containsKey(Integer.valueOf(fileIndex)) == false){
            lastPageIndexInEachRun.put(fileIndex, 0);        
        }
        ArrayList<Tuple> tupInRun = new ArrayList<Tuple>();
        Tuple currTup;
        for(Batch b : pagesInBuffer){
            //add all the tuples in page into tuples in the run
            for (int j = 0; j < batchSize; j++){
                currTup = b.get(j);
                if ((currTup == null) == false){
                    tupInRun.add(currTup);
                }
                else{
                    continue;
                }
            }
        }
        //now we have tupInRun list containing all the tuples to be sorted in a run
        tupComparator tupleCompare = new tupComparator(attrIndex);
        Collections.sort(tupInRun, tupleCompare);
        //output to fileoutputstream
        ArrayList<Batch> listOfBatches = generateListOfBatches(tupInRun, batchSize);
        int cntr = 0;
        Iterator<Batch> iter = listOfBatches.iterator();
        while(iter.hasNext()){
            Batch currBatch = iter.next();
            try{
                String fname = "ESrun-round-0-run-" + String.valueOf(fileIndex) + "-page-" + String.valueOf(cntr);
                FileOutputStream file = new FileOutputStream(fname);
                ObjectOutputStream out = new ObjectOutputStream(file);
                out.writeObject(currBatch);
                out.close();
                   
            }
            catch (IOException io){
                System.out.println("Error writing out to temp file for ES round 0 - sorted runs.");
            } 
            lastPageIndexInEachRun.replace(fileIndex, cntr);
            //lastPageIndex = cntr;
            cntr++;
        }
    }

    public ArrayList<Batch> generateListOfBatches(ArrayList<Tuple> tupInRun, int batchSize){
        ArrayList<Batch> listOfBatches = new ArrayList<Batch>();
        ArrayList<Tuple> oneBatch = new ArrayList<Tuple>();
        int ctr = 0;
        for (int i = 0; i < tupInRun.size(); i++){
            while(ctr < batchSize){
                Tuple currTup = tupInRun.get(i);
                ctr ++;
                oneBatch.add(currTup); 
            }
            Batch newBatch = createBatch(oneBatch);
            listOfBatches.add(newBatch);
            oneBatch.clear();
        }
        return listOfBatches;
    }

    public Batch createBatch(ArrayList<Tuple> oneBatch){
        int numTuples = oneBatch.size(); 
        Batch newBatch = new Batch(numTuples);
        for (Tuple tup: oneBatch){
            newBatch.add(tup);
        }
        return newBatch;
    }

    public void recursivelyMerge(int currRoundNum, HashMap<Integer, Integer> lastPageIndexInEachMergingRun){
        int newRoundNum = currRoundNum +1;
        HashMap<Integer, Integer> updatedLastPageIndexInEachMergingRun = merge(currRoundNum, lastPageIndexInEachMergingRun); //currRoundNum gives info on where is the thing stored
        if (updatedLastPageIndexInEachMergingRun.size() == 1){
            //able to merge within this run
            //update the lastRoundIndex to find where the final results are stored in directory
            this.lastRoundIndex = newRoundNum;
            this.lastSortedPage = updatedLastPageIndexInEachMergingRun.get(0);
            this.allSorted = true;
        }
        else{
            recursivelyMerge(newRoundNum, updatedLastPageIndexInEachMergingRun);
        }
    }

    public HashMap<Integer, Integer> merge(int currRoundNum, HashMap<Integer, Integer> lastPageIndexInEachMergingRun){
        //currRoundIndex is the INDEX OF THE ROUND THAT WE WANT TO MERGE
        int startRunIndex = 0;
        int endRunIndex = startRunIndex + numBuff-1;
        HashMap<Integer, Integer> newLastPageIndices = new HashMap<Integer, Integer>();
        int newRunIndex = 0;
        while(startRunIndex < numBuff-1){
            if(endRunIndex > numRuns){
                endRunIndex = numRuns;
            }
            int newLastPageNumberOfRun  = mergeRuns(startRunIndex, endRunIndex, currRoundNum, newRunIndex, lastPageIndexInEachMergingRun);
            newLastPageIndices.put(newRunIndex, newLastPageNumberOfRun);
            if(endRunIndex == numRuns){
                break; //break out from while loop when all the sets of runs are merged within themselves
            }
            else{
                startRunIndex = endRunIndex + 1; //then go through the while loop again
            }
            newRunIndex++;
        }
        return newLastPageIndices; //run index mapped to num pages in that run
    }
    /*
    * return newLastPageNumberOfRun
    * saves file with newRoundNum, newRunIndex, newPageNum(s)
    */ 
    public int mergeRuns(int startRunIndex, int endRunIndex, int currRoundNum, int newRunIndex, HashMap<Integer, Integer> lastPageIndexInEachMergingRun){
        int numBuffInUse = endRunIndex - startRunIndex + 1;
        Batch[] batchesInBuffer = new Batch[numBuffInUse];
        ObjectInputStream[] batchesInStream = new ObjectInputStream[numBuffInUse];
        boolean[] allPagesRead = new boolean[numBuffInUse];
        Comparator<TupleWInfo> tupWICompare = new tupWIComparator(attrIndex);
        PriorityQueue<TupleWInfo> intermediate = new PriorityQueue<TupleWInfo>(batchSize, tupWICompare);

        for(int runIndex = startRunIndex; runIndex <= endRunIndex; runIndex++){
            //retrieve first page of each run
            int indexInArray = runIndex - startRunIndex;
            String inputFile = "ESrun-round-"+ Integer.valueOf(currRoundNum) +"-run-" + String.valueOf(runIndex) + "-page-" + String.valueOf(0); 
            try{
                ObjectInputStream in = new ObjectInputStream(new FileInputStream(inputFile));
                batchesInStream[indexInArray] = in;
                batchesInBuffer[indexInArray] = (Batch) in.readObject();
                //the object read in is a batch(page)
                in.close();
            }
            catch(IOException io){
                System.err.printf("in mergeRuns of ES - error in reading the file %s.", inputFile);
                System.exit(1);
            }
            catch(ClassNotFoundException c){
                System.out.printf("in mergeRuns of ES: Error in deserialising temporary file %s.", inputFile);
                System.exit(1);
            }
            try{
            // put in first tuple in batch into the priority queue
                ArrayList<Tuple> curr = batchesInBuffer[indexInArray].getAllTuples();
                if(curr.isEmpty() == false){
                    //add first tuple of batch into PQ, with info: roundNum, index of where its page is stored in array
                    //and the page num of this tuple
                    //TupleWInfo(Tuple original, int roundNum, int runNum, int actualRunNum, int pageNum, int posInPage)
                    intermediate.add(new TupleWInfo(curr.get(0), currRoundNum, indexInArray, runIndex, 0, 0));
                }
            }
            catch(IndexOutOfBoundsException e){
                System.out.printf("IndexOutOfBoundsException when trying to add first tuple into PQ");
            }
        }
        //now we have a list of batches (havent start reading)(1st page of each run)
        // a hashmap of how many pages there are in each run
        //outputBatch = the batch to writeout
        Batch outputBatch = new Batch(batchSize);
        ObjectOutputStream output;

        int newRoundNum = currRoundNum +1;
        //int newRunNum = newRunIndex; <-- already in input
        int newPageNum = 0;
        
        while(intermediate.isEmpty() == false){
            TupleWInfo smallestTupWI = intermediate.poll();
            if(outputBatch.isFull()){
                //writeout
                String outputFileName = "ESrun-round-"+ Integer.valueOf(newRoundNum) +"-run-" + String.valueOf(newRunIndex) + "-page-" + String.valueOf(newPageNum); 
                try{
                    output = new ObjectOutputStream(new FileOutputStream(outputFileName));
                    output.writeObject(outputBatch);
                }
                catch (IOException io){
                    System.out.printf("in ES: mergeRuns: unable to write out page of a sorted run, %s", outputFileName);
                }
                outBatch.clear();
            }
            
            outputBatch.add(smallestTupWI.originalTuple);
            
            int buffIndexToAddFrom = smallestTupWI.runNum; //runNum = index of the run in array
            int pageNumOfTuple = smallestTupWI.pageNum;
            int indexOfNewTupleToAdd = smallestTupWI.posInPage + 1;
            int currActualRunNum = smallestTupWI.actualRunNum;
            Batch batchToAddFrom = batchesInBuffer[buffIndexToAddFrom];
            
            if(indexOfNewTupleToAdd > batchToAddFrom.getNumTuples()-1){
                //if the index of new tuple to add is more than the last tuple index in page
                //loadNextPage()
                int nextBatchIndex = smallestTupWI.pageNum + 1;
                Integer lastBatchPageNum = lastPageIndexInEachMergingRun.get(Integer.valueOf(currActualRunNum));
                //if all the pages in the run has been read
                if (nextBatchIndex > lastBatchPageNum){
                    //means that all pages in run already read
                    allPagesRead[smallestTupWI.runNum] = true;
                    //read next available 
                    continue;
                }
                else{ //there is still pages in run not read: load next page
                    String inputFile = "ESrun-round-"+ Integer.valueOf(smallestTupWI.roundNum) +"-run-" + String.valueOf(currActualRunNum) + "-page-" + String.valueOf(nextBatchIndex); 
                    try{
                        ObjectInputStream in = new ObjectInputStream(new FileInputStream(inputFile));
                        batchesInStream[buffIndexToAddFrom] = in;
                        batchesInBuffer[buffIndexToAddFrom] = (Batch) in.readObject();
                        //the object read in is a batch(page)
                        in.close();
                    }
                    catch(IOException io){
                        System.err.printf("in mergeRuns of ES - error in reading the file %s.", inputFile);
                        System.exit(1);
                    }
                    catch(ClassNotFoundException c){
                        System.out.printf("in mergeRuns of ES: Error in deserialising temporary file %s.", inputFile);
                        System.exit(1);
                    }
                    try{
                        // put in first tuple in batch into the priority queue
                        ArrayList<Tuple> curr = batchesInBuffer[buffIndexToAddFrom].getAllTuples();
                        if(curr.isEmpty() == false){
                            //add first tuple of batch into PQ, with info: roundNum, index of where its page is stored in array
                            //and the page num of this tuple
                            //TupleWInfo(Tuple original, int roundNum, int runNum, int actualRunNum, int pageNum, int posInPage)
                            intermediate.add(new TupleWInfo(curr.get(0), currRoundNum, buffIndexToAddFrom, currActualRunNum, nextBatchIndex, 0));
                        }
                    }
                    catch(IndexOutOfBoundsException e){
                        System.out.printf("IndexOutOfBoundsException when trying to add first tuple into PQ");
                    }
                    
                }
            }
            else{
                Tuple tup = batchToAddFrom.get(indexOfNewTupleToAdd);
                //TupleWInfo(Tuple original, int roundNum, int runNum, int actualRunNum, int pageNum, int posInPage)
                TupleWInfo tupToAdd = new TupleWInfo(tup, currRoundNum, buffIndexToAddFrom, currActualRunNum, pageNumOfTuple, indexOfNewTupleToAdd);
                intermediate.add(tupToAdd);
            }
        }
        return newPageNum;
    }

    public Batch next(){
        String inputFile;
        ObjectInputStream in;
        //read each page in the output
        inputFile = "ESrun-round-"+ Integer.valueOf(this.lastRoundIndex) +"-run-" + String.valueOf(0) + "-page-" + String.valueOf(this.nextOutputPage);
        try{
            //read the page
            in = new ObjectInputStream(new FileInputStream(inputFile));
            outBatch = (Batch) in.readObject();
            //the object read in is a batch(page)
            in.close();
        }
        catch(IOException io){
            System.err.printf("ES next() - error in reading the file %s.", inputFile);
            System.exit(1);
        }
        catch(ClassNotFoundException c){
            System.out.printf("ES next(): Error in deserialising temporary file %s.", inputFile);
            System.exit(1);
        } 
        this.nextOutputPage ++;
        return outBatch;
    }

    public boolean close(){
        super.close();
        return true;
    }

    
}
//need to create this informed tuple to know which buffer to get next tuple from
class TupleWInfo {
    public Tuple originalTuple;
    public int runNum;
    public int pageNum;
    public int roundNum; //roundNum is the round number of the round we are trying to merge
    public int posInPage;
    public int actualRunNum;
    public TupleWInfo(Tuple original, int roundNum, int runNum, int actualRunNum, int pageNum, int posInPage){
        this.originalTuple = original;
        this.roundNum = roundNum;
        this.runNum = runNum;
        this.pageNum = pageNum;
        this.actualRunNum = actualRunNum;
        this.posInPage = posInPage;
    }
}

class tupWIComparator implements Comparator<TupleWInfo>{
    //taken from tuple class
    /**
     * Comparing tuples in different tables with multiple conditions, used for join condition checking
     **/
    //list of indices of attributes to sort by
    ArrayList<Integer> attrIndex;

    //constructor: input: array
    public tupWIComparator(ArrayList<Integer> attrIndexList){
        //convert array into arraylist to use it in compare
        this.attrIndex = attrIndexList;
    }

    public int compare(TupleWInfo leftTup, TupleWInfo rightTup){
        /*
        if (leftIndex.size() != rightIndex.size()) {
            System.out.println("Tuple: Unknown comparision of the tuples");
            System.exit(1);
            return 0;
        }
        */
        Tuple left = leftTup.originalTuple;
        Tuple right = rightTup.originalTuple;
        for (int i = 0; i < attrIndex.size(); ++i) {
            Object leftdata = left.dataAt(attrIndex.get(i));
            Object rightdata = right.dataAt(attrIndex.get(i));
            if (leftdata.equals(rightdata)) continue;
            if (leftdata instanceof Integer) {
                return ((Integer) leftdata).compareTo((Integer) rightdata);
            } else if (leftdata instanceof String) {
                return ((String) leftdata).compareTo((String) rightdata);
            } else if (leftdata instanceof Float) {
                return ((Float) leftdata).compareTo((Float) rightdata);
            } else {
                System.out.println("Tuple: Unknown comparision of the tuples in ExternalSort");
                System.exit(1);
                return 0;
            }
        }
        return 0;
    }
}
class tupComparator implements Comparator<Tuple>{
    //taken from tuple class
    /**
     * Comparing tuples in different tables with multiple conditions, used for join condition checking
     **/
    //list of indices of attributes to sort by
    ArrayList<Integer> attrIndex;

    //constructor: input: array
    public tupComparator(ArrayList<Integer> attrIndexList){
        //convert array into arraylist to use it in compare
        this.attrIndex = attrIndexList;
    }

    public int compare(Tuple left, Tuple right){
        /*
        if (leftIndex.size() != rightIndex.size()) {
            System.out.println("Tuple: Unknown comparision of the tuples");
            System.exit(1);
            return 0;
        }
        */
        for (int i = 0; i < attrIndex.size(); ++i) {
            Object leftdata = left.dataAt(attrIndex.get(i));
            Object rightdata = right.dataAt(attrIndex.get(i));
            if (leftdata.equals(rightdata)) continue;
            if (leftdata instanceof Integer) {
                return ((Integer) leftdata).compareTo((Integer) rightdata);
            } else if (leftdata instanceof String) {
                return ((String) leftdata).compareTo((String) rightdata);
            } else if (leftdata instanceof Float) {
                return ((Float) leftdata).compareTo((Float) rightdata);
            } else {
                System.out.println("Tuple: Unknown comparision of the tuples in ExternalSort");
                System.exit(1);
                return 0;
            }
        }
        return 0;
    }
}
