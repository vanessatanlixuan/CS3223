/*
to perform external sort on a relation table
*/

package qp.operators;

import qp.utils.*;

import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
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
    Batch inBatch; //the page containing the input
    Batch outBatch; //the page containing the output
    int totalNumOfTuples;
    int lastRoundIndex;
    //lastPageIndexInEachRun stores the number of pages in each sorted run of round 0

    Map<Integer, Integer> lastPageIndexInEachRun = new HashMap<Integer, Integer>();
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
        numRuns = generateSortedRuns();
        merge();
    }

    public int generateSortedRuns() { //returns number of sorted runs
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
        return index;
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
                    totalNumOfTuples++;
                }
                else
                    continue;
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
        int currNumRuns = lastPageIndexInEachMergingRun.size();
        int newRoundNum = currRoundNum +1;
        HashMap<Integer, Integer> updatedLastPageIndexInEachMergingRun = merge(currRoundNum, lastPageIndexInEachMergingRun); //currRoundNum gives info on where is the thing stored
        if (currNumRuns <= numBuff-1){
            //able to merge within this run
            //update the lastRoundIndex to find where the final results are stored in directory
            this.lastRoundIndex = newRoundNum;
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
        
        for(int runIndex = startRunIndex; runIndex <= endRunIndex; runIndex++){
            retrieveFile(currRoundNum, runIndex, page 0 of each run);
            storeInBuffer();
            
        }
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

    public int compare(Tuple left, Tuple right) {
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
