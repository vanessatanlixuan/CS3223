/*
to perform external sort on a relation table
*/

package qp.operators;

import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Condition;
import qp.utils.Tuple;

import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class ExternalSort extends Operator {
    
    Operator base; //the relation (table) that we need to sort
    ArrayList<Attribute> attrset; //the attributes that we are sorting on
    int batchSize; //the size of every output page
    int numBuff; //number of buffer pages
    int numRuns; //number of sorted runs
    Batch inBatch; //the page containing the input
    Batch outBatch; //the page containing the output

    //sortedRunAddr will store the address to where the sorted runs are in disk
    ArrayList<Integer> sortedRunAddr = new ArrayList<Integer>();
    /**
     * index of the attributes in the base operator(relation/table)
     * * that are to be sorted
     **/
    int[] attrIndex;


    public ExternalSort(Operator base, ArrayList<Attribute> as, String type) {
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
        attrIndex = new int[attrset.size()]; 
        //for loop to obtain indices of attributes
        for (int i = 0; i < attrset.size(); i++) {
            Attribute attr = attrset.get(i);
            int index = baseSchema.indexOf(attr.getBaseAttribute()); //get the index of the attribute
            attrIndex[i] = index;
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
                index ++;
                //sort the pages already in Buffer + write out
                internalSort(pagesInBuffer, index); //counter = num of pages that buffer need to output
                //clear the pages in buffer
                pagesInBuffer.clear();
                eos = true;
                break;
            }
            if(counter < numBuff){
                pagesInBuffer.add(incomingPage);
                counter++;
            }
            else{
                index++;
                //sort the pagesInBuffer + write out
                internalSort(pagesInBuffer, index);
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
        ArrayList<Tuple> tupInRun = new ArrayList<Tuple>();
        Tuple currTup;
        for(Batch b : pagesInBuffer){
            //add all the tuples in page into tuples in the run
            for (int j = 0; j < batchSize; j++){
                currTup = b.get(j);
                if ((currTup == null) == false){
                    tupInRun.add(currTup);
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
            cntr++; 
            Batch currBatch = iter.next();
            try{
                String fname = "ESrun-" + String.valueOf(fileIndex) + "-page-" + String.valueOf(cntr);
                FileOutputStream file = new FileOutputStream(fname);
                ObjectOutputStream out = new ObjectOutputStream(file);
                out.writeObject(currBatch);
                out.close();
            }
            catch (IOException io){
                System.out.println("Error writing out to temp file for ES round 1 - sorted runs.");
            } 
        }
    }

    public ArrayList<Batch> generateListOfBatches(ArrayList<Tuple> tupInRun, int batchSize){
        ArrayList<Batch> listOfBatches = new ArrayList<Batch>();
        ArrayList<Tuple> oneBatch = new ArrayList<Tuple>();
        //List<Tuple> tempBatch = new ArrayList<Tuple>();
        int ctr = 0;
        for (int i = 0; i < tupInRun.size(); i++){
            while(ctr < batchSize){
                Tuple currTup = tupInRun.get(i);
                ctr ++;
                oneBatch.add(currTup); 
            }
            Batch newBatch = createBatch(oneBatch);
            listOfBatches.add(newBatch);
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

}

class tupComparator implements Comparator<Tuple>{
    //taken from tuple class
    /**
     * Comparing tuples in different tables with multiple conditions, used for join condition checking
     **/
    //list of indices of attributes to sort by
    List<Integer> attrIndex = new ArrayList<Integer>();

    //constructor: input: array
    public tupComparator(int[] attrIndexList){
        //convert array into arraylist to use it in compare
        Collections.addAll(attrIndex, attrIndexList);
    }
    public static int compare(Tuple left, Tuple right, ArrayList<Integer> attrIndex) {
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
