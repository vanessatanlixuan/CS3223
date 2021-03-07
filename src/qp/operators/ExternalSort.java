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

public class ExternalSort extends Operator {
    
    Operator base; //the relation (table) that we need to sort
    ArrayList<Attribute> attrset; //the attributes that we are sorting on
    int batchSize; //the size of every output page
    int numBuff; //number of buffer pages

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
        generateSortedRuns();
    }

    public void generateSortedRuns() { 
        boolean eos = false; //have we reached the end of the table yet?
        int index = 0; //index for first filename
        //numRounds = number of internal sort rounds needed 
        //1. load as many batches as possible into buffer

        ArrayList<Batch> pagesInBuffer = new ArrayList<Batch>();
        
        Batch incomingPage;

        int counter = 0;
        while (eos == false){ //while there are still pages in table(base)
            incomingPage = base.next();
            if(incomingPage == null){
                index ++;
                //sort the pages already in Buffer + write out
                internalSort(pagesInBuffer, index);
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
    }

    public void internalSort(ArrayList<Batch> pagesInBuffer, int fileIndex){
        //input arraylist of pages in the buffer
        //output: write out pages of sorted tuples
        ArrayList<Tuple> tupInRun = new ArrayList<Tuple>();
        Tuple currTup;
        for(Batch b : pagesInBuffer){
            //add all the tuples in page into tuples in the run
            tupInRun.addAll(b);
        }
        //now we have tupInRun list containing all the tuples to be sorted in a run
        
    }

}
