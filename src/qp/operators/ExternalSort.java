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
    Arraylist<Attribute> attrset; //the attributes that we are sorting on
    int batchSize; //the size of every output page

    Batch inBatch; //the page containing the input
    Batch outBatch; //the page containing the output

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

    public boolean open() {
        /** set number of tuples per batch **/
        int tuplesize = schema.getTupleSize(); // get size (num bytes) of tuple of the relation
        batchsize = Batch.getPageSize() / tuplesize; // get size (num bytes) of page of the relation

        if (!base.open()) return false;

        Schema baseSchema = base.getSchema();
        attrIndex = new int[attrset.size()]; //create an int array consisting of indices of req'd attributes
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
        
    }

}
