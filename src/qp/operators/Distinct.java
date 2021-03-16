package qp.operators;

import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Schema;
import qp.utils.Tuple;

import java.util.ArrayList;

public class Distinct extends Operator {
    
    // initialize list of attributes to distinct on 
    private ArrayList attr_list; 

    // number of tuples for outbatch i.e number of tuples in each o/p 
    private int batchsize;

    //sorted relation 
    //private Operator sortedrelation; 
    ExternalSort sortedrelation; 

    // number of buffers 
    private int bufferNo; 

    // unsorted relation; 
    private Operator relation; 

    //index list 
    private int[] indexList; 

    private boolean eos = false; 
    private int cursor = 0;
    private Batch outbatch;
    private Batch inbatch = null; 
    private boolean result = false; 
    private Tuple lastTuple = null; 
    private Schema schm;

    public Distinct(Operator relation, ArrayList attr_list){
        super(relation.optype);
        this.relation = relation; 
        this.attr_list = attr_list;
        schm =  relation.getSchema();
    }

    public Operator getBase() {
        return relation;
    }

    public void setBase(Operator relation) {
        this.relation = relation;
    }

    //find out the attribute col 

    public boolean open() {
        //get batch size 
        int tuplesize = schema.getTupleSize();
        batchsize = Batch.getPageSize() / tuplesize;

        //get unsorted relation schema
        //Schema schm = relation.getSchema();

        //get the index of the attributes needed
        for (int i=0; i < attr_list.size(); i++) {
            // attr name, find index on schema, store in index list 
            Attribute at = (Attribute) attr_list.get(i);
            indexList[i] =  Integer.valueOf(schm.indexOf(at));
        }

        //perform sorting with external algorithm 
        sortedrelation = new ExternalSort(relation, attr_list, bufferNo, 1); 
        sortedrelation.setSchema(schm);

        //sorted relation based on attr. open it 
        return sortedrelation.open(); 

    }

    public Batch next() {
        if (eos) { //end of file stream, close operator 
            close(); 
            return null; 
            
        }

        outbatch = new Batch(batchsize);
        //check if new page is needed to be fetched 
        if (inbatch == null) {
            inbatch = sortedrelation.next(); }

        while(!outbatch.isFull()){
            //check if end of file, when no more pages to be read into buffer 
            if (inbatch.size() <= cursor || inbatch == null){
                eos = true;  
                return outbatch; 
            }
            
            for (cursor=0; cursor < batchsize; cursor++){
                Tuple current = inbatch.get(cursor); 
                
                //add to outbatch whenever there is no last tuple or when tuples are not equal to each other 
                if (lastTuple == null){
                    outbatch.add(current); 
                    lastTuple = current;
                }
                //remove duplicates by comparing tuples 
                // using compareTuples(Tuple left, Tuple right, int leftIndex, int rightIndex) from utils/Tuple

                for (int index : indexList){
                    int eq = Tuple.compareTuples(lastTuple, current, index); 
                    if (eq != 0){
                        result = true; 
                        break; 
                    } 
                }

                if (result = true){
                    outbatch.add(current); 
                    lastTuple = current; 
                }

            } 
            if (cursor == batchsize){
                inbatch = sortedrelation.next(); 
                cursor = 0; 
            }

        } // end while

        return outbatch; 
    }

    @Override
    public boolean close() { 
        //return distinct relation
        //close 
        return sortedrelation.close(); 

    }

    public Object clone() {
        Operator newrelation = (Operator) relation.clone();
        ArrayList newattr_list = (ArrayList) attr_list.clone();  
        Distinct newdsct = new Distinct(newrelation, newattr_list);
        newdsct.setSchema(newrelation.getSchema());
        return newdsct;
    }

}


