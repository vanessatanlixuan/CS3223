package qp.operators;

import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Schema;
import qp.utils.Tuple;

import java.util.ArrayList;


public class Distinct extends Operator {
    
    // initialize list of attributes to distinct on 
    private ArrayList<Attribute> attr_list; 

    // number of tuples for outbatch i.e number of tuples in each o/p 
    private int batchsize;

    //sorted base 
    //private Operator sortedbase; 
    ExternalSort sortedbase; 
    ExternalSort.TupleSortComparator comparator;

    // number of buffers 
    private int bufferNo; 

    // unsorted base; 
    private Operator base; 

    //index list 
    private int[] indexList; 

    private boolean eos = false; 
    private Batch outbatch;
    private Batch inbatch = null; 
    boolean flag = false; 
    private Tuple lastTuple = null; 
    private Schema schm;

    public Distinct(Operator base, ArrayList<Attribute> attr_list, int type){
        super(type);
        this.base = base; 
        this.attr_list = attr_list;
    }

    public Distinct(Operator base, ArrayList<Attribute> attr_list, int type, int buffno){
        super(type);
        this.base = base; 
        this.attr_list = attr_list;
        this.numBuff = buffno; 
    }

    public Operator getBase() {
        return base;
    }

    public void setBase(Operator base) {
        this.base = base;
    }

    public void setOpType(int type) {
        this.optype = type;
    }

    public int getNumBuff(){
        return numBuff; 
    }

    public int setNumBuff(int no){
        this.numBuff = no; 
    }

    public Schema getSchema() {
        return schema;
    }

    public ArrayList<Attribute> getProjAttr() {
        return attr_list;
    }

    public boolean open() {
        //get batch size 
        int tuplesize = schema.getTupleSize();
        batchsize = Batch.getPageSize() / tuplesize;
        Schema schm = base.getSchema(); 

        //get the index of the attributes 
        for (Attribute attr in attr_list) {
            indexList.add(schm.indexOf(attr)); 
        }

        //perform sorting with external algorithm 
        sortedbase = new ExternalSort(base, attr_list, numBuff, +1); 
        sortedbase.setSchema(schm);

        //sorted base based on attr. open it 
        //check 
        if (!sortedbase.open()) return false;
        return true; 

    }

    public Batch next() {
        int cursor = 0;
        System.out.println("~~~~~~~~~~~~~~~Order By~~~~~~~~~~~~~~")
        if (eos) { //end of file stream, close operator 
            close();
            return null;  
        }
        //check if new page is needed to be fetched 
        if (inbatch == null) { inbatch = sortedbase.next(); }

        outbatch = new Batch(batchsize);
        while(!outbatch.isFull()){
            //check if end of file, when no more pages to be read into buffer 
            if (inbatch.size() <= cursor || inbatch == null){
                eos = true;  
                return outbatch; 
            }
            
            Tuple current = inbatch.get(cursor); 
            //add to outbatch whenever there is no last tuple or when tuples are not equal to each other 
            for (int index : indexList){
                int eq = Tuple.compareTuples(lastTuple, current, index); 
                if (eq != 0){
                    flag = false; 
                    break; 
                } 
            }
            flag = true; 

            if (lastTuple == null || flag = true ){
                outbatch.add(current); 
                lastTuple = current;
            }
            cursor++;    
            
            if (cursor == batchsize){
                inbatch = sortedbase.next(); 
                cursor = 0; 
            }

        } // end while

        return outbatch; 
    }
    
    @Override
    public boolean close() { 
        //return distinct base
        //close 
        return sortedbase.close(); 

    }

    @Override
    public Object clone() {
        Operator newbase = (Operator) base.clone();
        ArrayList<Attribute> newattr_list = new ArrayList<>();
        for (int i = 0; i < attr_list.size(); i++) {
            Attribute attribute = (Attribute) ((Attribute) attr_list.get(i)).clone();
            newattr_list.add(attribute);
        }
        Distinct newdsct = new Distinct(newbase, newattr_list, newtype);
        newdsct.setNumBuff(newbase.getNumBuff());
        newdsct.setSchema(newbase.getSchema());
        return newdsct;
    }

}


