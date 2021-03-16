package qp.operators;

import java.util.ArrayList;
import qp.utils.*;


public class OrderBy extends ExternalSort {
    Operator base;
    ArrayList<Attribute> as;
    int numBuff;
    int order;
    public OrderBy(Operator base, ArrayList<Attribute> as, int numBuff, int order){
        super(base, as, numBuff, order);
    }
}
