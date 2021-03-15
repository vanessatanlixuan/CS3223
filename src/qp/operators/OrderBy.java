package qp.operators;

import java.util.ArrayList;
import qp.utils.*;


public class OrderBy extends ExternalSort {
    Operator base;
    ArrayList<Attribute> as;
    int type;
    int order;
    public OrderBy(Operator base, ArrayList<Attribute> as, int type, int order){
        super(base, as, type, order);
    }
}
