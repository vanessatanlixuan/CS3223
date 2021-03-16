package qp.operators;

import java.util.ArrayList;
import qp.utils.*;


public class OrderBy extends ExternalSort {
    Operator base;
    ArrayList<Attribute> as;
    int order;
    public OrderBy(Operator base, ArrayList<Attribute> as, int order){
        super(base, as, order);
    }
}
