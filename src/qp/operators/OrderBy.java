package qp.operators;

import java.util.ArrayList;
import qp.utils.Attribute;

public class OrderBy extends ExternalSort {
    public OrderBy(Operator base, ArrayList<Attribute> as, int type, int order){
        super(base, as, type, order);
    }
}
