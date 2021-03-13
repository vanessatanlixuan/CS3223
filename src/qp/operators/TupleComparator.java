import qp.utils.Tuple;
import qp.utils.Attribute;

public class TupleComparator {

    /**
     * Compares two tuples based on the join attribute.
     *
     * @param tuple1 is the first tuple.
     * @param tuple2 is the second tuple.
     * @return an integer indicating the comparision result, compatible with the {@link java.util.Comparator} interface.
     */
    public static int compareTuples(int type, Tuple tuple1, Tuple tuple2, int index1, int index2) {
        Object value1 = tuple1.dataAt(index1);
        Object value2 = tuple2.dataAt(index2);

        switch (type) {
            case Attribute.INT:
                return Integer.compare((int) value1, (int) value2);
            case Attribute.STRING:
                return ((String) value1).compareTo((String) value2);
            case Attribute.REAL:
                return Float.compare((float) value1, (float) value2);
            default:
                return 0;
        }
    }
}