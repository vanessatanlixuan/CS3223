/**
 * Enumeration of join algorithm types
 * Change this class depending on actual algorithms
 * you have implemented in your query processor
 **/

package qp.operators;

public class JoinType {

    public static final int NESTEDJOIN = 0;
    public static final int BLOCKNESTEDJOIN = 1;
    public static final int SORTMERGEJOIN = 2;
    public static final int HASHJOIN = 3;

    public static int numJoinTypes() {
        return 1;
    }
}
