package qp.operators.blocknestedjoin;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

import qp.operators.Operator;
import qp.utils.Batch;

public class RightTableGenerator {
    private static int fileID = 0;
    private static String tableName = "";

    public static boolean createRightTable(Operator left, Operator right) {
        Batch rightPage;

        // Materializes the right table for the algorithm to perform.
        if (!right.open()) {
            return false;
        }

        /*
         * If the right operator is not a base table, then materializes the intermediate result
         * from right into a file.
         */
        fileID++;
        tableName = "BNJtemp-" + fileID;
        try {
            ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(tableName));
            rightPage = right.next();
            while (rightPage != null) {
                out.writeObject(rightPage);
                rightPage = right.next();
            }
            out.close();
        } catch (IOException io) {
            System.out.println("BlockNestedJoin: writing the temporary file error");
            return false;
        }

        if (!right.close()) {
            return false;
        }

        return left.open();

    }

    public static String getTableName() {
        return tableName;
    }
}
