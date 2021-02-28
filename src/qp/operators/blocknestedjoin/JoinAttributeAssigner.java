package qp.operators.blocknestedjoin;

import java.util.HashMap;

import qp.operators.Operator;
import qp.utils.Attribute;
import qp.utils.Condition;

public class JoinAttributeAssigner {

    public static HashMap<AttributeKey,Integer> getJoinAttributes(Condition condition, Operator left, Operator right) {
        Attribute leftAttr = condition.getLhs();
        Attribute rightAttr = (Attribute) condition.getRhs();
        HashMap<AttributeKey,Integer> attributes = new HashMap<>();
        attributes.put(AttributeKey.LEFT, left.getSchema().indexOf(leftAttr));
        attributes.put(AttributeKey.RIGHT, right.getSchema().indexOf(rightAttr));
        return attributes;
    }

    public enum AttributeKey {
        LEFT,
        RIGHT
    }
}
