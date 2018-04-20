package edu.uci.ics.tippers.operators;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by peeyush on 3/5/17.
 */
public class Operator {

    public Operator() {

    }

    public static Object pathNavigation(JSONObject jsonObject, String path, boolean field) throws JSONException {
        String[] tokens = path.split("\\.");
        int numTokens = tokens.length;
        Object currentObject = jsonObject;
        int i=0;
        String currentToken = tokens[0];
        if (field && !jsonObject.has(currentToken)) {
            i += 1;
            currentToken += "." + tokens[1];
        }

        while (i < numTokens) {
            currentObject = ((JSONObject)currentObject).get(currentToken);
            i += 1;
            if (i >= numTokens)
                break;
            currentToken = tokens[i];
        }
        return currentObject;
    }

    public static Boolean lt(Object leftOperand, Object rightOperand) {
        if (rightOperand instanceof Long)
            return ((Integer)leftOperand) < ((Long)rightOperand).intValue();
        if (rightOperand instanceof Double)
            return ((Integer) leftOperand) < ((Double)rightOperand).intValue();
        if(rightOperand instanceof Integer)
            return (Integer)leftOperand < (Integer)rightOperand;
        if (rightOperand instanceof String) {
            try {
                DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                Date leftDate = new Date((String)leftOperand);
                Date rightDate = df.parse((String)rightOperand);
                return leftDate.compareTo(rightDate) < 0 ;
            } catch (Exception e) {
                return false;
            }
        }

        return false;
    }

    public static Boolean gt(Object leftOperand, Object rightOperand) {
        if (rightOperand instanceof Long)
            return ((Integer)leftOperand) > ((Long)rightOperand).intValue();
        if (rightOperand instanceof Double)
            return ((Integer) leftOperand)> ((Double)rightOperand).intValue();
        if(rightOperand instanceof Integer)
            return (Integer)leftOperand > (Integer)rightOperand;
        if (rightOperand instanceof String) {
            try {
                DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                Date leftDate = new Date((String)leftOperand);
                Date rightDate = df.parse((String)rightOperand);
                return leftDate.compareTo(rightDate) > 0 ;
            } catch (Exception e) {
                return false;
            }
        }
        return false;
    }

    public static Boolean le(Object leftOperand, Object rightOperand) {
        if (rightOperand instanceof Long)
            return ((Integer)leftOperand) <= ((Long)rightOperand).intValue();
        if (rightOperand instanceof Double)
            return ((Integer) leftOperand) <= ((Double)rightOperand).intValue();
        if(rightOperand instanceof Integer)
            return (Integer)leftOperand <= (Integer)rightOperand;
        if (rightOperand instanceof String) {
            try {
                DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                Date leftDate = new Date((String)leftOperand);
                Date rightDate = df.parse((String)rightOperand);
                return leftDate.compareTo(rightDate) <= 0 ;
            } catch (Exception e) {
                return false;
            }
        }
        return false;
    }

    public static Boolean ge(Object leftOperand, Object rightOperand) {
        if (rightOperand instanceof Long)
            return ((Integer)leftOperand) >= ((Long)rightOperand).intValue();
        if (rightOperand instanceof Double)
            return ((Integer) leftOperand) >= ((Double)rightOperand).intValue();
        if(rightOperand instanceof Integer)
            return (Integer)leftOperand >= (Integer)rightOperand;
        if (rightOperand instanceof String) {
            try {
                DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                Date leftDate = new Date((String)leftOperand);
                Date rightDate = df.parse((String)rightOperand);
                return leftDate.compareTo(rightDate) >= 0 ;
            } catch (Exception e) {
                return false;
            }
        }
        return false;
    }

    public static Object plus(Object leftOperand, Object rightOperand) {
        if (rightOperand instanceof Long)
            if(leftOperand instanceof Long)
                return (Long)leftOperand + (Long)rightOperand;
            else
                return (Integer)leftOperand + ((Long)rightOperand).intValue();
        if (rightOperand instanceof Double)
            return (Double)leftOperand + ((Double)rightOperand).intValue();
        if(rightOperand instanceof Integer)
            return (Integer)leftOperand + (Integer)rightOperand;
        return false;
    }

    public static Object minus(Object leftOperand, Object rightOperand) {
        if (rightOperand instanceof Long)
            if(leftOperand instanceof Long)
                return (Long)leftOperand - (Long)rightOperand;
            else
                return (Integer)leftOperand - ((Long)rightOperand).intValue();
        if (rightOperand instanceof Double)
            return (Double)leftOperand - ((Double)rightOperand).intValue();
        if(rightOperand instanceof Integer)
            return (Integer)leftOperand - (Integer)rightOperand;
        return false;
    }

    public static Object multiply(Object leftOperand, Object rightOperand) {
        if (rightOperand instanceof Long)
            if(leftOperand instanceof Long)
                return (Long)leftOperand * (Long)rightOperand;
            else
                return (Integer)leftOperand * ((Long)rightOperand).intValue();
        if (rightOperand instanceof Double)
            return (Double)leftOperand * ((Double)rightOperand).intValue();
        if(rightOperand instanceof Integer)
            return (Integer)leftOperand * (Integer)rightOperand;
        return false;
    }

    public static Object divide(Object leftOperand, Object rightOperand) {
        if (rightOperand instanceof Long)
            if(leftOperand instanceof Long)
                return (Long)leftOperand / (Long)rightOperand;
            else
                return (Integer)leftOperand / ((Long)rightOperand).intValue();
        if (rightOperand instanceof Double)
            return (Double)leftOperand / ((Double)rightOperand).intValue();
        if(rightOperand instanceof Integer)
            return (Integer)leftOperand / (Integer)rightOperand;
        return false;
    }

    public static Object mod(Object leftOperand, Object rightOperand) {
        if (rightOperand instanceof Long)
            if(leftOperand instanceof Long)
                return (Long)leftOperand % (Long)rightOperand;
            else
                return (Integer)leftOperand % ((Long)rightOperand).intValue();
        if (rightOperand instanceof Double)
            return (Double)leftOperand % ((Double)rightOperand).intValue();
        if(rightOperand instanceof Integer)
            return (Integer)leftOperand % (Integer)rightOperand;
        return false;
    }

    public static Boolean eq(Object leftOperand, Object rightOperand) {
        if (leftOperand instanceof String)
            return ((String)leftOperand).equals((String)rightOperand);
        if (rightOperand instanceof Long)
            return ((Integer)leftOperand) == ((Long)rightOperand).intValue();
        if (rightOperand instanceof Double)
            return ((Integer)leftOperand) == ((Double)rightOperand).intValue();

        return rightOperand == leftOperand;
    }

    public static Boolean ne(Object leftOperand, Object rightOperand) {
        if (leftOperand instanceof String)
            return !(((String)leftOperand).equals((String)rightOperand));
        if (rightOperand instanceof Long)
            return !(((Integer)leftOperand) == ((Long)rightOperand).intValue());
        if (rightOperand instanceof Double)
            return !(((Integer)leftOperand) == ((Double)rightOperand).intValue());

        return rightOperand != leftOperand;
    }

    public static Boolean and(Object leftOperand, Object rightOperand) {
        return (Boolean) leftOperand && (Boolean)rightOperand;
    }

    public static Boolean or(Object leftOperand, Object rightOperand) {
        return (Boolean)leftOperand || (Boolean)rightOperand;
    }

    public static Boolean like(Object leftOperand, Object rightOperand) {
        if(((String)rightOperand).startsWith("%"))
            rightOperand=((String)rightOperand).substring(1);
        if(((String)rightOperand).endsWith("%"))
            rightOperand=((String)rightOperand).substring(0,((String)rightOperand).length()-1);
        return ((String)leftOperand).contains((String)rightOperand);
    }

    public static Boolean in(Object value, JSONArray list) throws JSONException {
        for(int i=0;i<list.length();i++)
            if(list.get(i).equals(value))
                return true;



        return false;
    }

    public static Boolean in(JSONArray leftList, JSONArray rightList) {
        return false;
    }

    public static Object getIndex(int index, JSONArray list) throws JSONException {
        return list.get(index);
    }


}
