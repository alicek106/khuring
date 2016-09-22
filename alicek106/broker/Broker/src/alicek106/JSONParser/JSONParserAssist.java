package alicek106.JSONParser;

import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

public class JSONParserAssist {
    static public JSONObject parseString(String jsonString){
    	try {

    	    Object obj = JSONValue.parseWithException(jsonString);

    	    JSONObject jobj = (JSONObject)obj;

    	    return jobj;

    	} catch (Exception e) {

    		e.printStackTrace();

    	}
    	
    	return null;
    }
    
}
