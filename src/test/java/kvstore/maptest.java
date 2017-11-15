package kvstore;

import java.util.HashMap;
import java.util.Map;

public class maptest {
   public static void main(String args[]) {
	   Map<String,Map<String,String>> store = new HashMap<>();
	   System.out.println(store.containsKey("1"));
   }
}
