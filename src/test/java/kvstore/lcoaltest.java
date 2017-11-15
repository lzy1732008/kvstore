package kvstore;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.HashMap;
import java.util.Map;

import org.jboss.netty.util.internal.ConcurrentHashMap;
import org.mortbay.util.ajax.JSON;

import cn.helium.kvstore.common.KvStoreConfig;

public class lcoaltest {
    private static  Map<String, Map<String, String>> store = new HashMap<>();
    private  static String localfilepath = "D:\\test\\store.txt";
    private static  String localfilename = "store.txt";
    private static File localfile =  new File(localfilepath);

	public static void main(String args[]) {
//		Map<String,Map<String,String>>  batch_put = new HashMap<String,Map<String,String>>();
		for(int i=0;i<10000;i++) {
			String key = i+"";
			String value1 = i+"A";
			String value2 = i+"B";
			Map<String,String> map = new HashMap<String,String>();
			map.put(value1, value2);
			store.put(key, map);
		}
		String input = String.format("%s", JSON.toString(store));
		savefile2local(input);
		store = null;
		
		loadfromDisk2Store();
		System.out.println(store.containsKey("1"));
		
	}
    public static  Boolean savefile2local(String input){
//      String localfilepath = "";
      try {
          if(!localfile.exists()){
              localfile.createNewFile();
          }
          BufferedOutputStream out = new BufferedOutputStream(new FileOutputStream(localfile), 16);
          String storejson = input;
          out.write(storejson.getBytes());
          out.write("\n".getBytes());
    //      save2log("save input to localdisk:".concat(input));
          return true;
      }
      catch(Exception e){
 //         save2log("save error!");
          e.printStackTrace();

          return false;
      }

  }
    public static  void loadfromDisk2Store(){
        try{
        BufferedInputStream in = new BufferedInputStream(new FileInputStream(localfile));
        StringBuffer builder = new StringBuffer();
        int len;
        byte[] bytes = new byte[10240];
        while ((len = in.read(bytes)) != -1) {
           builder.append(new String(bytes, 0, len));
        }
        store = (HashMap<String, Map<String, String>>) JSON.parse(builder.toString());
  //      save2log("read file from localdisk to store!");
        in.close();
        }
        catch(Exception e){
     //        save2log("load error!");
             e.printStackTrace();
        }

    }


}
