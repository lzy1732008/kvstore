package kvstore;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.jboss.netty.util.internal.ConcurrentHashMap;
import org.mortbay.util.ajax.JSON;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import cn.helium.kvstore.common.KvStoreConfig;

public class lcoaltest {
    private static  Map<String, Map<String, String>> store = new HashMap<>();
    private  static String localfilepath = "D:\\test\\store.txt";
    private  static String localdirpath = "D:\\test\\dir";
    private static  String localfilename = "store.txt";
    private static File localfile =  new File(localfilepath);

	public static void main(String args[]) {
		Map<String,Map<String,String>>  batch_put = new HashMap<String,Map<String,String>>();
		for(int i=0;i<10;i++) {
			String key = i+"";
			String key1 = "name"+i;
			String value2 = i+"B";
			Map<String,String> map = new HashMap<String,String>();
			map.put(key, value2);
		//	store.put(key, map);
			 String inputformat = String.format("%s%s",key,JSON.toString(map));
			savefile2local(inputformat);
		}
		store = new HashMap<>();
	    loadfromDiskAllFile2Store();
	   
	    for(String key:store.keySet()) {
	    	Map<String, String> value = store.get(key);
	    	System.out.println(key+":"+value);
	    }
		System.out.println(store.size());
		
		
		
		
//		loadfromDisk2Store();
//		System.out.println(store.containsKey("1"));
		
		
		
	}
	

    public synchronized boolean put(String key,Map<String,String> value){
		if(value == null||key == null){
            return false;
        }
        //首先json化输入数据
        String inputformat = String.format("%s%s",key,JSON.toString(value));
       
        //将数据放入store中
        store.put(key,value);
        
        //将数据存入本地文件中
        savefile2local(inputformat);
        
        //将数据写入hdfs中
//        save2hdfs(inputformat);
        return true;


    }
	
	
    public static  Boolean savefile2local(String input){
    	 try {
         	
         	//若本地文件没有产生则创建文件
             if(!localfile.exists()){
                 localfile.createNewFile();
             }
             BufferedOutputStream out = new BufferedOutputStream(new FileOutputStream(localfile,true), 16);
             String storejson = input;
             out.write(storejson.getBytes());
             out.write("\n".getBytes());
   //          save2log("save input to localdisk:".concat(input));
             return true;
         }
         catch(Exception e){
  //           save2log("save error!");
             e.printStackTrace();

             return false;
         }
  }

    public static void putMap2Map(Map<String,Map<String,String>> t1,Map<String,Map<String,String>> t2) {
    	for(String key:t1.keySet()) {
    	   Map<String,String> value = t1.get(key);
    	   t2.put(key, value);
    	}
    }
    
    
    public static void loadDiskfile2Store(String localfilepath) {
        try{
        BufferedInputStream in = new BufferedInputStream(new FileInputStream(localfilepath));
        StringBuffer builder = new StringBuffer();
        int len;
        byte[] bytes = new byte[10240];
        while ((len = in.read(bytes)) != -1) {
           builder.append(new String(bytes, 0, len));
        }
        System.out.println("builder:"+builder.toString());
//        store = (HashMap<String, Map<String, String>>) JSON.parse(builder.toString());
//        
        
        Gson gson = new Gson();
		Map<String,Map<String,String>> t = gson.fromJson(builder.toString(), new TypeToken<Map<String,Map<String,String>>>(){}.getType());
		if(t!=null&&t.size()>0) {
		putMap2Map(t,store);
		}
  //      save2log("read file from localdisk to store!");
        in.close();
        }
        catch(Exception e){
     //        save2log("load error!");
             e.printStackTrace();
        }
    }
    
    /*
     * 获取文件夹下的所有文件
     */
    public static List<String> getDiskDirFilePath(String dir) {
    	List<String> pathList = new ArrayList<>();
    	File dirFile = new File(dir) ;
    	if(!dirFile.exists()) {
    		return null;
    	}
    	File[] filelist = dirFile.listFiles();
    	for(File f:filelist) {
    		pathList.add(f.getPath());
    	}
    	return pathList;
    }
   
    /*
     * 将磁盘所有存放数据的文件都存进store中
     */
    public static  void loadfromDiskAllFile2Store(){
        //首先获取put对应的那个store.txt文件的内容
    	loadDiskfile2Store(localfilepath);
    	//然后获取batch_put对应的内容
    	String dirpath =localdirpath;//临时变量，后面需要改为全局变量****************************
    	List<String> pathList = new ArrayList<>();
    	pathList =  getDiskDirFilePath(dirpath);
    	for(String path:pathList) {
    		loadDiskfile2Store(path);
    	}
    }


}
