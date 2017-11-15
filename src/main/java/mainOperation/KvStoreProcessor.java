package mainOperation;

import cn.helium.kvstore.common.KvStoreConfig;
import cn.helium.kvstore.processor.Processor;
import cn.helium.kvstore.rpc.RpcServer;
import cn.helium.kvstore.rpc.RpcClientFactory;
import com.sun.org.apache.xpath.internal.operations.Bool;
import hdfsOperation.HdfsOperation;
import org.apache.avro.data.Json;
import org.jboss.netty.util.internal.ConcurrentHashMap;
import org.mortbay.util.ajax.JSON;


import java.io.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Dell on 2017/11/7.
 */
public class KvStoreProcessor implements Processor {
    private static  Map<String, Map<String, String>> store = new HashMap<>();
    private  static String localfilepath = "/opt/localdisk/store.txt";
    private static  String localfilename = "store.txt";
    private static String hdfsurl = KvStoreConfig.getHdfsUrl();
    private static  String hdfsfiledic = "/kvstore";
    private static String hdfsfilepath = hdfsurl+hdfsfiledic+"/store.txt";
    private  static String logpath = "/opt/localdisk/log.txt";
    private static File localfile =  new File(localfilepath);
    private static File locallog = new File(logpath);


    public KvStoreProcessor(){ }
    @Override
    public Map<String,String> get(String key){
        if(key==null){
            return null;
        }

        //首先进行key的格式化
        String keyformat = String.format("%s",key);
        //判断key值是否为空
        if(keyformat.isEmpty()){
            //log**
            save2log("input is empty!");
            return null;
        }
        else {
        	if(store == null) {
        		store = new HashMap<>();
        	}
        	
            {
                //判断内存中是否有key，当内存中没有key时，到磁盘中寻找
                if (!store.containsKey(keyformat)) {
                    //判断disk上是否有key
                    loadfromDisk2Store();
                    //如果当前磁盘不包含Key，则查询其他kvpod磁盘
                    if (!store.containsKey(keyformat)) {
                    	/*判断其他kvPod节点上是否有key
                    	 //1、获取当前kvPodid
                    	  * 2、向其他kvpod传递消息
                    	  * 3、从其他kvpod上获取返回值 
                    	*/
                    	int kvPodId = RpcServer.getRpcServerId();
                    	int kvPodNum = KvStoreConfig.getServersNum();
                    	for(int i = 0;i < kvPodNum; i++) {
                    		if(i!=kvPodId) {//不是当前kvPod
                    			//发送消息
                    			try {
									byte[] queryres = new byte[10240]; //存放查询结果
									queryres = RpcClientFactory.inform(i,keyformat.getBytes());
									if(queryres!=null) {//其他节点查询有结果
										ByteArrayInputStream byteIn = new ByteArrayInputStream(queryres);
										ObjectInputStream in = null;
										try {
						                    in = new ObjectInputStream(byteIn);
						                    return (Map<String, String>) in.readObject(); //返回查询结果
						                } catch (IOException e) {
						                    e.printStackTrace();
						                } catch (ClassNotFoundException e) {
						                	 e.printStackTrace();
						                }
										
									}
								} catch (IOException e) {
									// TODO Auto-generated catch block
									e.printStackTrace();
								}
                    			
                    			
                    		}
                    	}       
                    	//其他节点上也不存在，则需要去hdfs上寻找
//                        //判断hdfs中是否有key
//                        loadFromHdfs2Disk();
//                        loadfromDisk2Store();
//                        //再判断store中是否有key
//                        if (!store.containsKey(keyformat)) {
//                            save2log("cannot find the key!");
//                            return null;
//                        }
                    	return null; //此中hdfs尚未写完整，所以先返回一个null
                    
                    }
                    else { //当前Kvpod上存在key
                    return store.get(keyformat);
                    }
                }
                else { //当前内存中有key
                return store.get(keyformat);
                }
            }
//            else{
//                store = new HashMap<>();          
//                loadFromHdfs2Disk();
//                loadfromDisk2Store();
//                //再判断store中是否有key
//                if (!store.containsKey(keyformat)) {
//                    save2log("cannot find the key!");
//                    return null;
//                }
//                return store.get(keyformat);
//            }
        }

    }
  
	@Override
    public synchronized boolean put(String key,Map<String,String> value){
        if(value == null){
            return false;
        }
        //首先json化输入数据
        String inputformat = String.format("%s%s",key,JSON.toString(value));
       
        //将数据存入本地文件中
        savefile2local(inputformat);
        
        //将数据放入store中
        store.put(key,value);
  
        //将数据写入hdfs中
        save2hdfs(inputformat);
        return true;


    }
    @Override
    public synchronized boolean batchPut(Map<String,Map<String,String>> records){
        if(records == null){
            return false;
        }
        //首先json化输入数据
        String inputformat = String.format("%s",JSON.toString(records));      
        //将数据存入本地文件中
        savefile2local(inputformat);
        //将数据放入store中
        store.putAll(records);
        //将数据写入hdfs中
        save2hdfs(inputformat);
        return true;
    }

    @Override
    public byte[] process(byte[] input){
    	//接受其他kvpod发来请求key的消息并处理返回value
        String mykey = new String(input);
        Map<String, String> replymap = this.store.get(mykey);
        //如果没有找到相关的key，返回一个空的数组
        if (replymap == null)
            return null;
        else {
            //把map转换成byte[]数组
            ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
            try {
                ObjectOutputStream out = new ObjectOutputStream(byteOut);
                out.writeObject(replymap);
            } catch (IOException e) {
                e.printStackTrace();
            }
            byte[] replybyte = byteOut.toByteArray();
            return replybyte;
        }
    }
    
    @Override
    public   Map<Map<String,String>,Integer> groupBy(List<String> columns){
        Map<Map<String,String>,Integer> s= null;
        return s;
    }
    @Override
    public int count(Map<String, String> map) {
        return 0;
    }


    public  void loadFromHdfs2Disk(){
        try {
            HdfsOperation hdfs = new HdfsOperation();
            if (localfile.exists()) {
                localfile.delete();
                localfile.createNewFile();
            }
            hdfs.readFile(hdfsfilepath, localfilepath);
            save2log("read file from hdfs to localdisk!");
        }
        catch(Exception  e){
            save2log("load error!");
            e.printStackTrace();
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
        System.out.println("builder:"+builder.toString());
        store = (HashMap<String, Map<String, String>>) JSON.parse(builder.toString());

  //      save2log("read file from localdisk to store!");
        in.close();
        }
        catch(Exception e){
     //        save2log("load error!");
             e.printStackTrace();
        }

    }
    public  Boolean savefile2local(String input){
//        String localfilepath = "";
        try {
            if(!localfile.exists()){
                localfile.createNewFile();
            }
            BufferedOutputStream out = new BufferedOutputStream(new FileOutputStream(localfile), 16);
            String storejson = input;
            out.write(storejson.getBytes());
            out.write("\n".getBytes());
            save2log("save input to localdisk:".concat(input));
            return true;
        }
        catch(Exception e){
            save2log("save error!");
            e.printStackTrace();

            return false;
        }

    }

    public  Boolean save2hdfs(String input){
        HdfsOperation hdfs = new HdfsOperation();
        try{
            hdfs.deleteFile(hdfsfilepath);
            hdfs.copyFromLocal(localfilepath,hdfsurl+hdfsfiledic);
            save2log("save input to hdfs:".concat(input));
            return true;
        }
        catch (Exception e){
            save2log("save error!");
            e.printStackTrace();
            return false;
        }
    }


    public Boolean save2log(String input){
        try {
            if(!locallog.exists()){
                locallog.createNewFile();
            }
            BufferedOutputStream out = new BufferedOutputStream(new FileOutputStream(locallog), 16);
            out.write(input.getBytes());
            out.write("\n".getBytes());
            return true;
        }
        catch(Exception e){
            e.printStackTrace();
            return false;
        }
    }

}
