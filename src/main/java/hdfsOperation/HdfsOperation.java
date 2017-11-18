package hdfsOperation;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import cn.helium.kvstore.common.KvStoreConfig;

import java.io.*;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
/**
 * Created by Dell on 2017/11/8.
 */
public class HdfsOperation {
	public static String uri = KvStoreConfig.getHdfsUrl();
//  private static String hdfsurl = KvStoreConfig.getHdfsUrl();
  //列出目录下所有文件
  public void list(String srcPath) {
      Configuration conf = new Configuration();
      conf.set("fs.hdfs.impl","org.apache.hadoop.hdfs.DistributedFileSystem");
//       LOG.info("[Defaultfs] :" +conf.get("fs.default.name"));
//       conf.set("hadoop.job.ugi","app,app");   //It is not necessary for the default user.
      FileSystem fs;
      try {
          fs= FileSystem.get(conf);
          RemoteIterator<LocatedFileStatus> rmIterator = fs.listLocatedStatus(new Path(srcPath));
          while (rmIterator.hasNext()) {
              Path path = rmIterator.next().getPath();
              if(fs.isDirectory(path)){
    //              LOG.info("-----------DirectoryName: "+path.getName());
              }
              else if(fs.isFile(path)){
//                  LOG.info("-----------FileName: "+path.getName());
              }
          }
      }catch (IOException e) {
//          LOG.error("list fileSysetm object stream.:" , e);
          new RuntimeException(e);
      }
  }
  public void readFile(String file,String localpath){
      Configuration conf = new Configuration();
      conf.set("fs.hdfs.impl","org.apache.hadoop.hdfs.DistributedFileSystem");
      FileSystem fs;
      try {
          fs= FileSystem.get(URI.create(file),conf);
          Path path = new Path(file);
          if(!fs.exists(path)){
     //         LOG.warn("file'"+ file+"' doesn't exist!");
              return ;
          }
          FSDataInputStream in = fs.open(path);
      //    String filename = file.substring(file.lastIndexOf('/') + 1, file.length());
     //     String filepath = localpath+"/"+filename;
          String filepath = localpath;
          OutputStream out = new BufferedOutputStream(new FileOutputStream(
                  new File(filepath)));

          byte[] b = new byte[10240];
          int numBytes = 0;
          while ((numBytes = in.read(b)) > 0) {
              out.write(b,0, numBytes);
          }
          in.close();
          out.close();
          fs.close();
      }catch (IOException e) {
//          LOG.error("ifExists fs Exception caught! :" , e);
          new RuntimeException(e);
      }
  }
  public  Map<String,Map<String,String>> readFileToStore(String file) {
	  Configuration conf = new Configuration();
      conf.set("fs.hdfs.impl","org.apache.hadoop.hdfs.DistributedFileSystem");
      FileSystem fs;
      try {
          fs= FileSystem.get(URI.create(file),conf);
          Path path = new Path(file);
          if(!fs.exists(path)){
              return null;
          }
          FSDataInputStream in = fs.open(path);
          BufferedReader reader = new BufferedReader(new InputStreamReader(in));
          String line;
  		  Map<String,Map<String,String>> mapList = new HashMap<String,Map<String,String>>();
  		  while((line=reader.readLine())!=null) {
  			Gson gson = new Gson();
  			mapList.putAll((Map<? extends String, ? extends Map<String, String>>) gson.fromJson(line, new TypeToken<Map<String,Map<String,String>>>(){}.getType()));
  		}
  		  reader.close();
  		  in.close();
          fs.close();
          return mapList;
      }catch (IOException e) {
//          LOG.error("ifExists fs Exception caught! :" , e);
          new RuntimeException(e);
      }
      return null;
  }
  
  public void copyFromLocal (String source, String dest) {

      Configuration conf = new Configuration();
      conf.set("fs.hdfs.impl","org.apache.hadoop.hdfs.DistributedFileSystem");
      FileSystem fs;
      try {
          fs= FileSystem.get(URI.create(uri),conf);
          Path srcPath = new Path(source);

          Path dstPath = new Path(dest);
          // Check if the file alreadyexists
          if (!(fs.exists(dstPath))) {
          	  HdfsOperation hdfs = new HdfsOperation();
          	  hdfs.mkdirFile(dest);
   //           LOG.warn("dstPathpath doesn't exist" );
   //           LOG.error("No such destination " + dstPath);
             
          }

          // Get the filename out of thefile path
          String filename = source.substring(source.lastIndexOf('/') + 1, source.length());

          try{
              //if the file exists in thedestination path, it will throw exception.
//                                 fs.copyFromLocalFile(srcPath,dstPath);
              //remove and overwrite files withthe method
              //copyFromLocalFile(booleandelSrc, boolean overwrite, Path src, Path dst)
              fs.copyFromLocalFile(false, true, srcPath, dstPath);
//               LOG.info("File " + filename + "copied to " + dest);
          }catch(Exception e){
//              LOG.error("copyFromLocalFile exception caught!:" , e);
              new RuntimeException(e);
          }finally{
              fs.close();
          }
      }catch (IOException e1) {
//          LOG.error("copyFromLocal IOException objectstream. :" ,e1);
          new RuntimeException(e1);
      }
  }
  public void deleteFile(String file)  {
      Configuration conf = new Configuration();
      conf.set("fs.hdfs.impl","org.apache.hadoop.hdfs.DistributedFileSystem");
      FileSystem fs;
      try {
          fs= FileSystem.get(URI.create(file),conf);

          Path path = new Path(file);
          if (!fs.exists(path)) {
//              LOG.info("File " + file + " does not exists");
              return;
          }
                                   /*
                                    * recursively delete the file(s) if it is adirectory.
                                    * If you want to mark the path that will bedeleted as
                                    * a result of closing the FileSystem.
                                    *  deleteOnExit(Path f)
                                    */
          fs.delete(new Path(file), true);
          fs.close();
      }catch (IOException e) {
//           LOG.error("deleteFile Exception caught! :" , e);
          new RuntimeException(e);
      }

  }
  /*
   * 创建新文件夹
   */
  public Boolean mkdirFile(String dir) {
  	Configuration conf = new Configuration();
      conf.set("fs.hdfs.impl","org.apache.hadoop.hdfs.DistributedFileSystem");
      FileSystem fs;
      try {
          fs= FileSystem.get(URI.create(dir),conf);
          fs.mkdirs(new Path(dir));
          fs.close();
          return true;
      }catch (IOException e) {
//           LOG.error("deleteFile Exception caught! :" , e);
          new RuntimeException(e);
      }
      return false;
  }
  /*
   * 创建新文件
   */
  public void createFile(String file) {
  	Configuration conf = new Configuration();
      conf.set("fs.hdfs.impl","org.apache.hadoop.hdfs.DistributedFileSystem");
      FileSystem fs;
      try {
          fs= FileSystem.get(URI.create(file),conf);
          fs.create(new Path(file));
          fs.close();
      }catch (IOException e) {
//           LOG.error("deleteFile Exception caught! :" , e);
          new RuntimeException(e);
      }
  }
  public void writeFile(String file,String content) {
	  Configuration conf = new Configuration();
      conf.set("fs.hdfs.impl","org.apache.hadoop.hdfs.DistributedFileSystem");
      FileSystem fs;
      try {
          fs= FileSystem.get(URI.create(file),conf);
          Path path = new Path(file);
          if(!fs.exists(path)) {
        	 fs.createNewFile(path);
          }
          FSDataOutputStream os = fs.append(path);
          os.write(content.getBytes());
          os.flush();
          os.close();
          
          
      }catch (IOException e) {
//           LOG.error("deleteFile Exception caught! :" , e);
          new RuntimeException(e);
      }
  }

}
