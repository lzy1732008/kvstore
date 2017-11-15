package mainOperation;

import cn.helium.kvstore.common.KvStoreConfig;
import cn.helium.kvstore.common.Parameters;
import cn.helium.kvstore.processor.Processor;
import cn.helium.kvstore.rest.RestfulService;
import cn.helium.kvstore.rpc.RpcServer;
import com.beust.jcommander.JCommander;

/**
 * Created by Dell on 2017/11/8.
 */
public class Main {
    public static void main(String[] args) throws InterruptedException {
        Parameters parameters = new Parameters();
        new JCommander(parameters, args);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            RestfulService.stop();
            RpcServer.stop();
        }));
        startServer(parameters);
    }

    private static void startServer(Parameters parameters) {
        try {
            Class e = Class.forName(parameters.processorClass);
            Processor processor = (Processor) e.getConstructor(new Class[0]).newInstance(new Object[0]);
            RestfulService.startService(processor, parameters.restPort);
            KvStoreConfig.loadConfig(parameters);
            RpcServer.startServer(parameters.rpcId, parameters.rpcThreadNum, processor);
            Thread.currentThread().join();
        } catch (Exception var3) {
            var3.printStackTrace();
        }
    }
}
