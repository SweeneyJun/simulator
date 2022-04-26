package sosp.main;

import java.io.FileInputStream;
import java.util.*;

import sosp.algorithm.Algorithm;
import sosp.algorithm.Max3Reducer;
import sosp.jobs.Job;
import sosp.jobs.MapTask;
import sosp.jobs.ReduceTask;
import sosp.network.Coflow;
import sosp.network.Flow;
import sosp.network.Macroflow;

public class SeparateTraffic {

    @SuppressWarnings("unchecked")
    public static Job[] loadFromFile(String traffic){
        assert(Settings.nHosts>0);
        assert(Settings.sizeScale>0);
        assert(Settings.timeScale>0);

        // 存算分离 且 远端存储抽象为单节点的情况下, 不再需要搞这个本地机器了
//        ArrayList<Integer> dfs = new ArrayList<Integer>();  // 后续用来打乱生成input文件所在位置的数组
//        for(int i = Settings.nHosts; i < Settings.nHosts + Settings.nStorageHosts; ++i){ // 前nHosts个机器是计算节点, 后nStorageHosts是存储节点
//            dfs.add(i % (Settings.nHosts + Settings.nStorageHosts));
//        }

        ArrayList<Integer> queuePick = new ArrayList<>(); // 用来从多个JobQueue中随机选一个的Queue的数组
        for (int i = 0; i < TestSunderer.jobQueues.size(); ++i){
            queuePick.add(i);
        }

        Job[] jobs = null; // 最后要返回的读取的Job数组
        Scanner cin = null;
        try{
            cin = new Scanner(new FileInputStream(traffic));
        }
        catch (Exception e) {
            assert(false);
        }

        int jobId = 0; // 后续读取到第几个Job

        Random error = new Random(0); // 设定误差种子, 后续估计运行时间时会使用

        int ii = 0;

        while(cin.hasNextLine()){
            String[] elem = cin.nextLine().split("\\s+"); // "\\s+" match any blank character
            if(elem.length==0){
                continue;
            }
            if(jobs == null){
                jobs = new Job[Integer.parseInt(elem[1])];
                continue;
            }

            int pointer = 0;
            Job job = new Job(jobId);
            Coflow coflow = new Coflow(job);
            job.coflow = coflow;

            job.arriveTime = Long.parseLong(elem[++pointer])/1000.0 * Settings.timeScale; // normalized time: s

            Collections.shuffle(queuePick, Settings.r);
            job.jobQueue = TestSunderer.jobQueues.get(queuePick.get(0));

            // parse the num
            int mappers = Integer.parseInt(elem[++pointer]); // 注意这只是从日志中读取出来的mapper的数量, 并不是最终模拟采用的数量, 后面会自行估计一下inputSize进而再估计实际应该部署的mapper数量
            int reducers = Integer.parseInt(elem[pointer += mappers + 1]); // 这个数量就是和最终模拟中部署的reducer数量一致

            // reduce-tasks
            job.pendingReducerList = new ArrayList<ReduceTask>();
            job.emittedReducerList = new ArrayList<ReduceTask>();
            coflow.macroflows = new Macroflow[reducers]; // 新建Macroflow变量, 接下来的循环就是对其进行初始化以及设置
            for(int i = 0; i < reducers; ++i){
                ReduceTask reduceTask = new ReduceTask(job, i);
                Macroflow mf = new Macroflow(coflow, reduceTask);
                mf.size = Double.parseDouble(elem[++pointer].split(":")[1])*8/1024 * Settings.sizeScale; // in Gb
                coflow.size += mf.size;
                reduceTask.macroflow = coflow.macroflows[i] = mf;
                reduceTask.computationDelay = Settings.reducerCompTimePerMB * mf.size / 8 * 1024; // in MB
                job.pendingReducerList.add(reduceTask);
            }

//            if (Settings.algo instanceof Max3Reducer) { // 一段暂时不打算在存算分离场景中使用的祖传代码
//                Collections.sort(job.pendingReducerList, new Comparator<ReduceTask>() {
//                    @Override
//                    public int compare(ReduceTask arg0, ReduceTask arg1) {
//                        return arg1.macroflow.size == arg0.macroflow.size ? 0 : (arg1.macroflow.size < arg0.macroflow.size ? -1 : 1);
//                    }
//                });
//            }
            job.nReducers = reducers;

            // map-tasks
            job.pendingMapperList = new ArrayList<MapTask>();
            job.emittedMapperList = new ArrayList[Settings.nHosts];
            for(int i = 0; i < Settings.nHosts; ++i){
                job.emittedMapperList[i] = new ArrayList<MapTask>();
            }

            double jobSize; // 看起来是, 若输出中给出的reducer数量不为0, 则会给出MacroFlow的size进而给出Coflow的size, 就用这个coflowSize作为jobSize
            if (reducers != 0) {
                jobSize = coflow.size;
            }
            else{ // 若没给出reducer数量, 上面初始化Macroflow的循环就不会执行, 此时输入会给出jobSize, 用jobSize作为CoflowSize
                jobSize = Double.parseDouble(elem[++pointer]);
                job.coflow.size = jobSize;
            }
            job.mapInputSize = job.coflow.size; // 文章中假设Map阶段的I/O比率是1:1

            int nRealMappers = Algorithm.estimateRealMapperNumber(jobSize/8*1024,mappers); // from Gb to MB
            for(int i = 0; i < nRealMappers; ++i){
                MapTask mapTask = new MapTask(job, i);
                mapTask.computationDelay = Algorithm.estimateMapperTime((jobSize/8*1024)/nRealMappers); // TODO 兆琛: 这个估计很不合理, 后续工作中需要修改提出更严谨的估计计算时间
                mapTask.predictComputationDelay = mapTask.computationDelay *= Math.pow(Settings.sizeEstimationError, 1-2*error.nextDouble());

                mapTask.inputSize = job.coflow.size / nRealMappers; // wcx: 后续用这个size除以分配的带宽来计算输入阶段花费的时间predictInputTime

                //存算分离 且 远端存储抽象为单节点的情况下, 不再需要搞这个本地机器了
                // 打乱存储节点编号数组以生成MapTask的输入所在的机器
//                Collections.shuffle(dfs, Settings.r);
//                mapTask.hdfsHost = new int[Settings.nReplications];
//                for(int k = 0; k < Settings.nReplications; ++k){
//                    mapTask.hdfsHost[k] = dfs.get(k);
//                }
                job.pendingMapperList.add(mapTask);
            }
            job.nMappers = nRealMappers;

            // flows
            for(int i = 0; i < reducers; ++i){
                Macroflow mf = job.coflow.macroflows[i];
                mf.flows = new Flow[Settings.nHosts]; // merge same-source flows
                for(int j = 0; j < Settings.nHosts; ++j){
                    Flow flow = new Flow(mf, coflow, j);
                    flow.sender = j;
                    mf.flows[j] = flow;
                }
            }


            job.estimatedRunningTime = job.nMappers/(double)(Settings.nSlots*Settings.nHosts) * job.pendingMapperList.get(0).computationDelay;
            job.estimatedRunningTime += job.coflow.size/(double)(Settings.nHosts); // 这个祖传估计运行时间似乎没有在调度中被用到, 有没有在其他过程中被使用等看到再说, 先复制到这儿了
            if(Math.abs(Settings.sizeEstimationError-1)>Settings.epsilon)
                job.estimatedRunningTime *= Math.pow(Settings.sizeEstimationError, 1-2*error.nextDouble());
            jobs[jobId++] = job; // 此job处理完毕, 编号递增
            // System.out.printf("Read jobID: %d from log file\n", jobId-1);
        }
        cin.close();
        // System.out.println("i: "+ ii);
        return jobs;
    }
}
