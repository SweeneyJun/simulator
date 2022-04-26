package sosp.main;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.util.*;

import com.sun.xml.internal.ws.api.config.management.policy.ManagementAssertion;
import sosp.algorithm.Algorithm.HostAndTask;
import sosp.algorithm.Max3Reducer;
import sosp.algorithm.MultiUserMf;
import sosp.jobs.*;
import sosp.network.Coflow;
import sosp.network.Flow;
import sosp.network.Macroflow;

public class TestSunderer{

    public static int parallelism = 0;

    public static double time = 0; // current simulation time
    public static Job[] jobs = null; // all of the jobs
    public static int[] freeSlots = null; //  free slots in each host
    public static double[] freeBw = null;
    public static double totalFreeBw = -1;
    public static double switchFreeBw = -1;
    public static int debugCount = 0;

    public static ArrayList<Job> activeJobs = new ArrayList<Job>(); // arrived and not finished coflows
    public static ArrayList<Job> pendingJobs = new ArrayList<Job>(); // arrived and not finished coflows
    public static ArrayList<ReduceTask> activeReducers = new ArrayList<ReduceTask>(); // emitted and not finished reducers
    public static ArrayList<MapTask> activeMappers = new ArrayList<MapTask>(); // emitted and not finished mappers 更改需求后暂时不用它了? 4.9:好像还是要用...
    public static ArrayList<MapTask> InputMappers = new ArrayList<MapTask>();
    public static ArrayList<Measurement.Throughput> throughput = null;
    public static ArrayList<Double> slot = null;

//    public static ArrayList<MapTask> arrivedMappers = new ArrayList<MapTask>(); // job已经到来, 但仍未执行input的MapTask  因为要先选择job再选择里面的MapTask进行Input
    // 所以不太方便在Scheduler里设置全局的统计

    public static long alCount = 0;
    public static double alTime = 0;

    public static ArrayList<JobQueue> jobQueues = new ArrayList<>();


    public static HostInfo[] hostInfos = null;

    private static PrintWriter scheduleOut = null;

    public static void main(String[] args) throws Exception{
        assert(args.length == 2);
        int ep = Integer.parseInt(args[1]);

        jobQueues.add(new JobQueue("fair", 0.5, 1));
//		jobQueues.add(new JobQueue("fair", 0.5, 0.8));

        PrintWriter cout = new PrintWriter(new FileOutputStream("./result/SLog_" + args[1] + "_" + args[0]));
        for(int j = 0; j < ep; ++j) {
            time = 0;

            Settings.loadFromFile("config.ini", args);//载入配置文件
            initialize(args[0]);

            simulate();
            System.out.printf("Loop Times: %d ScheduleTime:%f  avgTime: %f\n", alCount, alTime, alTime/(alCount));
            // Measurement.OutputCompletionTime2(jobs);



            cout.printf("%d  %f  %f\n", alCount, alTime, alTime/(alCount));

        }
        cout.close();
    }

    private static void initialize(String log) throws FileNotFoundException{
        alCount = 0;
        alTime = 0;
        jobs = SeparateTraffic.loadFromFile(log);
        freeSlots = new int[Settings.nHosts];
        for(int i = 0; i < freeSlots.length; ++i) {
            freeSlots[i] = Settings.nSlots;
        }

        if(Settings.isGaintSwitch) {
            Topology.loadGaint();
        }
        else {
            Topology.loadTwoLayer();
        }

        Topology.loadSeparateGaint();
        freeBw = Topology.getLinkBw(); // 让Scheduler知道各个机器的带宽情况以便后续MapInput阶段调度使用
        totalFreeBw = 0;
        for(int i = 0; i < Settings.nHosts * 2; ++i){
            totalFreeBw += freeBw[i];
        }

        switchFreeBw = Settings.switchBottleFreeBw;

        // System.out.printf("TotalFreeBw: %f\n", totalFreeBw);


        throughput = Measurement.newThroughput();
        // record the slot using ratio during the schedule
        slot = new ArrayList<Double>();
        slot.add(0.0);

        hostInfos = new HostInfo[Settings.nHosts];
        for(int i = 0; i < Settings.nHosts;i ++) {
            hostInfos[i] = new HostInfo(i);
        }
        // scheduleOut = new PrintWriter(new FileOutputStream("SeparateSchedule.txt"));

        parallelism = (int)(Settings.parallelism*Settings.nSlots*Settings.nHosts); // 这几行都是复制的*山
        // System.out.println(parallelism);
        assert(Settings.nPriorities>=0);
        assert(Settings.minTimeStep>0);
        assert(Settings.epsilon>=0); // 这个在部分比较运算时加上, 避免产生浮点数舍入误差

    }

    private static void simulate(){
        int nFinishedJobs = 0;
        Job.nArrivedJobs = 0;
        while(nFinishedJobs < jobs.length){ // 条件成立说明还有没完成的job
            while(Job.nArrivedJobs < jobs.length && jobs[Job.nArrivedJobs].arriveTime <= time + Settings.epsilon){ // 还有可以到来的job, 以及模拟时间追上到达时间时才可以开始调度该job
                debugCount = 0;

                Job job = jobs[Job.nArrivedJobs++];
                Coflow coflow = job.coflow;

                if(activeJobs.size() < parallelism){
                    activeJobs.add(job);
                    job.jobQueue.activeJobs.add(job);
                }
                else{
                    pendingJobs.add(job);
                }
                // System.out.printf("%.3f job %d started\n", time, job.jobId);

                job.notInputMapperList = new ArrayList<MapTask>();
                for(int i = 0; i < job.nMappers; i++){
                    job.notInputMapperList.add(job.pendingMapperList.get(i)); // 供调度算法选择的还未Input的MapTask
                }
            }

            // 2. scheduling tasks (both mapper and reducer)
            // when scheduling the mapTask, handle the input file transmitting process
            while(true){
                alCount += 1;
                // HostAndTask represent the <host, task> pair
                double tempTime = System.currentTimeMillis();
                HostAndTask ht = Settings.algo.allocateHostAndTask();
                alTime += (System.currentTimeMillis() - tempTime);
                if(ht == null){
                    break;
                }
                --freeSlots[ht.host];
                hostInfos[ht.host].freeSlots--;
                if(ht.task instanceof MapTask){
                    MapTask mapper = (MapTask) ht.task;
                    mapper._job.oneMapperBeginInput(ht.host, mapper);
                }
                else{ // instanceof ReduceTask
                    // TODO
                    ReduceTask reducer = (ReduceTask) ht.task;
                    reducer._job.oneReducerStarted(ht.host, reducer);
                    reducer.emit(ht.host, time);
                    activeReducers.add(reducer);

                    hostInfos[ht.host].hostShuffleSize += reducer.macroflow.size;
                    hostInfos[ht.host].shuffleReducer[hostInfos[ht.host].shuffleReducerNum] = reducer;
                    hostInfos[ht.host].shuffleReducerNum += 1;

                    if (HostInfo.hostMaxReducerNum < hostInfos[ht.host].shuffleReducerNum) {
                        HostInfo.hostMaxReducerNum = hostInfos[ht.host].shuffleReducerNum;
                    }

                    Arrays.sort(hostInfos[ht.host].shuffleReducer, 0, hostInfos[ht.host].shuffleReducerNum, new Comparator<ReduceTask>() {
                        @Override
                        public int compare(ReduceTask o1, ReduceTask o2) {
                            if (o1.macroflow._coflow.size != o2.macroflow._coflow.size) {
                                return Double.compare(o1.macroflow._coflow.size, o2.macroflow._coflow.size);
                            }
//							if (o1._job.jobId != o2._job.jobId) {
//								return Integer.compare(o1._job.jobId, o2._job.jobId);
//							}
                            else {
                                return Double.compare(o2.macroflow.size, o1.macroflow.size);
                            }
                        }
                    });
                    hostInfos[ht.host].updateHostInfo();
                    double offset = 0;
                    for (int i = 0; i < hostInfos[ht.host].shuffleReducerNum; i ++) {
                        ReduceTask r = hostInfos[ht.host].shuffleReducer[i];
                        if (r.deadline == -1) {
                            r.deadline = time + r.macroflow.size/Settings.speed*Settings.nSlots;
                        }
                        if (r.predictFinishTime == -1) {
                            r.predictFinishTime = time + r.macroflow.size/Settings.speed;
                            offset += r.predictFinishTime;
                        }
                        else {
                            r.predictFinishTime += offset;
                        }
                    }
                    HostInfo.hostMaxMinCoflowSize = 0;
                    for(int i = 0; i < Settings.nHosts; i ++) {
                        if (hostInfos[i].minCoflowSize > HostInfo.hostMaxMinCoflowSize) {
                            HostInfo.hostMaxMinCoflowSize = hostInfos[i].minCoflowSize;
                        }
                    }
                }
            }

            // 3. transmitting flows
            double nFreeSlotsRatio = countFreestHost_const()/(double)(Settings.nSlots*Settings.nHosts);
            ArrayList<Flow>[] activeFlows =  Settings.algo.getPriority();

            // get Link Bandwidth
//            freeBw = Topology.getLinkBw();
            // work conservation
            MaxMin.getMaxMin(activeFlows, freeBw);
            double step = getSimulationSteps_const();
            Measurement.measureThroughput(throughput, freeBw, time, step);
            slot.add(1-nFreeSlotsRatio);
            time += step; // update current time
            int flag = activeFlows[0].size() + activeFlows[1].size() + activeFlows[2].size() > 0 ? 0 : 1;
            for(ArrayList<Flow> flows:activeFlows){
                for(Flow flow:flows){
                    if(flow.allocatedBw > Settings.epsilon){
                        flag = 1;
                    }
//					System.out.println(flow._macroflow._reducer.host + ":" + leastShuffleSize[flow._macroflow._reducer.host]);
                    flow.sentSize += flow.allocatedBw * step;
//					leastShuffleSize[flow._macroflow._reducer.host] -= flow.allocatedBw * step;
//					assert (leastShuffleSize[flow._macroflow._reducer.host] >= flow._macroflow.size * step * -1);
                    if(flow.sentSize + Settings.epsilon > flow.size) { // gt, not ge
                        flow.Finish(time);

                        // wcx: 在去掉了阶段3的 freeBw = Topology.getLinkBw();这句后, 带宽情况不能作为一次性情况每次获取了, 而是得由SeparateScheduler全局管理
                        // 所以这里每当一个flow结束了以后我们需要归还该flow所占用的带宽
                        for (int node : flow.route) {
                            freeBw[node] += flow.allocatedBw;
                            totalFreeBw += flow.allocatedBw;
                        }
                    }

                }
            }
            // assert (flag == 1);

            // 4(1) finish mappers input
            Iterator<MapTask> itm = InputMappers.iterator();
            while(itm.hasNext()){
                MapTask mapper = itm.next();
                if(mapper.inputFinishTime < 0){
                    if(mapper.inputStartTime + mapper.predictInputTime < time + Settings.epsilon){
                        mapper.inputFinishTime = time;
                        mapper._job.oneMapperEndInput(mapper.host, mapper);
                        itm.remove();
                    }
                }
                else{
                    assert(false);
                }
            }

            // 4(2) finish mappers compute
            itm = activeMappers.iterator();
            while(itm.hasNext()){
                MapTask mapper = itm.next();
                if(mapper.finishTime < 0 && mapper.inputFinishTime > 0){
                    if(mapper.startTime + mapper.computationDelay < time + Settings.epsilon){
                        ++freeSlots[mapper.host];
                        hostInfos[mapper.host].freeSlots ++;
                        mapper.finish(time);
                        mapper._job.oneMapperFinished();
                        if(mapper._job.isAllMapperFinished_const()) {
                            mapper._job.mapStageFinish(time);
                            mapper._job.coflow.start(time); // coflow应该放在这里开始计时

                            double tempSum = 0;
                            for (int i = 0; i < TestSunderer.freeBw.length; ++i) { // 原来只想统计下载带宽, 但是考虑到map之前可能有上一个job的reduce阶段占用了上传带宽没有归还, 这里还是统计一下下载和上传所有带宽吧
                                tempSum += freeBw[i];
                            }
                            if(activeJobs.size() == 1) {
                                assert (tempSum == totalFreeBw); // 这里这个assert似乎是不对的, 因为如果此时有很多job的同时运行的话，一个job的mapStage结束后其他job的mapper仍在占用带宽传输!
                                assert (Math.abs(tempSum - Settings.speed * Settings.nHosts * 2) < 1e-3);
                                totalFreeBw = Settings.speed * Settings.nHosts * 2; // 重新置回初始设置, 以免误差继续累计
                                for(int i = 0; i < freeBw.length; ++i){
                                    freeBw[i] = Settings.speed; // 重新置回初始设置, 以免误差继续累计
                                }
                            }

                        }
                        Settings.algo.releaseHost(new HostAndTask(mapper.host,mapper));
//						scheduleOut.println(time+" [M] "+ mapper._job.jobId+"@"+mapper.mapperId +" finishes");
                        itm.remove();
                    }
                }else{
                    // cannot happen, because itm.remove()
                    assert(false);
                }
            }

            // 4(3) finish(reducers, macroflows)
            Iterator<ReduceTask> itr = activeReducers.iterator();
            while(itr.hasNext()){
                ReduceTask reducer = itr.next();
                Macroflow mf = reducer.macroflow;
                if(reducer.networkFinishTime<0){ // check if network phase finished
                    if(mf.isAllFlowsFinished_const()){
                        mf.finish(time); // we do not free the slot due to computation phase
                        reducer.networkFinished(time);

//						scheduleOut.println(time+" [R] "+ reducer._job.jobId+"@"+reducer.reducerId +" shuffle finishes");

                        for(int i = 0; i < hostInfos[reducer.host].shuffleReducerNum; i ++){
//							assert (shuffleReducer[reducer.host][i].mfPriority == i + 1);
                            if (hostInfos[reducer.host].shuffleReducer[i] == reducer) {
                                for (int j = i; j < hostInfos[reducer.host].shuffleReducerNum - 1; j ++) {
                                    hostInfos[reducer.host].shuffleReducer[j] = hostInfos[reducer.host].shuffleReducer[j + 1];
                                    hostInfos[reducer.host].shuffleReducer[j].mfPriority = j + 1;
                                }
                            }
                        }
                        hostInfos[reducer.host].shuffleReducerNum -= 1;
                        hostInfos[reducer.host].hostShuffleSize -= reducer.macroflow.size;
                        hostInfos[reducer.host].updateHostInfo();
                        HostInfo.hostMaxMinCoflowSize = 0;
                        for(int i = 0; i < Settings.nHosts; i ++) {
                            if (hostInfos[i].minCoflowSize > HostInfo.hostMaxMinCoflowSize) {
                                HostInfo.hostMaxMinCoflowSize = hostInfos[i].minCoflowSize;
                            }
                        }
                        assert (hostInfos[reducer.host].hostShuffleSize >= -1*Settings.epsilon);
                    }
                }
                if(reducer.computationFinishTime<0){ // check if computation phase finished
                    if(reducer.networkFinishTime>=0 && reducer.networkFinishTime + reducer.computationDelay < time + Settings.epsilon){
                        ++freeSlots[reducer.host];
                        hostInfos[reducer.host].freeSlots ++;
                        reducer.computationFinished(time);
                        reducer._job.oneReducerFinished(reducer);
                        Settings.algo.releaseHost(new HostAndTask(reducer.host,reducer));
//						scheduleOut.println(time+" [R] "+ reducer._job.jobId+"@"+reducer.reducerId +" finishes");
                        itr.remove();
                    }
                }else{
                    // cannot happen, because itr.remove()
                    assert(false);
                }
            }

            // 4(3). finish (jobs, coflows)
            Iterator<Job> itj = activeJobs.iterator();
            while(itj.hasNext()){
                Job job = itj.next();
                Coflow coflow = job.coflow;
                if(coflow.finishTime<0){ // check if coflow finished
                    if(coflow.isAllMacroflowsFinished_const()){
                        coflow.finish(time);
                    }
                }
                if(job.reduceStageFinishTime<0){ // check if job finished
                    if(job.nReducers > 0 && job.isAllReducerFinished_const()){
                        ++nFinishedJobs;
                        job.reduceStageFinish(time);
                        job.jobQueue.activeJobs.remove(job);
                        itj.remove();
                        // System.out.printf("%.3f job %d finished\n", time, job.jobId);
                    }
                    if(job.nReducers == 0 && job.mapStageFinishTime > 0) {
                        ++nFinishedJobs;
                        job.reduceStageFinish(time);
                        job.jobQueue.activeJobs.remove(job);
                        itj.remove();
                        // System.out.printf("%.3f job %d finished\n", time, job.jobId);
                    }
                }else{
                    assert(false);
                }
            }
            while (activeJobs.size() < parallelism && pendingJobs.size() != 0) {
                Job j = pendingJobs.get(0);
                pendingJobs.remove(0);
                activeJobs.add(j);
                j.jobQueue.activeJobs.add(j);
            }
        }

    }

    public static int countFreestHost_const(){
        int sum = 0;
        for(int i=0;i<freeSlots.length;++i)
            sum += freeSlots[i];
        return sum;
    }

    private static double getNextArrivalTime_const(){
        double t = Double.POSITIVE_INFINITY;
        if(Job.nArrivedJobs<jobs.length)
            t = jobs[Job.nArrivedJobs].arriveTime;
        return t;
    }

    private static double getSimulationSteps_const(){
        double step = Math.max(Settings.minTimeStep, getNextArrivalTime_const() - time);
        for(MapTask mapper:InputMappers){
            debugCount += 1;
            assert (mapper.inputFinishTime < 0);
            double remainingTrans = mapper.inputStartTime + mapper.predictInputTime - time;
            // System.out.printf("remainingTrans: %f Count: %d\n", remainingTrans, debugCount);
            if(remainingTrans < 0){
                //System.out.printf("remainingComp < 0! mapper.inputStartTime: %f + mapper.predictInputTime: %f - time: %f = remainingComp: %f\n", mapper.inputStartTime, mapper.predictInputTime, time, remainingTrans);
                //System.out.flush();
            }
            assert (remainingTrans >= 0);
            step = Math.min(step, remainingTrans);
            if(step<=Settings.minTimeStep) {
                return Settings.minTimeStep;
            }
        }
        for(MapTask mapper:activeMappers){
            assert (mapper.inputFinishTime > 0);
            assert (mapper.startTime > 0);
            double remainingComp = mapper.startTime + mapper.computationDelay - time;
            if(remainingComp < 0){
                //System.out.printf("remainingComp < 0! mapper.startTime: %f + mapper.computationDelay: %f - time: %f = remainingComp: %f\n", mapper.startTime, mapper.computationDelay, time, remainingComp);
                //System.out.flush();
            }
            assert (remainingComp >= 0);
            step = Math.min(step, remainingComp);
            if(step<=Settings.minTimeStep) {
                return Settings.minTimeStep;
            }
        }
        for(ReduceTask reducer:activeReducers){
            if(reducer.networkFinishTime<0) {

                Macroflow mf = reducer.macroflow;
                for (Flow flow : mf.flows) {
                    if (flow.finishTime >= 0) {
                        continue;
                    }
                    step = Math.min(step, flow.size - flow.sentSize);
                }
                if (mf.isAllFlowsFinished_const()) {
//                    mf.finish(time); // 如果reducer和mapper被部署在一个节点上, 这里就不会生成流, 会直接结束, 原代码好像没有处理这种conner case
//                    reducer.networkFinished(time);
                    step = 0;
                    return step; // conner case: reducer和mapper位于同一个机器, 没生成流, 则流的结束时间就是当前时间, 此时不能迈步子, 要原地踏步一下交给Simulate中4.3去完结Network阶段
                }
            }
            else{
                assert(reducer.computationFinishTime<0);
                double remainingComp = reducer.networkFinishTime + reducer.computationDelay - time;
                if(remainingComp < 0){
                    //System.out.printf("remainingComp < 0! reducer.networkFinishTime: %f + reducer.computationDelay: %f - time: %f = remainingComp: %f\n", reducer.networkFinishTime, reducer.computationDelay, time, remainingComp);
                    //System.out.flush();
                }
                assert(remainingComp >= 0);
                step = Math.min(step, remainingComp);
            }
            if(step<=Settings.minTimeStep) {
                return Settings.minTimeStep;
            }
        }
        return Double.isInfinite(step)?Settings.minTimeStep:step;

    }

}
