package sosp.main;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.util.*;

import sosp.algorithm.Algorithm.HostAndTask;
import sosp.algorithm.Max3Reducer;
import sosp.algorithm.MultiUserMf;
import sosp.jobs.*;
import sosp.network.Coflow;
import sosp.network.Flow;
import sosp.network.Macroflow;

public class SeparateScheduler{

    public static int parallelism = 0;

    public static double time = 0; // current simulation time
    public static Job[] jobs = null; // all of the jobs
    public static int[] freeSlots = null; // # free slots in each host
    public static double[] freeBw = null;

    public static ArrayList<Job> activeJobs = new ArrayList<Job>(); // arrived and not finished coflows
    public static ArrayList<Job> pendingJobs = new ArrayList<Job>(); // arrived and not finished coflows
    public static ArrayList<ReduceTask> activeReducers = new ArrayList<ReduceTask>(); // emitted and not finished reducers
    public static ArrayList<MapTask> activeMappers = new ArrayList<MapTask>(); // emitted and not finished mappers
    public static ArrayList<Measurement.Throughput> throughput = null;
    public static ArrayList<Double> slot = null;

    public static ArrayList<MapTask> arrivedMappers = new ArrayList<MapTask>();

    public static ArrayList<JobQueue> jobQueues = new ArrayList<>();


    public static HostInfo[] hostInfos = null;

    private static PrintWriter scheduleOut = null;

    public static void main(String[] args) throws Exception{
        jobQueues.add(new JobQueue("fair", 0.5, 1));

        Measurement.tic(); // 系统当前时间
        Settings.loadFromFile("config.ini", args);
        assert(Settings.isSeparate); // 非存算分离场景下不应该运行这个类
    }

    private static void initialize() throws FileNotFoundException{
        jobs = SeparateTraffic.loadFromFile("FB2010-1Hr-150-0.txt");
        freeSlots = new int[Settings.nHosts];
        for(int i = 0; i < Settings.nHosts; ++i) {
            freeSlots[i] = Settings.nSlots;
        }

        if(Settings.isGaintSwitch)
            Topology.loadGaint();
        else
            Topology.loadTwoLayer();


        Topology.loadSeparateGaint();

        throughput = Measurement.newThroughput();
        // record the slot using ratio during the schedule
        slot = new ArrayList<Double>();
        slot.add(0.0);

        hostInfos = new HostInfo[Settings.nHosts];
        for(int i = 0; i < Settings.nHosts;i ++) {
            hostInfos[i] = new HostInfo(i);
        }
        scheduleOut = new PrintWriter(new FileOutputStream("SeparateSchedule.txt"));

        parallelism = (int)(Settings.parallelism*Settings.nSlots*Settings.nHosts); // 这几行都是复制的*山
        System.out.println(parallelism);
        assert(Settings.nPriorities>=0);
        assert(Settings.minTimeStep>0);
        assert(Settings.epsilon>=0); // 这个在部分比较运算时加上, 避免产生浮点数舍入误差

    }

    private static void simulate(){
        int nFinishedJobs = 0;
        while(nFinishedJobs < jobs.length){ // 条件成立说明还有没完成的job
            while(Job.nArrivedJobs < jobs.length && jobs[Job.nArrivedJobs].arriveTime <= time + Settings.epsilon){ // 还有可以到来的job, 以及模拟时间追上到达时间时才可以开始调度该job
                Job job = jobs[Job.nArrivedJobs++];
                Coflow coflow = job.coflow;

                if(activeJobs.size() < parallelism){
                    activeJobs.add(job);
                }
                else{
                    pendingJobs.add(job);
                }
                System.out.printf("%.3f job %d started\n", time, job.jobId);

                for(int i = 0; i < job.nMappers; i++){
                    arrivedMappers.add(job.pendingMapperList.get(i));
                }
            }


        }

        // 2. scheduling tasks (both mapper and reducer)
        // when scheduling the mapTask, handle the input file transmitting process
    }

}
