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

public class Scheduler {

	/* Note: this class only contains runtime states. 
	 * Properties related to setting files are defined in Settings.java
	 */

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

	public static ArrayList<JobQueue> jobQueues = new ArrayList<>();

//	public static double[] leastShuffleSize = null;
//	public static double[] minCoflowSize = null;
//	public static int[] shuffleReducerNum = null;
//	public static ReduceTask[][] shuffleReducer = null;
	public static HostInfo[] hostInfos = null;

	private static PrintWriter scheduleOut = null;

//	private static PrintWriter taskTp = null;
	
	public static void main(String[] args) throws Exception{

		jobQueues.add(new JobQueue("fair", 0.5, 1));
//		jobQueues.add(new JobQueue("fair", 0.5, 0.8));

		Measurement.tic();//系统当前时间ms
		Settings.loadFromFile("config.ini",args);//载入配置文件
		initialize();

		simulate();
		Measurement.OutputCompletionTime2(jobs);
		
		PrintWriter cout = new PrintWriter(new FileOutputStream("tp_cxw.txt"));
		for(int i=0;i<throughput.size();++i){
			cout.printf("%.3f %.3f %.3f\n",throughput.get(i).time, throughput.get(i).hostThroughput, slot.get(i));
		}

		cout.close();
		Measurement.toc();

		ArrayList<Task> tasks = new ArrayList<>();
		for (Job job: jobs) {
//			System.out.println(job.arriveTime + ", " + job.reduceStageFinishTime);
			for (int i = 0; i < Settings.nHosts; i ++){
				tasks.addAll(job.emittedMapperList[i]);
			}
				tasks.addAll(job.emittedReducerList);
		}
		System.out.println(tasks.size());
		Collections.sort(tasks, new Comparator<Task>(){
            @Override public int compare(Task arg0, Task arg1) {
                return arg1.startTime==arg0.startTime ? 0 : (arg1.startTime>arg0.startTime ? -1 : 1);
            }
        });
		for(Task task: tasks) {
			if(task instanceof MapTask) {
				MapTask mt = (MapTask) task;
				scheduleOut.println(mt._job.jobId + ", " + mt.mapperId + ", " + mt.host + ", " + mt.startTime + ", " + mt.finishTime + ", " + -1);
			}
			if(task instanceof ReduceTask) {
				ReduceTask rt = (ReduceTask) task;
				scheduleOut.println(rt._job.jobId + ", " + rt.reducerId + ", " + rt.host + ", " + rt.startTime + ", " + rt.networkFinishTime + ", " + rt.computationFinishTime);
			}
		}
		scheduleOut.flush();
		scheduleOut.close();
//		taskTp.flush();
//		taskTp.close();

		System.out.println("max reducer num: " + HostInfo.hostMaxReducerNum);
		
		/*if(Settings.algo instanceof Neat)
			System.out.println(Neat.allocationTime);
		else if(Settings.algo instanceof NeatGlobal)
			System.out.println(NeatGlobal.allocationTime);*/
	}
	
	
	private static void initialize() throws FileNotFoundException {
		jobs = Traffic.loadFromFile("FB2010-1Hr-150-0.txt");
		//jobs = Traffic.loadFromFile("testbed-toy.txt");
		//jobs = Traffic.loadFromFile("neat.txt");
		//jobs = Traffic.loadFromFile("neat-executing-time.txt");
		freeSlots = new int[Settings.nHosts];
		for(int i=0;i<freeSlots.length;++i)
			freeSlots[i] = Settings.nSlots;
		if(Settings.isGaintSwitch)
			Topology.loadGaint();
		else
			Topology.loadTwoLayer();
		throughput = Measurement.newThroughput();
		// record the slot using ratio during the schedule
		slot = new ArrayList<Double>();
		slot.add(0.0);

//		leastShuffleSize = new double[Settings.nHosts];
//		minCoflowSize = new double[Settings.nHosts];
//		shuffleReducerNum = new int[Settings.nHosts];
//		if (Settings.algo instanceof Max3Reducer) {
//			shuffleReducer = new ReduceTask[Settings.nHosts][Settings.reduceNum];
//		}
//		else {
//			shuffleReducer = new ReduceTask[Settings.nHosts][Settings.nSlots];
//		}
		hostInfos = new HostInfo[Settings.nHosts];
		for(int i = 0; i < Settings.nHosts;i ++) {
			hostInfos[i] = new HostInfo(i);
		}
		scheduleOut = new PrintWriter(new FileOutputStream("schedule.txt"));

//		taskTp = new PrintWriter(new FileOutputStream("tasktp.txt"));

		parallelism = (int)(Settings.parallelism*Settings.nSlots*Settings.nHosts);
		System.out.println(parallelism);
		assert(Settings.nPriorities>=0);
		assert(Settings.minTimeStep>0);
		assert(Settings.epsilon>=0);
	}
	
	private static void simulate(){
		int nFinishedJobs = 0;
		while(nFinishedJobs<jobs.length){
			// 1. check for newly arrived coflows
			// job's arriveTime is increasing
			while(Job.nArrivedJobs<jobs.length && jobs[Job.nArrivedJobs].arriveTime <= time){
				Job job = jobs[Job.nArrivedJobs++];
				Coflow coflow = job.coflow;
				coflow.start(time);
				if (activeJobs.size() < parallelism) {
					activeJobs.add(job);
					job.jobQueue.activeJobs.add(job);
				}
				else {
					pendingJobs.add(job);
				}

				System.out.printf("%.3f job %d started\n", time, job.jobId);
			}
			
			
			// 2. scheduling tasks (both mapper and reducer)
			while(true){
				// HostAndTask represent the <host, task> pair
				HostAndTask ht = Settings.algo.allocateHostAndTask();
				if(ht==null)
					break;
				--freeSlots[ht.host];
				hostInfos[ht.host].freeSlots --;
				if(ht.task instanceof MapTask){
					MapTask mapper = (MapTask) ht.task;
					mapper._job.oneMapperStarted(ht.host, mapper);
					mapper.emit(ht.host, time);
					activeMappers.add(mapper);
//					scheduleOut.println(time+" [M] "+ mapper._job.jobId+"@"+mapper.mapperId +" "+ht.host);
				}
				else{ // instanceof ReduceTask
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
//					scheduleOut.println(time+" [R] "+ reducer._job.jobId+"@"+reducer.reducerId +" "+ht.host);
				}
			}


			// 3. transmitting flows
			double nFreeSlotsRatio = countFreestHost_const()/(double)(Settings.nSlots*Settings.nHosts);
			ArrayList<Flow>[] activeFlows =  Settings.algo.getPriority();
			// ArrayList<Flow>[] activeFlows = Priority.getPrioritizedFlows(/*nFreeSlotsRatio*/throughput.get(throughput.size()-1).hostThroughput<slot.get(slot.size()-1)?0:1);

			// get Link Bandwidth
			freeBw = Topology.getLinkBw();
			// work conservation
			MaxMin.getMaxMin(activeFlows, freeBw);
			double step = getSimulationSteps_const();
			Measurement.measureThroughput(throughput, freeBw, time, step);
			slot.add(1-nFreeSlotsRatio);
			time += step; // update current time
			for(ArrayList<Flow> flows:activeFlows){
  				for(Flow flow:flows){
//					System.out.println(flow._macroflow._reducer.host + ":" + leastShuffleSize[flow._macroflow._reducer.host]);
					flow.sentSize += flow.allocatedBw * step;
//					leastShuffleSize[flow._macroflow._reducer.host] -= flow.allocatedBw * step;
//					assert (leastShuffleSize[flow._macroflow._reducer.host] >= flow._macroflow.size * step * -1);
					if(flow.sentSize + Settings.epsilon > flow.size) // gt, not ge
						flow.Finish(time);
				}
			}
//			Measurement.measureTaskThroughput(time, activeFlows, taskTp);


			// 4(1). finish (mappers)
			Iterator<MapTask> itm = activeMappers.iterator();
			while(itm.hasNext()){
				MapTask mapper = itm.next();
				if(mapper.finishTime<0){
					if(mapper.startTime + mapper.computationDelay < time + Settings.epsilon){
						++freeSlots[mapper.host];
						hostInfos[mapper.host].freeSlots ++;
						mapper.finish(time);
						mapper._job.oneMapperFinished();
						if(mapper._job.isAllMapperFinished_const())
							mapper._job.mapStageFinish(time);
						Settings.algo.releaseHost(new HostAndTask(mapper.host,mapper));
//						scheduleOut.println(time+" [M] "+ mapper._job.jobId+"@"+mapper.mapperId +" finishes");
						itm.remove();
					}
				}else{
					// cannot happen, because itm.remove()
					assert(false);
				}
			}

			// 4(2). finish (reducers, macroflows)
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
						reducer._job.oneReducerFinished();
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
						System.out.printf("%.3f job %d finished\n", time, job.jobId);
					}
					if(job.nReducers == 0 && job.mapStageFinishTime > 0) {
						++nFinishedJobs;
						job.reduceStageFinish(time);
						job.jobQueue.activeJobs.remove(job);
						itj.remove();
						System.out.printf("%.3f job %d finished\n", time, job.jobId);
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
		if(Settings.algo instanceof MultiUserMf) {
			// the ratio enoughSlot schedule time / total schedule time
			System.out.println(MultiUserMf.enoughSlot / (double) MultiUserMf.total);
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

	// return the max step we can advance
	private static double getSimulationSteps_const(){
		double step = Math.max(Settings.minTimeStep, getNextArrivalTime_const() - time);
		for(MapTask mapper:activeMappers){
			double remainingComp = mapper.startTime + mapper.computationDelay - time;
//			System.out.println(mapper.startTime);
//			System.out.println(mapper.computationDelay);
//			System.out.println(time);
//			System.out.println(remainingComp);
			assert(remainingComp>0);
			step = Math.min(step, remainingComp);
			// zy: return Settings.minTimeStep to reduce simulation's time
			if(step<=Settings.minTimeStep)
				return Settings.minTimeStep;
		}
		for(ReduceTask reducer:activeReducers){
			if(reducer.networkFinishTime<0){
				Macroflow mf = reducer.macroflow;
				for(Flow flow:mf.flows){
					if(flow.finishTime>=0)
						continue;
					step = Math.min(step, flow.size - flow.sentSize);
				}
			}else{
				assert(reducer.computationFinishTime<0);
				double remainingComp = reducer.networkFinishTime + reducer.computationDelay - time;
				assert(remainingComp>0);
				step = Math.min(step, remainingComp);
			}
			if(step<=Settings.minTimeStep)
				return Settings.minTimeStep;
		}
		return Double.isInfinite(step)?Settings.minTimeStep:step;
	}

}
