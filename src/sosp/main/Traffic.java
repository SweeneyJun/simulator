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

public class Traffic {
	
	@SuppressWarnings("unchecked")
	public static Job[] loadFromFile(String traffic){
		assert(Settings.nHosts>0);
		//assert(Settings.r!=null);
		assert(Settings.sizeScale>0);
		assert(Settings.timeScale>0);
		
		ArrayList<Integer> dfs = new ArrayList<Integer>();
		for(int i=0;i<Settings.nHosts;++i) {
			dfs.add(i % Settings.nHosts);  //??
		}

		ArrayList<Integer> queuePick = new ArrayList<>();
		for (int i = 0; i < Scheduler.jobQueues.size(); i ++){
			queuePick.add(i);
		}
		
		Job[] jobs = null;
		Scanner cin = null;
		try {
			cin = new Scanner(new FileInputStream(traffic));
//			assert(cin!=null);
		} catch (Exception e) {
			assert(false);
		}
		int jobId = 0;
		
		Random error = new Random(0);

		int ii = 0;

		while(cin.hasNextLine()){
			String[] elem = cin.nextLine().split("\\s+"); // "\\s+" match any blank character
//			if(elem==null || elem.length==0)
			if(elem.length==0)
				continue;
			if(jobs==null){// first line, not knowing #jobs yet
				jobs = new Job[Integer.parseInt(elem[1])];
				continue;
			}
			
			int pointer = 0;
			Job job = new Job(jobId);
			Coflow coflow = new Coflow(job);
			job.coflow = coflow;
//			System.out.println(elem[pointer + 1]);
			job.arriveTime = Long.parseLong(elem[++pointer])/1000.0 * Settings.timeScale; // normalized time: s

			Collections.shuffle(queuePick,Settings.r);
			job.jobQueue = Scheduler.jobQueues.get(queuePick.get(0)); // wcx:随机选一个JobQueue塞进去?

			// tasks
			int mappers = Integer.parseInt(elem[++pointer]);
			int reducers = Integer.parseInt(elem[pointer+=mappers+1]); // inside [] code runs first
			
			// reduce-tasks
			job.pendingReducerList = new ArrayList<ReduceTask>();
			job.emittedReducerList = new ArrayList<ReduceTask>();
			coflow.macroflows = new Macroflow[reducers];
			for(int i=0;i<reducers;++i){
				ReduceTask reduceTask = new ReduceTask(job,i);
				Macroflow mf = new Macroflow(coflow,reduceTask);
				mf.size = Double.parseDouble(elem[++pointer].split(":")[1])*8/1024 * Settings.sizeScale; // in Gb
				coflow.size += mf.size;
				reduceTask.macroflow = coflow.macroflows[i] = mf;
				reduceTask.computationDelay = Settings.reducerCompTimePerMB * mf.size / 8* 1024;
				job.pendingReducerList.add(reduceTask);
			}
			if (Settings.algo instanceof Max3Reducer) {
				Collections.sort(job.pendingReducerList, new Comparator<ReduceTask>() {
					@Override
					public int compare(ReduceTask arg0, ReduceTask arg1) {
						return arg1.macroflow.size == arg0.macroflow.size ? 0 : (arg1.macroflow.size < arg0.macroflow.size ? -1 : 1);
					}
				});
			}
			job.nReducers = reducers;


//			if (job.coflow.size > 0.1) {
//				job.jobQueue = Scheduler.jobQueues.get(1);
//				ii ++;
//			}
//			else{
//				job.jobQueue = Scheduler.jobQueues.get(0);
//			}

			// map-tasks
			job.pendingMapperList = new ArrayList<MapTask>();
			job.emittedMapperList = new ArrayList[Settings.nHosts];
			for(int i=0;i<Settings.nHosts;++i)
				job.emittedMapperList[i] = new ArrayList<MapTask>();

			double jobSize;
			if (reducers != 0) {
				jobSize = coflow.size;
			}
			else{
				jobSize = Double.parseDouble(elem[++pointer]);
				job.coflow.size = jobSize;
			}

			int nRealMappers = Algorithm.estimateRealMapperNumber(jobSize/8*1024,mappers); // from Gb to MB
			for(int i=0;i<nRealMappers;++i){
				MapTask mapTask = new MapTask(job,i);
				mapTask.computationDelay = Algorithm.estimateMapperTime((jobSize/8*1024)/nRealMappers);

				mapTask.predictComputationDelay = mapTask.computationDelay *= Math.pow(Settings.sizeEstimationError, 1-2*error.nextDouble());

				// Collecions.shuffle disorders the list members
				Collections.shuffle(dfs,Settings.r);
				mapTask.hdfsHost = new int[Settings.nReplications];
				for(int k=0;k<Settings.nReplications;++k) {
					mapTask.hdfsHost[k] = dfs.get(k);
				}
				job.pendingMapperList.add(mapTask);
			}
			job.nMappers = nRealMappers;

			// flows
			for(int i=0;i<reducers;++i){
				Macroflow mf = job.coflow.macroflows[i];
				mf.flows = new Flow[Settings.nHosts]; // merge same-source flows
				for(int j=0;j<Settings.nHosts;++j){
					Flow flow = new Flow(mf,coflow,j); // note that we cannot determine the flow size yet
					flow.sender = j;
					mf.flows[j] = flow;
				}
			}
			
			//zy:  the mapper stage's time is correct?
			job.estimatedRunningTime = job.nMappers/(double)(Settings.nSlots*Settings.nHosts) * job.pendingMapperList.get(0).computationDelay;
			job.estimatedRunningTime += job.coflow.size/(double)(Settings.nHosts);
			if(Math.abs(Settings.sizeEstimationError-1)>Settings.epsilon)
				job.estimatedRunningTime *= Math.pow(Settings.sizeEstimationError, 1-2*error.nextDouble());
			jobs[jobId++] = job;
		}
		cin.close();
		System.out.println("i: "+ ii);
		return jobs;
	}
	
	/*private static String[] mergeTasks(String[] elem){
		ArrayList<String> out = new ArrayList<String>();
		int p = 0;
		out.add(elem[p++]); // index
		out.add(elem[p++]); // time
		int nMappers = Integer.parseInt(elem[p++]);
		p += nMappers;
		nMappers = Math.min(nMappers, Settings.nHosts);
		out.add(Integer.toString(nMappers));
		ArrayList<Integer> mapper = new ArrayList<Integer>();
		for(int i =0;i<Settings.nHosts;++i)
			mapper.add(i);
		Collections.shuffle(mapper, Settings.r);
		for(int i = 0;i<nMappers;++i)
			out.add(mapper.get(i).toString());
		
		int nReducers = Integer.parseInt(elem[p++]);
		double[] receiver_size = new double[Settings.nHosts];
		for(int i=0;i<nReducers;++i){
			String[] pair = elem[p++].split(":");
			int host = Integer.parseInt(pair[0]) % Settings.nHosts;
			double size = Double.parseDouble(pair[1]);
			receiver_size[host] += size;
		}
		ArrayList<String> receiver_size_nonzero = new ArrayList<String>();
		for(int i=0;i<receiver_size.length;++i)
			if(receiver_size[i]>0)
				receiver_size_nonzero.add(i+":"+receiver_size[i]);
		out.add(Integer.toString(receiver_size_nonzero.size()));
		for(String s:receiver_size_nonzero)
			out.add(s);
		return out.toArray(new String[0]);
	}*/
	
}
