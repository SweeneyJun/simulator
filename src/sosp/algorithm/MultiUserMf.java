package sosp.algorithm;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Random;

import sosp.jobs.Job;
import sosp.jobs.MapTask;
import sosp.jobs.ReduceTask;
import sosp.jobs.Task;
import sosp.main.Priority;
import sosp.main.Scheduler;
import sosp.main.Settings;
import sosp.main.Topology;
import sosp.network.Flow;

public class MultiUserMf implements Algorithm {

	private mfState state = null;
	
	private int nFreeSlots = 0;
	private boolean enoughSlots = true;
	
	static public int enoughSlot=0, total=0;
	
	private class MapperDistribution{
		public int[] nMapper = new int[Settings.nHosts];
	}

	private enum mfState {
		MACROFLOW, COFLOW, HYBRID
	}
	
	private MapperDistribution[] mapperDist = null;
	
	private ArrayList<Job>[] hostJob = null;
	private ArrayList<ReduceTask>[] hostReducer = null;
	
	public MultiUserMf(String s){
		if(s.equalsIgnoreCase("macroflow"))
//			state = 1;
			state = mfState.MACROFLOW;
		if(s.equalsIgnoreCase("coflow"))
//			state = -1;
			state = mfState.COFLOW;
		if(s.equalsIgnoreCase("hybrid"))
//			state = 0;
			state = mfState.HYBRID;
		if(s.equalsIgnoreCase("none")){
//			state = -1;
			state = mfState.COFLOW;
			Settings.nPriorities = 1;
		}
			
		System.out.println(state);
	}
	
	@SuppressWarnings("unchecked")
	private void init(){
		if(mapperDist==null) {
			mapperDist = new MapperDistribution[Scheduler.jobs.length];
			for(int i=0;i<mapperDist.length;++i)
				mapperDist[i] = new MapperDistribution();
		}
		if(hostJob==null) {
			hostJob = new ArrayList[Settings.nHosts];
			for(int i=0;i<hostJob.length;++i)
				hostJob[i] = new ArrayList<Job>();
		}
		if(hostReducer == null){
			hostReducer = new ArrayList[Settings.nHosts];
			for(int i=0;i<hostReducer.length;++i)
				hostReducer[i] = new ArrayList<ReduceTask>();
		}
	}
	
	private HostAndTask recordReducerAllocation(int host, ReduceTask reducer){
		hostJob[host].add(reducer._job);
		hostReducer[host].add(reducer);
		return new HostAndTask(host,reducer);
	}
	
	@Override
	public HostAndTask allocateHostAndTask() {
		//assert(Settings.nHosts == Settings.nReplications);
		init();
		
		Job[] activeJobs = Settings.fairJobScheduler ? Algorithm.jobSorterFair() : Algorithm.jobSorter();
		if(activeJobs.length==0)
			return null;
		double[] freeBw = Scheduler.freeBw==null ? Topology.getLinkBw() : Scheduler.freeBw;
		double tp = Topology.getHostThroughput(freeBw);
		boolean isCongested = tp>Settings.congestionThreshold;
		//boolean isMapper = chosenJob.hasPendingMappers_const();
		nFreeSlots = Scheduler.countFreestHost_const();
		if(nFreeSlots==0){
			enoughSlots = false;
			return null;
		}
		enoughSlots = activeJobs[0].countReadyTasks_const() <= nFreeSlots;
		//enoughSlots = Scheduler.countFreestHost_const() <= nFreeSlots;
		//enoughSlots = 1 <= nFreeSlots;
		
		if(enoughSlots){
			if(isCongested){
				while(activeJobs[0].countReadyTasks_const()>0){ // run only once
					if(activeJobs[0].hasPendingMappers_const()){ // is mapper
						//MapTask mapper = activeJobs[0].pendingMapperList.get(0);
						HostAndTask ht = getMapperHost(activeJobs[0]);
						if(ht==null) break;
						++mapperDist[activeJobs[0].jobId].nMapper[ht.host];
						return ht;
					} else { // is reducer
						ReduceTask reducer = activeJobs[0].pendingReducerList.get(0);
						int host = getReducerHostEx(activeJobs[0]);
						if(host<0) return null;
						
						// XXX
						//reducer = activeJobs[0].pendingReducerList.get(0);
						//host = getFreestHost();
						
						return recordReducerAllocation(host,reducer);
					}
					//break;
				}
				for(int i=1;i<activeJobs.length;++i){
					if(activeJobs[i].countReadyTasks_const()>0 && activeJobs[i].hasPendingMappers_const()){
						//MapTask mapper = activeJobs[i].pendingMapperList.get(0);
						HostAndTask ht = getMapperHost(activeJobs[i]);
						if(ht==null) continue;
						++mapperDist[activeJobs[i].jobId].nMapper[ht.host];
						return ht;
					}
				}
				return null;
			} else {
				while(activeJobs[0].countReadyTasks_const()>0){ // only once
					if(activeJobs[0].hasPendingMappers_const()){ // is mapper
						//MapTask mapper = activeJobs[0].pendingMapperList.get(0);
						HostAndTask ht = getMapperHost(activeJobs[0]);
						if(ht==null) break;
						++mapperDist[activeJobs[0].jobId].nMapper[ht.host];
						return ht;
					} else { // is reducer
						ReduceTask reducer = activeJobs[0].pendingReducerList.get(0);
						int host = getReducerHostEx(activeJobs[0]);
						if(host<0) return null;
						
						// XXX
						//reducer = activeJobs[0].pendingReducerList.get(0);
						//host = getFreestHost();
						
						return recordReducerAllocation(host,reducer);
					}
				}
				for(int i=1;i<activeJobs.length;++i){
					if(activeJobs[i].countReadyTasks_const()>0 && !activeJobs[i].hasPendingMappers_const()){
						ReduceTask reducer = activeJobs[i].pendingReducerList.get(0);
						int host = getReducerHostEx(activeJobs[i]);
						if(host<0) continue;
						
						// XXX
						//reducer = activeJobs[i].pendingReducerList.get(0);
						//host = getFreestHost();
						
						return recordReducerAllocation(host,reducer);
					}
				}
				return null;
			}
		} else {
			while(activeJobs[0].countReadyTasks_const()>0){ // run only once
				if(activeJobs[0].hasPendingMappers_const()){ // is mapper
					//MapTask mapper = activeJobs[0].pendingMapperList.get(0);
					HostAndTask ht = getMapperHost(activeJobs[0]);
					if(ht==null) break;
					++mapperDist[activeJobs[0].jobId].nMapper[ht.host];
					return ht;
				} else { // is reducer
					ReduceTask reducer = null;
					for(ReduceTask r:activeJobs[0].pendingReducerList){
						if(reducer==null)
							reducer = r;
						else if(reducer.macroflow.size > r.macroflow.size)
							reducer = r;
					}
					assert(reducer!=null);
					int host = getReducerHost(activeJobs[0]);
					if(host<0) return null;
					
					// XXX
					//reducer = activeJobs[0].pendingReducerList.get(0);
					//host = getFreestHost();
					
					return recordReducerAllocation(host,reducer);
				}
			}
			return null;
		}
	}
	

	
	private HostAndTask getMapperHost(Job job) {
		ArrayList<HostAndTask> a = new ArrayList<HostAndTask>();
		int[] distribution = mapperDist[job.jobId].nMapper;
		for(MapTask mt:job.pendingMapperList) {
			for(int i=0;i<mt.hdfsHost.length;++i) {
				if(Scheduler.freeSlots[mt.hdfsHost[i]]==0) continue;
				HostAndTask ht = new HostAndTask(mt.hdfsHost[i],mt);
				if(a.size()==0) { a.add(ht); continue; } 
				int best = distribution[a.get(0).host];
				if(distribution[i]==best) {
					a.add(ht);
				}else if(distribution[i] < best) {
					a.clear(); a.add(ht);
				}
			}
		}
		if(a.isEmpty()){
			MapTask mt = job.pendingMapperList.get(0);
			for(int i=0;i<Settings.nHosts;++i){
				if(Scheduler.freeSlots[i]==0) continue;
				HostAndTask ht = new HostAndTask(i,mt);
				a.add(ht);
			}
		}
		a.sort(new Comparator<HostAndTask>(){ // ascending order
			@Override public int compare(HostAndTask arg0, HostAndTask arg1) {
				return Scheduler.freeSlots[arg0.host]-Scheduler.freeSlots[arg1.host];
			}
		});
		
		return a.get(a.size()-1);
	}

	
	private int getReducerHost(Job job) {
		//int DOWN = Settings.nHosts;
		ArrayList<Integer> a = new ArrayList<Integer>();
		double[] smallest = new double[Settings.nHosts];
		for(int i=0;i<Settings.nHosts;++i) {
			if(Scheduler.freeSlots[i]==0) continue;
			smallest[i] = Double.POSITIVE_INFINITY;
			for(Job j:hostJob[i]) 
				if(smallest[i]==0 || smallest[i]>j.coflow.size)
					smallest[i] = j.coflow.size;
			if(smallest[i]>0 && job.coflow.size >= smallest[i]) continue;
			a.add(i);
		}
		a.sort(new Comparator<Integer>() {
			@Override public int compare(Integer o1, Integer o2) {
				double a = smallest[o1] - smallest[o2];
				return a>0?1:(a<0?-1:0);
			}
		});
		if(a.isEmpty())return -1;
		return a.get(a.size()-1);
	}
	
	private int getReducerHostEx(Job job) {
		//int DOWN = Settings.nHosts;
		ArrayList<Integer> a = new ArrayList<Integer>();
		double[] smallest = new double[Settings.nHosts];
		for(int i=0;i<Settings.nHosts;++i) {
			if(Scheduler.freeSlots[i]==0) continue;
			smallest[i] = Double.POSITIVE_INFINITY;
			for(Job j:hostJob[i]) 
				if(smallest[i]==0 || smallest[i]>j.coflow.size)
					smallest[i] = j.coflow.size;
			if(smallest[i]>0 && job.coflow.size >= smallest[i]) continue;
			a.add(i);
		}
		a.sort(new Comparator<Integer>() {
			@Override public int compare(Integer o1, Integer o2) {
				int a = hostReducer[o1].size() - hostReducer[o2].size();
				return a>0?1:(a<0?-1:0);
			}
		});
		if(a.isEmpty())return -1;
		return a.get(0);
	}
	
	private int getReducerHostMf(ReduceTask reducer) {
		ArrayList<Integer> a = new ArrayList<Integer>();
		double[] smallest = new double[Settings.nHosts];
		for(int i=0;i<Settings.nHosts;++i) {
			if(Scheduler.freeSlots[i]==0) continue;
			for(ReduceTask r:hostReducer[i]) 
				if(smallest[i]==0 || smallest[i]>r.macroflow.size)
					smallest[i] = r.macroflow.size;
			if(smallest[i]>0 && reducer.macroflow.size >= smallest[i]) continue;
			a.add(i);
		}
		a.sort(new Comparator<Integer>() {
			@Override public int compare(Integer o1, Integer o2) {
				double a = smallest[o1] - smallest[o2];
				return a>0?1:(a<0?-1:0);
			}
		});
		if(a.isEmpty())return -1;
		return a.get(a.size()-1);
	}
	
	private int getFreestHost(){
		int best = -1;
		for(int i=0;i<Settings.nHosts;++i){
			if(Scheduler.freeSlots[i]==0)
				continue;
			if(best<0 || Scheduler.freeSlots[best]<Scheduler.freeSlots[i])
				best = i;
		}
		return best;
	}

	@Override
	public void releaseHost(HostAndTask ht) {
		if(ht.task instanceof ReduceTask) {
			Job job = ((ReduceTask)ht.task)._job;
			assert(hostJob[ht.host].remove(job));
			assert(hostReducer[ht.host].remove((ReduceTask)ht.task));
		}
	}

	@Override
	public ArrayList<Flow>[] getPriority() {
		if(Settings.nPriorities>0) {
			++total;
			switch(state){
				case HYBRID:
					if(enoughSlots){++enoughSlot;
						return Priority.HybridPriority(1);}
					else
						return Priority.HybridPriority(0);
				case MACROFLOW:
					return Priority.HybridPriority(0);
				case COFLOW:
					return Priority.HybridPriority(1);
				default:
					assert(false);
					return null;
			}

		} else {
			++total;
			switch(state){
				case HYBRID:
					if(enoughSlots) {++enoughSlot;
						return Priority.infinitePrioritiesCoflow();
						//return Priority.infinitePrioritiesMf();
					}else {
						return Priority.infinitePrioritiesMf();
						//return Priority.infinitePrioritiesCoflow();
					}
				case MACROFLOW:
					return Priority.infinitePrioritiesMf();
				case COFLOW:
					return Priority.infinitePrioritiesCoflow();
				default:
					return null;
			}
		}
	}

}
