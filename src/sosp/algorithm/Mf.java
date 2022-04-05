package sosp.algorithm;

import java.util.ArrayList;
import java.util.Comparator;

import sosp.jobs.Job;
import sosp.jobs.MapTask;
import sosp.jobs.ReduceTask;
import sosp.main.Priority;
import sosp.main.Scheduler;
import sosp.main.Settings;
import sosp.main.Topology;
import sosp.network.Flow;

public class Mf implements Algorithm {

	private int nReadyTasks = 0;
	private int nFreeSlots = 0;
	
	static public int enoughSlot=0, total=0;
	
	private class MapperDistribution{
		public int[] nMapper = new int[Settings.nHosts];
	}
	
	private MapperDistribution[] mapperDist = null;

	
	@Override
	public HostAndTask allocateHostAndTask() {
		// why does the nHosts equal nReplication?
		assert(Settings.nHosts == Settings.nReplications);
		if(mapperDist==null) {
			mapperDist = new MapperDistribution[Scheduler.jobs.length];
			for(int i=0;i<mapperDist.length;++i)
				mapperDist[i] = new MapperDistribution();
		}
		
		Job chosenJob = Algorithm.fairJobSelector();
		if(chosenJob == null)
			return null;
		double[] freeBw = Scheduler.freeBw==null ? Topology.getLinkBw() : Scheduler.freeBw;
		double tp = Topology.getHostThroughput(freeBw);
		boolean isCongested = tp>0.9; // TODO move this parameter to Settings class 
		boolean isMapper = chosenJob.hasPendingMappers_const();
		nReadyTasks = countReadyTasks(); 
		nFreeSlots = Scheduler.countFreestHost_const();

		if(nFreeSlots==0)
			return null;
		
		
		++total;
		if(nReadyTasks <= nFreeSlots) { // sufficient slot
			++enoughSlot;
			if(isCongested) {
				if(isMapper) {
					int host = getMapperHost(chosenJob);
					MapTask chosenTask = chosenJob.pendingMapperList.get(0);
					//return new HostAndTask(host, chosenTask);
					return Algorithm.delayScheduling(chosenJob);
				} else {
					int host = getReducerHost(chosenJob);
					ReduceTask chosenTask = null;
					for(ReduceTask task:chosenJob.pendingReducerList) {
						if(chosenTask==null || task.macroflow.size<chosenTask.macroflow.size) // lt
							chosenTask = task;
					}
					return new HostAndTask(host, chosenTask);
					//return new HostAndTask(host, chosenJob.pendingReducerList.get(0));
				}
			}else {
				if(isMapper) {
					int host = getMapperHost(chosenJob);
					MapTask chosenTask = chosenJob.pendingMapperList.get(0);
					//return new HostAndTask(host, chosenTask);
					return Algorithm.delayScheduling(chosenJob);
				} else {
					int host = getReducerHost(chosenJob);
					ReduceTask chosenTask = null;
					for(ReduceTask task:chosenJob.pendingReducerList) {
						if(chosenTask==null || task.macroflow.size>chosenTask.macroflow.size) // gt
							chosenTask = task;
					}
					return new HostAndTask(host, chosenTask);
					//return new HostAndTask(host, chosenJob.pendingReducerList.get(0));
				}
			}
		} else { // insufficient slot
			if(isMapper) {
				int host = getMapperHost(chosenJob);
				MapTask chosenTask = chosenJob.pendingMapperList.get(0);
				//return new HostAndTask(host, chosenTask);
				return Algorithm.delayScheduling(chosenJob);
			} else {
				int host = getReducerHost(chosenJob);
				ReduceTask chosenTask = null;
				for(ReduceTask task:chosenJob.pendingReducerList) {
					if(chosenTask==null || task.macroflow.size<chosenTask.macroflow.size) // lt
						chosenTask = task;
				}
				return new HostAndTask(host, chosenTask);
				//return new HostAndTask(host, chosenJob.pendingReducerList.get(0));
			}
		}
	}
	
	private int countReadyTasks() {
		int cnt = 0;
		for(Job job:Scheduler.activeJobs)
			cnt += job.countReadyTasks_const();
		return cnt;
	}
	
	private int getMapperHost(Job job) {
		ArrayList<Integer> a = new ArrayList<Integer>();
		int[] distribution = mapperDist[job.jobId].nMapper;
		for(int i=0;i<distribution.length;++i) {
			if(Scheduler.freeSlots[i]==0) continue;
			if(a.size()==0) { a.add(i); continue; } 
			int best = distribution[a.get(0)];
			if(distribution[i]==best) {
				a.add(i);
			}else if(distribution[i] < best) {
				a.clear(); a.add(i);
			}
		}
		a.sort(new Comparator<Integer>(){ // ascending order
			@Override public int compare(Integer arg0, Integer arg1) {
				return Scheduler.freeSlots[arg0]-Scheduler.freeSlots[arg1];
			}
		});
		assert(!a.isEmpty());
		return a.get(a.size()-1);
	}
	
	private int getReducerHost(Job job) {
		int DOWN = Settings.nHosts;
		int chosen = -1;
		for(int i=0;i<Settings.nHosts;++i) {
			if(Scheduler.freeSlots[i]==0) continue;
			if(chosen<0) { chosen=i; continue;}
			//if(Scheduler.freeBw[DOWN+i] > Scheduler.freeBw[DOWN+chosen])
			if(Scheduler.freeSlots[i] > Scheduler.freeSlots[chosen])
				chosen = i;
		}
		assert(chosen>=0);
		return chosen;
	}

	@Override
	public void releaseHost(HostAndTask ht) {
		// nothing to do
	}

	@Override
	public ArrayList<Flow>[] getPriority() {
		if(Settings.nPriorities>0) {// condition should be averaged over time!
			//if(nReadyTasks <= nFreeSlots) // sufficient slot)
			if(Scheduler.countFreestHost_const()>0)
				return Priority.HybridPriority(1);
			else
				return Priority.HybridPriority(0);
		} else {
			//if(nReadyTasks <= nFreeSlots) // sufficient slot)
			if(Scheduler.countFreestHost_const()>0)
				return Priority.infinitePrioritiesCoflow();
			else
				return Priority.infinitePrioritiesMf();
		}
	}

}
