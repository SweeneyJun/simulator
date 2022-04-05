package sosp.algorithm;

import java.util.ArrayList;

import sosp.jobs.Job;
import sosp.jobs.Task;
import sosp.main.Priority;
import sosp.main.Scheduler;
import sosp.main.Settings;
import sosp.network.Flow;

public class Default implements Algorithm {

	double weight = 1;
	boolean hybrid = false;
	
	public Default(String mode){
		if(mode.equalsIgnoreCase("coflow"))
			weight=1;
		else if (mode.equalsIgnoreCase("macroflow"))
			weight=0;
		else if (mode.equalsIgnoreCase("hybrid"))
			hybrid = true;
		else
			assert(false);
	}

	@Override
	public HostAndTask allocateHostAndTask() {
		// host
		int host = -1;
		for(int i=0;i<Scheduler.freeSlots.length;++i){
			if(Scheduler.freeSlots[i]==0)
				continue;
			host = (host<0) ? i : host;
			if(Scheduler.freeSlots[host]<Scheduler.freeSlots[i])
				host = i;
		}
		if(host<0)
			return null;
		
		// task
		//Job chosenJob = Algorithm.fairJobSelector();
		Job chosenJob = Settings.fairJobScheduler ? Algorithm.fairJobSelector() : Algorithm.smallestJobSelector();
		Task chosenTask = null;
		if(chosenJob!=null){
			if(chosenJob.hasPendingMappers_const()){
				return Algorithm.delayScheduling(chosenJob);
			}else{
				chosenTask = chosenJob.pendingReducerList.get(0);
			}
		}else{
			return null;
		}
		
		return new HostAndTask(host,chosenTask);
	}


	@Override
	public void releaseHost(HostAndTask ht) {
		// do nothing
	}


	@Override
	public ArrayList<Flow>[] getPriority() {
		if(Settings.nPriorities>0)
			return Priority.HybridPriority(hybrid?(Scheduler.countFreestHost_const()>0?1:0):weight);
		else
			return hybrid? (Scheduler.countFreestHost_const()>0?Priority.infinitePrioritiesCoflow():Priority.infinitePrioritiesMf()) 
					: (weight>0?Priority.infinitePrioritiesCoflow():Priority.infinitePrioritiesMf());
	}

}
