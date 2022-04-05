package sosp.algorithm;

import java.util.*;

import sosp.jobs.Job;
import sosp.jobs.ReduceTask;
import sosp.main.*;
import sosp.network.Coflow;
import sosp.network.Flow;
import sosp.network.Macroflow;

public class Neat implements Algorithm {

	private ArrayList<Coflow>[] link_cf = null; // coflow list on each link
	//private double[] s = null; // current emitted size of a coflow 
	private double[][] link_cf_size = null; // current emitted size of the i-th coflow, on the j-th link
	
	double weight = 0;
	
	public static long allocationTime = 0;
	
	public Neat(String mode) {
		if(mode.equalsIgnoreCase("fair"))
			weight = 1;
		else if(mode.equalsIgnoreCase("pmpt"))
			weight = 0;
		else
			assert(false);
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public HostAndTask allocateHostAndTask() {
		double[] bw = Topology.getLinkBw();
		if(link_cf==null){
			link_cf = new ArrayList[bw.length];
			for(int i=0;i<link_cf.length;++i)
				link_cf[i] = new ArrayList<Coflow>();
			//s = new double[Scheduler.jobs.length];
			link_cf_size = new double[Scheduler.jobs.length][bw.length];
		}
		
		// 1. find a job and task
		//Job chosenJob = Algorithm.fairJobSelector();
//		Job chosenJob = Settings.fairJobScheduler ? Algorithm.fairJobSelector() : Algorithm.fifoJobSelector();

		Job chosenJob = Algorithm.jobSelector();

		if(chosenJob == null)
			return null;
		if(chosenJob.hasPendingMappers_const()) {// mapper
			if(Algorithm.delayScheduling(chosenJob) == null) {
				int host = -1;
				for(int i = 0; i< Scheduler.freeSlots.length; ++i){
					if(Scheduler.freeSlots[i]==0)
						continue;
					host = (host<0) ? i : host;
					if(Scheduler.freeSlots[host]<Scheduler.freeSlots[i])
						host = i;
				}
				if(host<0)
					return null;
				System.out.println("fasdjfklasjfkljasklfjlksajlfk");
				return new HostAndTask(host, chosenJob.pendingMapperList.get(0));
			}
			else{
				return Algorithm.delayScheduling(chosenJob);
			}
		}
		ReduceTask chosenTask = null;
		// biggest task first
//		for(ReduceTask task : chosenJob.pendingReducerList){
//			if(chosenTask == null)
//				chosenTask = task;
//			else
//				chosenTask = (task.macroflow.size > chosenTask.macroflow.size) ? task : chosenTask;
//		}
        chosenTask = chosenJob.pendingReducerList.get(0);
		Macroflow mf = chosenTask.macroflow;

		// allocation time = toc - tic = toc + (-tic)
		allocationTime -= System.nanoTime();

		// 2. find a host for reducer
		double bestDelta = Double.POSITIVE_INFINITY; // best task completion time forecast
		int bestSlot = -1; // best host to place the task
		double[] bestLinkSize = null; // the best size on each link after choosing the best host

		for(int i=0;i<Scheduler.freeSlots.length;++i){
			if(Scheduler.freeSlots[i]==0)
				continue;
			double[] deltaLinkSize = new double[bw.length]; // flow size delta in each link
			double[] linkSize = new double[bw.length]; // macroflow size distribution in each link
			double delta = 0; // time delta
			boolean[] relatedLink = new boolean[bw.length]; // macroflow related link
			for(Flow flow : mf.flows){
					int[] linkList = Topology.getPath(flow.sender, i);
					for(int k=0;k<linkList.length;++k){
						int link = linkList[k];
						linkSize[link] += flow.size;
						relatedLink[link] = true;
					}
			}
			for(int link=0;link<deltaLinkSize.length;++link){
				if(!relatedLink[link])
					continue;
				int selectedJob = chosenTask._job.jobId;
				double selectedCoflowSize = chosenTask._job.coflow.size;
				// selected job link size change during the selected job running
				deltaLinkSize[link] = link_cf_size[selectedJob][link] + linkSize[link];
				for(Coflow coflow : link_cf[link]){
					int job = coflow._job.jobId;
					double coflowSize = coflow.size;
					if(job == selectedJob)
						continue;
					// related job link size change during the selected job running
					if(coflowSize <= selectedCoflowSize)
						deltaLinkSize[link] += link_cf_size[job][link] + (link_cf_size[selectedJob][link]+linkSize[link])*coflowSize/selectedCoflowSize * weight;
					else
						deltaLinkSize[link] += link_cf_size[job][link]*(selectedCoflowSize)/coflowSize*weight + (link_cf_size[selectedJob][link]+linkSize[link]);
				}
				deltaLinkSize[link] /= bw[link];
				delta = Math.max(delta, deltaLinkSize[link]);
			}
			//System.out.print(delta+ (i==Scheduler.freeSlots.length-1?"\n":" "));
			if(delta<bestDelta){
				bestDelta = delta;
				bestSlot = i;
				bestLinkSize = linkSize;
			}
		}
		if(Double.isInfinite(bestDelta)){
			allocationTime += System.nanoTime();
			return null;
		}

		// 3. update information
		//s[chosenTask._job.jobId] += mf.size;
		for(Flow flow:mf.flows){
			int[] linkList = Topology.getPath(flow.sender, bestSlot);
			for(int i=0;i<linkList.length;++i){
				int link = linkList[i];
				if(!link_cf[link].contains(mf._coflow))
					link_cf[link].add(mf._coflow);
				link_cf_size[chosenTask._job.jobId][link] += bestLinkSize[link];
			}
		}
		allocationTime += System.nanoTime();
		return new HostAndTask(bestSlot, chosenTask);
	}

	@Override
	public void releaseHost(HostAndTask ht) {
		if(ht.task instanceof ReduceTask){
			ReduceTask reducer = (ReduceTask) ht.task;
			Coflow c = reducer._job.coflow;
			for(ArrayList<Coflow> link:link_cf)
				link.remove(c);
		}
	}

	@Override
	public ArrayList<Flow>[] getPriority() {
		if(Settings.nPriorities>0)
			return Priority.HybridPriority(1);
		else
			return Priority.infinitePrioritiesCoflow();

//		// FIFO priority
//		int nPriority = Settings.nPriorities;
//		ArrayList<Flow>[] activeFlows = new ArrayList[nPriority];
//		for(int i=0;i<activeFlows.length;++i)
//			activeFlows[i] = new ArrayList<Flow>();
//
//		ArrayList<Integer> jobid = new ArrayList<>();
//		for (Job job :Scheduler.activeJobs) {
//			jobid.add(job.jobId);
//		}
//
//		Collections.sort(jobid);
//
//		for (ReduceTask rt: Scheduler.activeReducers) {
//			int a = jobid.indexOf(rt._job.jobId);
//			for (Flow flow: rt.macroflow.flows){
//				if(flow.finishTime>=0)
//					continue;
//				if (nPriority - a - 1 >= 0){
//					activeFlows[nPriority - a - 1].add(flow);
//				}
//				else {
//					activeFlows[0].add(flow);
//				}
//			}
//		}
//		return activeFlows;
	}
}
