package sosp.algorithm;

import java.util.ArrayList;

import sosp.jobs.Job;
import sosp.jobs.ReduceTask;
import sosp.main.Priority;
import sosp.main.Scheduler;
import sosp.main.Settings;
import sosp.main.Topology;
import sosp.network.Coflow;
import sosp.network.Flow;
import sosp.network.Macroflow;

public class NeatGlobal implements Algorithm {

	private ArrayList<Coflow>[] link_cf = null; // coflow list on each link
	private double[][] link_cf_size = null; // current emitted size of the i-th coflow, on the j-th link
	
	double weight = 0;
	
	public static long allocationTime = 0;
	
	public NeatGlobal(String mode) {
		if(mode.equalsIgnoreCase("fair"))
			weight = 1;
		else if(mode.equalsIgnoreCase("pmpt"))
			weight = 0;
		else
			assert(false);
	}
	
	private double time = 0;
	private ArrayList<HostAndTask> pending = new ArrayList<HostAndTask>();
	
	@SuppressWarnings("unchecked")
	@Override
	public HostAndTask allocateHostAndTask() {
		double[] bw = Topology.getLinkBw();
		if(link_cf==null){
			link_cf = new ArrayList[bw.length];
			for(int i=0;i<link_cf.length;++i)
				link_cf[i] = new ArrayList<Coflow>();
			link_cf_size = new double[Scheduler.jobs.length][bw.length];
		}
		
		// 1. find a job and task
		// zy: why?
		assert(Algorithm.countJobWithReadyTasks()<=1); // there cannot have more than 1 job that has ready tasks
		//Algorithm.countJobWithReadyTasks();
		Job chosenJob = Algorithm.fairJobSelector();
		if(chosenJob == null)
			return null;
		//System.out.println(Scheduler.time + " " + Scheduler.countFreestHost_const());
		if(chosenJob.hasPendingMappers_const()) // mapper
			return Algorithm.delayScheduling(chosenJob);

		allocationTime -= System.nanoTime();
		
		// 2. find the matching between reducers and slots
		if(!pending.isEmpty()) {
			assert(Math.abs(Scheduler.time - time) < Settings.epsilon);
			HostAndTask ht = pending.get(0);
			pending.remove(ht);
			allocationTime += System.nanoTime();
			return ht;
		}

		assert(chosenJob.hasPendingTasks_const()); // must have ready reducers
		time = Scheduler.time;
		int needed = chosenJob.pendingReducerList.size();
		ArrayList<Integer[]> slots = enumSlotPosition(needed);
		ArrayList<int[]> perms = permReducers(needed);
		
		double bestDelta = Double.POSITIVE_INFINITY; // best task completion time forecast
		Integer[] bestSlot = null; // best hosts to place the task
		int[] bestPerm = null; // best permutation to place the task
		double[] bestLinkSize = null; // best coflow distribution over link
		
		for(Integer[] slot:slots) {
			for(int[] perm:perms) {
				//System.out.println("*");
				double delta = 0;
				double[] deltaLink = new double[bw.length]; // completion time forecast
				double[] linkSize = new double[bw.length]; // coflow size distribution over link, for current job
				// calculate linkSize
				for(int r = 0;r<chosenJob.pendingReducerList.size();++r) {
					ReduceTask reducer = chosenJob.pendingReducerList.get(r);
					Macroflow mf = reducer.macroflow;
					int receiver = slot[perm[r]];
					for(Flow flow : mf.flows) {
						int[] linkList = Topology.getPath(flow.sender, receiver);
						for(int j=0;j<linkList.length;++j){
							int link = linkList[j];
							linkSize[link] += flow.size;
						}
					}
				}
				// optimization
				for(int link=0;link<deltaLink.length;++link) {
					deltaLink[link] += linkSize[link];
					for(Coflow coflow : link_cf[link]){
						int c = coflow._job.jobId;
						if(coflow.size < chosenJob.coflow.size)
							deltaLink[link] += link_cf_size[c][link] + linkSize[link]*coflow.size/chosenJob.coflow.size*weight;
						else
							deltaLink[link] += link_cf_size[c][link]*chosenJob.coflow.size/coflow.size*weight + linkSize[link];
					}
					deltaLink[link] /= bw[link];
					delta = Math.max(delta, deltaLink[link]);
				}
				// compare
				if(delta < bestDelta) {
					bestDelta = delta;
					bestSlot = slot;
					bestPerm = perm;
					bestLinkSize = linkSize;
				}
			}
		}
		
		// 3. update information
		for(int r = 0;r<chosenJob.pendingReducerList.size();++r) {
			ReduceTask reducer = chosenJob.pendingReducerList.get(r);
			Macroflow mf = reducer.macroflow;
			int receiver = bestSlot[bestPerm[r]];
			for(Flow flow : mf.flows) {
				int[] linkList = Topology.getPath(flow.sender, receiver);
				for(int i=0;i<linkList.length;++i){
					int link = linkList[i];
					if(!link_cf[link].contains(mf._coflow))
						link_cf[link].add(mf._coflow);
					link_cf_size[chosenJob.jobId][link] += bestLinkSize[link];
				}
			}
			pending.add(new HostAndTask(receiver,reducer));
		}
		HostAndTask ht = pending.get(0);
		pending.remove(ht);
		allocationTime += System.nanoTime();
		return ht;
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
	}
	
	
	//---- Slots Selection ----
	
	private ArrayList<Integer[]> slotsList = new ArrayList<Integer[]>(); // the list of chosen slots
	private int nNeededSlots = 0;
	private int[] d = null;

	// return n slot positions using dfs/array traversal
	private ArrayList<Integer[]> enumSlotPosition(int n) { // n: the number of needed slots
		System.out.println("n: " + n + " free slots: " + Scheduler.countFreestHost_const());
		assert(n<=Scheduler.countFreestHost_const());
		//assert(Scheduler.countFreestHost_const()<=100); // cannot run too large example
		slotsList.clear();
		nNeededSlots = n;
		d = new int[Settings.nHosts];
		dfs(0,0);
		return slotsList;
	}

	// you can see it as array traversal
	private void dfs(int p, int sum) {
		if(p==d.length) {
			if(sum==nNeededSlots) {
				ArrayList<Integer> x = new ArrayList<Integer>();
				for(int i=0;i<d.length;++i)
					for(int k=0;k<d[i];++k)
						x.add(i);
				slotsList.add(x.toArray(new Integer[0]));
			}
			return;
		}
		for(int i=0;i<=Scheduler.freeSlots[p];++i) {
			if(sum+i>nNeededSlots)
				break;
			d[p] = i;
			dfs(p+1,sum+i);
		}
	}
	
	
	//---- Reducer Permutation ----
	// e.g. n = 3, so a = [0, 1, 2]
	// a's permutation:
	// [0, 1, 2], [0, 2, 1], [1, 0, 2], [1, 2, 0], [2, 1, 0], [2, 0, 1]
	private ArrayList<int[]> perm = new ArrayList<int[]>();
	
	private ArrayList<int[]> permReducers(int n){
		perm.clear();
		int[] a = new int[n];
		for(int i=0;i<a.length;++i)
			a[i] = i;
		permutation(a,0);
		return perm;
	}
	
	private void swap(int[] a, int i, int j) {  
		int temp = a[i]; a[i] = a[j]; a[j] = temp;  
	} 
	
	private void permutation (int[] a, int st) {  
		if (st == a.length-1)  {  
			perm.add(a.clone()); 
			return;
		}  
		for (int i=st; i<a.length; ++i) {  
			swap(a, st, i);  
			permutation(a, st + 1);  
			swap(a, st, i);  
		}  
	}  
	
	

}
