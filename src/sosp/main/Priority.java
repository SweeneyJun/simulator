package sosp.main;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

import sosp.jobs.Job;
import sosp.jobs.MapTask;
import sosp.jobs.ReduceTask;
import sosp.network.Coflow;
import sosp.network.Flow;
import sosp.network.Macroflow;

public class Priority {

	private static double[] ifThres = null;		// input Flow threshold
	private static double[] coflowThres = null;	// coflow threshold
	private static double[] mfThres = null;		// macroflow threshold
	
	public static PrintWriter cout = null;
	static {
		try {
			cout = new PrintWriter(new FileOutputStream("prio.txt"));
		} catch (FileNotFoundException e) {
			assert(false);
		}
	}
	
	/*public static ArrayList<Flow>[] getPrioritizedFlows (double coflowWeight){
		if(coflowThres==null)
			coflowThres = expThresholdsCoflow();
		if(mfThres==null)
			mfThres = expThresholdsMf();
		
		ArrayList<Flow>[] flows = null;
		if(Settings.nPriorities>0){
			//assert(Settings.nPriorities == 1);
			//flows = onePriorities(activeMfs);
			switch(Settings.schedulingMode){
			case coflow: 
				//flows = MultiPrioritiesCoflow();
				flows = HybridPriority(1);
				break;
			case macroflow:
				//flows = MultiPrioritiesMf();
				flows = HybridPriority(0);
				break;
			case hybrid:
				//flows = nFreeSlots>0.05 ? MultiPrioritiesCoflow() : MultiPrioritiesMf();
				flows = HybridPriority(coflowWeight);
				break;
			default:
				assert(false);
			}
		}else{
			switch(Settings.schedulingMode){
			case coflow: 
				flows = infinitePrioritiesCoflow();
				break;
			case macroflow:
				flows = infinitePrioritiesMf();
				break;
			case hybrid:
				assert(false);
			default:
				assert(false);
			}
		}
		return flows;
	}*/
	
	
	/*private static ArrayList<Flow>[] onePriorities(){
		assert(false);// TODO remove this function 
		@SuppressWarnings("unchecked") ArrayList<Flow>[] activeFlows = new ArrayList[1];
		ArrayList<Flow> flows = new ArrayList<Flow>();
		for(int i=0;i<Scheduler.activeReducers.size();++i){
			Macroflow mf = Scheduler.activeReducers.get(i).macroflow;
			if(mf.finishTime>=0)
				continue;
			for(Flow flow:mf.flows){
				if(flow.finishTime>=0)
					continue;
				flows.add(flow);
			}
		}
		activeFlows[0] = flows;
		return activeFlows;
	}*/
	
	/*private static ArrayList<Flow>[] MultiPrioritiesMf(){
		assert(false);// TODO remove this function 
		ArrayList<Flow>[] flows = infinitePrioritiesMf();
		@SuppressWarnings("unchecked") ArrayList<Flow>[] activeFlows = new ArrayList[Settings.nPriorities];
		for(int i=0;i<Settings.nPriorities;++i){
			activeFlows[i] = new ArrayList<Flow>();
			int s = i*flows.length/Settings.nPriorities;
			int t = (i+1)*flows.length/Settings.nPriorities;
			for(int j=s;j<t;++j)
				for(Flow flow:flows[j])
					activeFlows[i].add(flow);
		}
		for(ArrayList<Flow>f:activeFlows)
			cout.print(f.size()+" ");
		cout.println();
		cout.flush();
		return activeFlows;
	}*/
	
	/*private static ArrayList<Flow>[] MultiPrioritiesCoflow(){
		assert(false);// TODO remove this function 
		ArrayList<Flow>[] flows = infinitePrioritiesCoflow();
		@SuppressWarnings("unchecked") ArrayList<Flow>[] activeFlows = new ArrayList[Settings.nPriorities];
		for(int i=0;i<Settings.nPriorities;++i){
			activeFlows[i] = new ArrayList<Flow>();
			int s = i*flows.length/Settings.nPriorities;
			int t = (i+1)*flows.length/Settings.nPriorities;
			for(int j=s;j<t;++j)
				for(Flow flow:flows[j])
					activeFlows[i].add(flow);
		}
		for(ArrayList<Flow>f:activeFlows)
			cout.print(f.size()+" ");
		cout.println();
		cout.flush();
		return activeFlows;
	}*/

	// return active Flows in the infinitePrioritiesMf scene
	public static ArrayList<Flow>[] infinitePrioritiesMf(){
		Collections.sort(Scheduler.activeReducers, new Comparator<ReduceTask>(){
			@Override public int compare(ReduceTask arg0, ReduceTask arg1) {
				return arg1.macroflow.size==arg0.macroflow.size ? 0 : (arg1.macroflow.size<arg0.macroflow.size ? -1 : 1);
			}
		});
		// activeFlows is 2-dimension ArrayList
		@SuppressWarnings("unchecked") ArrayList<Flow>[] activeFlows = new ArrayList[Scheduler.activeReducers.size()];
		for(int i=0;i<Scheduler.activeReducers.size();++i){
			activeFlows[i] = new ArrayList<Flow>();
			Macroflow mf = Scheduler.activeReducers.get(i).macroflow;
			if(mf.finishTime>=0)
				continue;
			for(Flow flow:mf.flows){
				if(flow.finishTime>=0)
					continue;
				activeFlows[i].add(flow);
			}
		}
		for(int i = 0; i < activeFlows.length; i ++) {
			if (activeFlows[i] == null){
				System.out.println("1111111");
			}
		}
		return activeFlows;
	}

	// return active Flows in the infinitePrioritiesCoflow scene
	public static ArrayList<Flow>[] infinitePrioritiesCoflow(){
		Collections.sort(Scheduler.activeJobs, new Comparator<Job>(){
			@Override public int compare(Job arg0, Job arg1) {
				return arg1.coflow.size==arg0.coflow.size ? 0 : (arg1.coflow.size<arg0.coflow.size ? -1 : 1);
			}
		});
		int[] coflowOrder = new int[2333]; // maximum possible number of coflows

		// alloc the sorted order to jobId sequence
		for(int i=0;i<Scheduler.activeJobs.size();++i)
			coflowOrder[Scheduler.activeJobs.get(i).jobId] = i;
		
		@SuppressWarnings("unchecked") ArrayList<Flow>[] activeFlows = new ArrayList[Scheduler.activeJobs.size()];
		for(int i=0;i<activeFlows.length;++i)
			activeFlows[i] = new ArrayList<Flow>();
		for(ReduceTask reducer:Scheduler.activeReducers){
			Macroflow mf = reducer.macroflow;
			if(mf.finishTime>=0)
				continue;
			for(Flow flow:mf.flows){
				if(flow.finishTime>=0)
					continue;
				activeFlows[coflowOrder[mf._coflow._job.jobId]].add(flow);
			}
		}
		return activeFlows;
	}
	
	// calculate the queues' threshold using Coflow size
	private static double[] expThresholdsCoflow(){
		assert(Settings.nPriorities-1 >= 0);
		double[] thres = new double[Settings.nPriorities-1];
		double min = Double.POSITIVE_INFINITY, max = 0;
		for(Job job:Scheduler.jobs){
			Coflow coflow = job.coflow;
			min = Math.min(min, coflow.size);
			max = Math.max(max, coflow.size);
		}
		double normMax = max/min;
		double e = Math.pow(normMax, 1.0/Settings.nPriorities);
		for(int i=1;i<Settings.nPriorities;++i)
			thres[i-1] = Math.pow(e, i)*min;
		return thres;
	}

	// calculate the queues' threshold using Macroflow size
	private static double[] expThresholdsMf(){
		assert(Settings.nPriorities-1 >= 0);
		double[] thres = new double[Settings.nPriorities-1];
		double min = Double.POSITIVE_INFINITY, max = 0;
		for(Job job:Scheduler.jobs){
			Coflow coflow = job.coflow;
			for(Macroflow mf:coflow.macroflows){
				min = Math.min(min, mf.size);
				max = Math.max(max, mf.size);
			}
		}
		double normMax = max/min;
		double e = Math.pow(normMax, 1.0/Settings.nPriorities);
		for(int i=1;i<Settings.nPriorities;++i)
			thres[i-1] = Math.pow(e, i)*min;
		return thres;
	}

	private static double[] expThresholdsInputFlow(int nRealMappers){
		double[] thres = expThresholdsCoflow();
		for(int i = 0; i < thres.length; ++i){
			thres[i] /= nRealMappers;
		}
		return thres;
	}

	// get the priority
	private static int queryPriority(double size, double[] thres){ // prio \in [0,thres.length)
		int prio = 0;
		for(int i=thres.length-1; i>=0; --i){
			if(size < thres[i])
				++prio;
			else
				break;
		}
		return prio;
	}

	// the Hybrid means that we schedule flows using hybrid methods: coflow schedule and macroflow schedule
	public static ArrayList<Flow>[] HybridPriority(double weight_of_coflow){ // weight_of_coflow \in [0,1]
		if(coflowThres==null)
			coflowThres = expThresholdsCoflow();
		if(mfThres==null)
			mfThres = expThresholdsMf();
		
		@SuppressWarnings("unchecked") ArrayList<Flow>[] activeFlows = new ArrayList[Settings.nPriorities];
		for(int i=0;i<activeFlows.length;++i)
			activeFlows[i] = new ArrayList<Flow>();
		for(ReduceTask reducer:Scheduler.activeReducers){
			Macroflow mf = reducer.macroflow;
			if(mf.finishTime>=0)
				continue;
			double coflowPriority = queryPriority(mf._coflow.size, coflowThres);
			double mfPriority = queryPriority(mf.size, mfThres);
			// the priority is a hybrid one between coflow and macroflow
			int prio = (int) Math.round( weight_of_coflow*coflowPriority + (1-weight_of_coflow)*mfPriority );
			for(Flow flow:mf.flows){
				if(flow.finishTime>=0)
					continue;
				activeFlows[prio].add(flow);
			}
		}

		for(ArrayList<Flow>f:activeFlows)
			cout.print(f.size()+" ");
		cout.println();
		cout.flush();
		return activeFlows;
	}

	// wcx: 抽象成一个远端节点后似乎不需要考虑这个流优先级了
//	public static ArrayList<Flow>[] InputPriority(int nRealMappers){
//		assert(Settings.isSeparate);
//		assert(SeparateScheduler.jobs.length > 0);
//
//		if(ifThres == null){
//			ifThres = expThresholdsInputFlow(nRealMappers);
//		}
//		@SuppressWarnings("unchecked") ArrayList<Flow>[] activeFlows = new ArrayList[Settings.nPriorities];
//		for(int i = 0; i < activeFlows.length; ++i){
//			activeFlows[i] = new ArrayList<Flow>();
//		}
//		for(MapTask mapper: SeparateScheduler.arrivedMappers){
//
//		}
}

