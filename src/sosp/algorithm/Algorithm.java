package sosp.algorithm;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;

import sosp.jobs.Job;
import sosp.jobs.JobQueue;
import sosp.jobs.MapTask;
import sosp.jobs.Task;
import sosp.main.Scheduler;
import sosp.main.TestSunderer;
import sosp.main.Settings;
import sosp.network.Flow;

public interface Algorithm { 
	// task placement & network scheduling 
	
	public class HostAndTask{
		public int host = -1;
		public Task task = null;
		public HostAndTask(int h, Task t) { host=h; task=t; }
	}

	public HostAndTask allocateHostAndTask();
	public void releaseHost(HostAndTask ht);
	public ArrayList<Flow>[] getPriority();


	public static int estimateRealMapperNumber(double shuffleSizeMB, int nMapperHosts){
		// we assume the I/O ratio of map-phase is 1, and the input data size of each map-task is 100MB
		// wcx: 这里后面可以考虑根据一小段job的分布提取一个job shuffle一般多大的先验, 通过I/O比率转化为Input Size后, 再动态决定每个map-task应该多大
		return Math.max((int)Math.ceil(shuffleSizeMB/Settings.mapperSizeMB), nMapperHosts);
	}
	
	public static double estimateMapperTime(double mapperSizeMB){
		// we assume 60 seconds are needed to process 100MB input data
		return mapperSizeMB*Settings.mapperCompTimePerMB;
	}

	// choose a least load job to place a map task on its locality host
	public static Job fairJobSelector(){
		Job chosenJob = null;
		for(Job job:TestSunderer.activeJobs){ // find an active job with un-emitted reducers
			// check mapper task and reducer task
			if(!job.hasPendingTasks_const())
				continue; // no pending tasks
			if(!job.hasPendingMappers_const() && job.mapStageFinishTime<0)
				continue; // no pending mappers but not all mappers finished 
			if(job.notInputMapperList.size() > 0){
			}
			if(chosenJob==null || job.nActiveTasks_const() < chosenJob.nActiveTasks_const())
				chosenJob = job;
		}
		return chosenJob;
	}

	public static Job jobSelector(){

		ArrayList<JobQueue> jobQueues = new ArrayList<>();
		jobQueues = TestSunderer.jobQueues;

		Collections.sort(jobQueues, new Comparator<JobQueue>() {
			@Override
			public int compare(JobQueue o1, JobQueue o2) {
				return Double.compare(o1.resourceRatio(), o2.resourceRatio());
			}
		});

		for (JobQueue jobQueue: TestSunderer.jobQueues) {
			if (1.0*jobQueue.nActiveTasks()/Settings.nSlots/Settings.nHosts > jobQueue.maxResource) {
				continue;
			}
			Job[] jobs = jobQueue.jobSorter();
			Job chosenJob = null;
			for (Job job : jobs) { // find an active job with un-emitted reducers
				// check mapper task and reducer task
				if (!job.hasPendingTasks_const())
					continue; // no pending tasks
				if (!job.hasPendingMappers_const() && job.mapStageFinishTime < 0)
					continue; // no pending mappers but not all mappers finished
				if (job.hasPendingMappers_const()) {
					boolean hostFree = false;
					for (MapTask mt : job.pendingMapperList) {
						for (int i = 0; i < mt.hdfsHost.length; ++i)
							hostFree = hostFree || (TestSunderer.freeSlots[mt.hdfsHost[i]] > 0);
						if (hostFree) break;
					}
					if (!hostFree)
						continue; // job in map-stage, but corresponding HDFS hosts have no slot
				}
				if (jobQueue.mode.equals("FIFO")) {
					if (chosenJob == null || job.jobId < chosenJob.jobId)
						chosenJob = job;
				}
				else {
					if (chosenJob == null || job.nActiveTasks_const() < chosenJob.nActiveTasks_const())
						chosenJob = job;
				}
			}
			if (chosenJob != null) {
				if (chosenJob.mapStageFinishTime > 0 && jobQueue.mode.equals("FIFO") && !jobQueue.canTransfer(chosenJob)){
					continue;
				}
				return chosenJob;
			}
		}
		return null;
	}

	// return the number of job that haven't finish its map phase
	public static int countJobWithReadyTasks() {
		int n=0;
		for(Job job:TestSunderer.activeJobs){ // find an active job with un-emitted reducers
			if(!job.hasPendingTasks_const())
				continue; // no pending tasks
			if(!job.hasPendingMappers_const() && job.mapStageFinishTime<0)
				continue; // no pending mappers but not all mappers finished 
			++n;
		}
		return n;
	}

	// sort job according to its estimated running time
	public static Job[] jobSorter(){
		Job[] jobs = TestSunderer.activeJobs.toArray(new Job[0]);
		Arrays.sort(jobs, new Comparator<Job>(){
			@Override public int compare(Job arg0, Job arg1) {
				double a = arg0.estimatedRunningTime - arg1.estimatedRunningTime;
				return a>0?1:(a<0?-1:0);
			}
		});
		return jobs;
	}

	// sort job according to its number of active tasks
	public static Job[] jobSorterFair(){
		Job[] jobs = TestSunderer.activeJobs.toArray(new Job[0]);
		Arrays.sort(jobs, new Comparator<Job>(){
			@Override public int compare(Job arg0, Job arg1) {
//				if (arg0.nActiveTasks_const() != arg1.nActiveTasks_const()) {
					return arg0.nActiveTasks_const() - arg1.nActiveTasks_const();
//				}
//				else {
//					return Double.compare(arg0.coflow.size, arg1.coflow.size);
//				}
			}
		});
		return jobs;
	}

	public static Job[] jobSorterFifo() {
		Job[] jobs = TestSunderer.activeJobs.toArray(new Job[0]);
		Arrays.sort(jobs, new Comparator<Job>(){
			@Override public int compare(Job arg0, Job arg1) {
				return arg0.jobId - arg1.jobId;
			}
		});
		return jobs;
	}

	public static Job[] jobSorterSJF() {
		Job[] jobs = TestSunderer.activeJobs.toArray(new Job[0]);
		Arrays.sort(jobs, new Comparator<Job>(){
			@Override public int compare(Job arg0, Job arg1) {
				return Double.compare(arg0.coflow.size, arg1.coflow.size);
			}
		});
		return jobs;
	}

	// delay schedule map task to its locality host
	public static HostAndTask delayScheduling(Job job){
		assert(job.hasPendingMappers_const());
		//Task chosenTask = job.pendingMapperList.get(0); // get the 1st pending task
		// re-find a HDFS host
		//int host = -1;
		for(MapTask mt: job.pendingMapperList) {
			for(int i=0;i<mt.hdfsHost.length;++i) 
				if(TestSunderer.freeSlots[mt.hdfsHost[i]]>0)
					return new HostAndTask(mt.hdfsHost[i], mt);
		}
		assert(false);
		return null;
	}

	public static Job smallestJobSelector(){
		Job[] jobs = jobSorter();
		//Job chosenJob = null;
		for(Job job:jobs){ // find an active job with un-emitted reducers
			if(!job.hasPendingTasks_const())
				continue; // no pending tasks
			if(!job.hasPendingMappers_const() && job.mapStageFinishTime<0)
				continue; // no pending mappers but not all mappers finished 
			if(job.hasPendingMappers_const()){
				boolean hostFree = false;
				for(MapTask mt:job.pendingMapperList) {
					for(int i=0;i<mt.hdfsHost.length;++i)
						hostFree = hostFree || (TestSunderer.freeSlots[mt.hdfsHost[i]]>0);
					if(hostFree) break;
				}
				if(!hostFree)
					continue; // job in map-stage, but corresponding HDFS hosts have no slot
			}
			return job;
		}
		return null;
	}

	public static Job fifoJobSelector(){
		Job[] jobs = jobSorterFifo();
		//Job chosenJob = null;
		for(Job job:jobs){ // find an active job with un-emitted reducers
			if(!job.hasPendingTasks_const())
				continue; // no pending tasks
			if(!job.hasPendingMappers_const() && job.mapStageFinishTime<0)
				continue; // no pending mappers but not all mappers finished
			if(job.hasPendingMappers_const()){
				boolean hostFree = false;
				for(MapTask mt:job.pendingMapperList) {
					for(int i=0;i<mt.hdfsHost.length;++i)
						hostFree = hostFree || (TestSunderer.freeSlots[mt.hdfsHost[i]]>0);
					if(hostFree) break;
				}
				if(!hostFree)
					continue; // job in map-stage, but corresponding HDFS hosts have no slot
			}
			return job;
		}
		return null;
	}

//	public static boolean networkIsFull() {
//		int sum = 0;
//		for(int num: TestSunderer.shuffleReducerNum) {
//			sum += num;
//		}
//		return sum == 3*Settings.nHosts;
//	}
}
