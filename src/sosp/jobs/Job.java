package sosp.jobs;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Map;

import sosp.main.TestSunderer;
import sosp.main.Settings;
import sosp.network.Coflow;
import sosp.network.Macroflow;

public class Job {

	public JobQueue jobQueue;

	// predefined constants
	public int jobId = -1;
	public int userId = 0;

	//public int[] hdfsHosts = null; // input file position of mappers (default: 3 replications) 
	public Coflow coflow = null;
	public double arriveTime = -1;
	public int nMappers = -1;
	public int nReducers = -1;
	
	// runtime constants (modified by only once)
	public double mapStageFinishTime = -1;
	public double reduceStageFinishTime = -1;
	public double estimatedRunningTime = 0;
	
	// runtime variables (modified by many times)
	public int nInputMappers = 0;
	public int nActiveMappers = 0; 
	public int nActiveReducers = 0;
	public ArrayList<MapTask> notInputMapperList = null;
	public ArrayList<MapTask> pendingMapperList = null;
	public ArrayList<ReduceTask> pendingReducerList = null;
	public ArrayList<MapTask>[] emittedMapperList = null; // the j-th mapper on the i-th host, see $hdfsHosts$ for host number
	public ArrayList<ReduceTask> emittedReducerList = null;

	public double mapInputSize = 0;
	
	// global static variables
	public static int nArrivedJobs = 0;
	
	
	// functions
	public Job(int id){
		jobId = id;
	}
	
	public int nActiveTasks_const(){
		return Settings.isSeparate ? nInputMappers + nActiveMappers + nActiveReducers : nActiveMappers + nActiveReducers;
	}
	
	public boolean hasPendingTasks_const(){
		return Settings.isSeparate ? notInputMapperList.size() + pendingMapperList.size()>0 || pendingReducerList.size()>0 : pendingMapperList.size()>0 || pendingReducerList.size()>0;
	}
	
	public int countReadyTasks_const() {
		return mapStageFinishTime<=0 ? pendingMapperList.size() : pendingReducerList.size();
	}
	
	public boolean hasPendingMappers_const(){
		return pendingMapperList.size()>0;
	}

	public void oneMapperBeginInput(int host, MapTask mapper){
		++nInputMappers;
		TestSunderer.InputMappers.add(mapper); // 在步骤4仿照原Scheduler 4.1 和 4.2部分用迭代器移除
		assert(notInputMapperList.remove(mapper)); // java默认禁用assert, 要开启-ea参数
		assert(TestSunderer.switchFreeBw > 1e-6);
		assert(TestSunderer.freeBw[host] > 1e-6);
		double allocatedBw = Math.min(TestSunderer.switchFreeBw, TestSunderer.freeBw[host]);
		TestSunderer.switchFreeBw -= allocatedBw;
		TestSunderer.freeBw[host] -= allocatedBw;
		TestSunderer.totalFreeBw -= allocatedBw;
		mapper.allocatedInputBw = allocatedBw; // 后续要归还switchFreeBw/freeBw[host]/totalFreeBw
		mapper.emit(host, TestSunderer.time);

		mapper.inputStartTime = TestSunderer.time;
		mapper.predictInputTime = mapper.inputSize / mapper.allocatedInputBw;

		BigDecimal bd = new BigDecimal(mapper.predictInputTime);
		mapper.predictInputTime = bd.setScale(3, BigDecimal.ROUND_HALF_UP).doubleValue();
	}
	public void oneMapperEndInput(int host, MapTask mapper){ // TODO Input结束时是否应该开始运行行为? 即原Scheduler Simulator函数里调用oneMapperStarted(ht.host, mapper)的行为, 还是另写过程, 等待思考
		--nInputMappers;
		TestSunderer.switchFreeBw += mapper.allocatedInputBw;
		TestSunderer.freeBw[host] += mapper.allocatedInputBw;
		TestSunderer.totalFreeBw += mapper.allocatedInputBw;

		mapper.inputFinishTime = TestSunderer.time; // 一个mapper只有在input阶段结束后才可以开始计算

		// begin Running
		mapper._job.oneMapperStarted(host, mapper);
		mapper.emit(host, TestSunderer.time);
		TestSunderer.activeMappers.add(mapper);
	}
	
	public void oneMapperStarted(int host, MapTask mapper){
		++nActiveMappers;
		assert(pendingMapperList.remove(mapper)); // 为什么assert(pendingMapperList.remove(mapper))在这个场景中没效果了? answer: java默认禁用assert, 要开启-ea参数
		//for(int i=0;i<Settings.nReplications;++i){
		//	if(hdfsHosts[i]==host){
				emittedMapperList[host].add(mapper);
		//		return;
		//	}
		//}
		//assert(false); // this host has no input file
	}
	
	public void oneMapperFinished(){
		--nActiveMappers;
	}
	
	public void oneReducerStarted(int host, ReduceTask reducer){
		++nActiveReducers;
		assert(pendingReducerList.remove(reducer)); // assert(pendingReducerList.remove(reducer))为什么没用了! answer: java默认禁用assert, 要开启-ea参数
		emittedReducerList.add(reducer);
		System.out.printf("reducer %d of job %d start at time %f\n", reducer.reducerId, reducer._job.jobId, TestSunderer.time);
	}
	
	public void oneReducerFinished(ReduceTask reducer){
		--nActiveReducers;
		System.out.printf("reducer %d of job %d finish at time %f\n", reducer.reducerId, jobId, TestSunderer.time);
	}
	
	public void mapStageFinish(double time){
		mapStageFinishTime = time;
		System.out.printf("mapStage of job %d finish at time %f\n", coflow._job.jobId, time);
		for(Macroflow mf:coflow.macroflows)
			mf.setFlowSize();
	}
	
	public void reduceStageFinish(double time){
		reduceStageFinishTime = time;
	}
	
	public boolean isAllMapperFinished_const(){
		if(pendingMapperList.size()>0)
			return false;
		for(ArrayList<MapTask> list:emittedMapperList)
			for(MapTask m:list)
				if(m.finishTime<0)
					return false;
		return true;
	}
	
	public boolean isAllReducerFinished_const(){
		if(pendingReducerList.size()>0)
			return false;
		for(ReduceTask r:emittedReducerList)
			if(r.computationFinishTime<0)
				return false;
		return true;
	}

	public boolean isAllShuffleFinished_const(){
		if(pendingReducerList.size()>0)
			return false;
		for(ReduceTask r:emittedReducerList)
			if(r.networkFinishTime<0)
				return false;
		return true;
	}

}


/* JOB STRUCTURE
* 	Job
*	|-	mappers[]
*	|	|-	mapper
*	|-	reducers[]
*	|	|-	reducer
*	|	|	|-	macroflow
*	|	|		|-	flows[]
*	|	|			|-	flow
*	|-	coflow
*		|-	macroflows[]
*			|-	macroflow
*				|-	flows[]
*					|-	flow
*/