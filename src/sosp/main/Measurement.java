package sosp.main;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;

import sosp.jobs.Job;
import sosp.jobs.ReduceTask;
import sosp.network.Coflow;
import sosp.network.Flow;
import sosp.network.Macroflow;

public class Measurement {
	
	static long tm = -1;
	static long begin = System.currentTimeMillis();
	
	// Group 1. completion time
	public static void OutputCompletionTime(Job[] jobs){
		ArrayList<Double> jct = new ArrayList<Double>();
		
		for(Job job:jobs){
			jct.add(job.reduceStageFinishTime - job.arriveTime);
			double mt = job.mapStageFinishTime - job.arriveTime;
			double rt = job.reduceStageFinishTime - job.mapStageFinishTime;
			System.out.println("[" + job.jobId+"] "+job.nMappers+" "+job.nReducers +" "+mt+" "+rt+" "+job.coflow.size);
			for(Macroflow mf:job.coflow.macroflows) {
				for(Flow f:mf.flows)
					System.out.println(f.sender+" "+f.receiver +" "+f.size +" "+" "+f.finishTime+" "+(f.finishTime-f.startTime));
			}
		}
		
		System.out.printf("%.3f\n",mean(jct));		
		
	}
	
	public static void OutputCompletionTime2(Job[] jobs){
		ArrayList<Double> jct = new ArrayList<Double>(); // job completion time
		ArrayList<Double> cct = new ArrayList<Double>(); // coflow completion time
		ArrayList<Double> mct = new ArrayList<Double>(); // macroflow completion time
		ArrayList<Double> fct = new ArrayList<Double>(); // flow completion time
		double[] sum = new double[10]; // 将job的coflow大小划分为10个档次, 分别统计每个档次中的job completion time的和
		int[] cnt = new int[10]; // 将job的coflow大小划分为10个档次, 分别统计每个档次中的个数
		for(Job job:jobs){
			Coflow coflow = job.coflow;
			for(Macroflow mf:coflow.macroflows){
				for(Flow flow:mf.flows)
					fct.add(flow.finishTime - flow.startTime);
				mct.add(mf.finishTime - mf.startTime);
			}
			cct.add(coflow.finishTime - coflow.startTime);
			jct.add(job.reduceStageFinishTime - job.arriveTime);
			int k = (int)Math.log10(job.coflow.size/8*1024+1e-6);
			++cnt[k];
			sum[k]+= job.reduceStageFinishTime - job.arriveTime;
		}
//		System.out.printf("AJCT: %.3f\n",mean(jct));
		System.out.printf("mean of job completion time: %.3f\n",mean(jct));
		//System.out.printf("ACCT: %.3f\n",mean(cct));
		//System.out.printf("AMCT: %.3f\n",mean(mct));
		//System.out.printf("AFCT: %.3f\n",mean(fct));
		
		int[] p = new int[]{90,95,99,100};
		for(int i=0;i<p.length;++i){
//			System.out.printf("JCT%d: %.3f\n",p[i],pct(jct,p[i]/100.0));
			System.out.printf("%dth percentile %.3f\n",p[i],pct(jct,p[i]/100.0));
			//System.out.printf("CCT%d: %.3f\n",p[i],pct(cct,p[i]/100.0));
			//System.out.printf("MCT%d: %.3f\n",p[i],pct(mct,p[i]/100.0));
			//System.out.printf("FCT%d: %.3f\n",p[i],pct(fct,p[i]/100.0));
		}
		for(int i=0;i<cnt.length;++i){
			System.out.printf("%.3f ",sum[i]/cnt[i]);
		}
		System.out.println();
		
		
		
		
	}
	
	private static double mean(ArrayList<Double> a){
		double m = 0;
		for(Double d:a)
			m+=d;
		return m/a.size();
	}
	
	private static double pct(ArrayList<Double> a, double p){
		assert(0<=p && p<=1 && !a.isEmpty());
		Collections.sort(a);
		return a.get((int)((a.size()-1)*p));
	}
	
	
	// Group 2. Throughput
	// class Throughput record host throughput and core throughput at time tm
	public static class Throughput{
		public double time;
		public double duration;
		public double hostThroughput;
		public double coreThroughput;
		public Throughput(double tm, double dur, double htp, double ctp){ // wcx: htp是host throughput  ctp是core throughput
			time = tm; duration = dur; hostThroughput = htp; coreThroughput = ctp;
		}
	}
	
	public static ArrayList<Throughput> newThroughput(){ // note that the 0-th item has special meaning
		ArrayList<Throughput> tp = new ArrayList<Throughput>();
		double[] bw = Topology.getLinkBw();
		double hostBw = 0;
		// zy: why is single direction's bandwidth?
		for(int i=0;i<Settings.nHosts;++i)
			hostBw += bw[i];
		if(Settings.isGaintSwitch){
			tp.add(new Throughput(-1,-1,hostBw,-1));
		}else{
			double coreBw = 0;
			for(int i=0;i<Settings.nRacks;++i)
				coreBw+=bw[Settings.nHosts+i];
			tp.add(new Throughput(-1,-1,hostBw,coreBw));
		}
		return tp;
	}
	
	public static void measureThroughput(ArrayList<Throughput> tp, double[] bw, double time, double duration){
		assert(!tp.isEmpty());
		double hostBw = 0;
		for(int i=0;i<Settings.nHosts;++i)
			hostBw += bw[i];
		if(Settings.isGaintSwitch){
			tp.add(new Throughput(time,duration,1-hostBw/tp.get(0).hostThroughput,-1));
		}else{
			double coreBw = 0;
			for(int i=0;i<Settings.nRacks;++i)
				coreBw+=bw[Settings.nHosts+i];
			tp.add(new Throughput(time,duration,1-hostBw/tp.get(0).hostThroughput,1-coreBw/tp.get(0).coreThroughput));
		}
	}

	public static void measureTaskThroughput(double time, ArrayList<Flow>[] activeFlows, PrintWriter file) {
		double[] taskTp = new double[Scheduler.activeReducers.size()];
		for (ArrayList<Flow> flows: activeFlows) {
			for (Flow flow: flows) {
				int index = Scheduler.activeReducers.indexOf(flow._macroflow._reducer);
				taskTp[index] += flow.allocatedBw;
			}
		}
		for (ReduceTask rt: Scheduler.activeReducers) {
			file.write(time + ", " + rt.host + ", " + rt._job.jobId + ", " + rt.reducerId + ", " + taskTp[Scheduler.activeReducers.indexOf(rt)]  + "\n");
		}
	}
	
	public static void tic(){
		tm = System.currentTimeMillis();
	}
	
	public static void toc(){
		assert(tm>=0);
		System.out.println("Time: "+(System.currentTimeMillis()-tm)/1000 + " seconds.");
	}
}
