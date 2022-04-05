package sosp.main;

import sosp.jobs.Job;

public class TrafficAnalyzer {

	public static void main(String[] args) {
		final int step = 50;
		int[] bins = new int[100];
		double[] sz = new double[100]; // sz: size
		int[] bin = new int[7];
		Settings.loadFromFile("config.ini",args);
		Job[] jobs = Traffic.loadFromFile("FB2010-1Hr-150-0.txt");
		for(Job job:jobs){
			++bins[(int)job.arriveTime/step];
			sz[(int)job.arriveTime/step] += job.coflow.size/8*1024;
			bin[(int)Math.log10(job.coflow.size/8*1024+0.0001)]++;
		}
//		for(int i=0;i<sz.length;++i) {
//			System.out.println(sz[i]);
//		}
	}

}
