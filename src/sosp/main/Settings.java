package sosp.main;

import java.io.FileInputStream;
import java.util.Properties;
import java.util.Random;

import sosp.algorithm.*;

public class Settings {
	
	/* Note: this class only contains properties related to setting files. 
	 * Runtime states are defined in Scheduler.java
	 */
	
	public static enum SchedulingMode{  //流量模式?coflow,macroflow或者混合模式
		coflow, macroflow, hybrid
	}
	
	public final static boolean fairJobScheduler = true; // TODO note!
	
	// topologies
	public static boolean isGaintSwitch = true;  //gaint switch是什么?
	public static int nHosts = -1; 		// host number
	public static int nRacks = -1; 		// rack number, not used in giant switch
	public static double fanIn = 0; 	// not used in giant switch
	public static int nSlots = 4; 		// slot number
	public static int nPriorities = 1; 	// 0 means infinite priorities, >0 means n priorities

	// topologies in condition that separation between storage and computation
	public static boolean isSeparate = false;
	public static int storageFanIn = 0;
	public static double storageSpeed = 0;
	public static double switchBottleFreeBw = 0;

	
	// traffics
	public static double sizeScale = 1; // size unit: Gb
	public static double timeScale = 1; // time unit: s
	public static double reducerCompTimePerMB = 0; // unit: s
	public static double mapperSizeMB = 100; // each mapper reads 100 MB from DFS
	public static double mapperCompTimePerMB = 1; // each mapper need 1s to process 1MB input data
	public static int nReplications = 3; // each file has $nReplications$ replications in DFS
	public static double congestionThreshold = 0.9; // shuffle watcher, in [0,1] ???
	public static double sizeEstimationError = 1.0; // in (0,1]. 1 means no error; the more it is close to 0, the larger error it has
	//public static boolean mergeTasks = true; // whether tasks are merged such that each coflow has limited mappers and reducers (limit to #hosts)
	
	// simulators
	public static Random r = null;  // seed
	public static double minTimeStep = 0.001; // seconds
	public static double epsilon = 0.000000001; // float precision
	//public static SchedulingMode schedulingMode = SchedulingMode.coflow;
	public static int algorithm = -1;
	public static Algorithm algo = null; // task placement and network scheduling algorithms

	public static double speed = 0;  // 计算集群中的链路带宽 (实现中默认上传下载带宽相同, 皆为此值, 在Topology.getLinkBw()函数中被使用)

	public static int reduceNum = 3;
	public static double parallelism = 0; //?

	public static void loadFromFile(String file, String[]
			args){
		try{
			Properties p = new Properties();
			FileInputStream fis = new FileInputStream(file);
			assert(fis!=null);
			p.load(fis);
			
//			for(int i=1;i<args.length;i+=2)
//				p.setProperty(args[i], args[i+1]);
			
			// read topologies
			isGaintSwitch = readBoolean(p, "isGaintSwitch");
			nHosts = readInteger(p, "nHosts");
			if(!isGaintSwitch){
				nRacks = readInteger(p, "nRacks");
				fanIn = readDouble(p, "fanIn");
			}
			nSlots = readInteger(p, "nSlots");
			nPriorities = readInteger(p, "nPriorities");

			// read topologies in condition that separation between storage and computation
			isSeparate = readBoolean(p, "isSeparate");

			storageFanIn = readInteger(p, "storageFanIn");

			storageSpeed = readDouble(p, "storageSpeed");
			switchBottleFreeBw = readDouble(p, "switchBottleFreeBw");
			
			// read traffics
			sizeScale = readDouble(p, "sizeScale");
			timeScale = readDouble(p, "timeScale");
			reducerCompTimePerMB = readDouble(p, "reducerCompTimePerMB");
			mapperSizeMB = readDouble(p, "mapperSizeMB");
			mapperCompTimePerMB = readDouble(p, "mapperCompTimePerMB");
			nReplications = readInteger(p, "nReplications");
			congestionThreshold = readDouble(p, "congestionThreshold");
			sizeEstimationError = readDouble(p, "sizeEstimationError");
//			assert(0<sizeEstimationError && sizeEstimationError<=1);
			//mergeTasks = readBoolean(p,"mergeTasks");
			
			// read utilities
			r = new Random(readInteger(p, "seed"));
			minTimeStep = readDouble(p, "minTimeStep");
			epsilon = readDouble(p, "epsilon");
			speed = readDouble(p, "speed");
			reduceNum = readInteger(p, "reduceNum");
			//schedulingMode = SchedulingMode.valueOf(readString(p,"schedulingMode").toLowerCase()); // valueOf may throw an exception

			parallelism = readDouble(p, "parallelism");
			
			//System.out.println(args[0]);
			if(args.length%2==1){   //???
				switch(Integer.parseInt(args[0])){
					case 0: algo = new MultiUserMf("coflow"); break;
					case 1: algo = new MultiUserMf("macroflow"); break;
					case 2: algo = new MultiUserMf("hybrid"); break;
					case 3: algo = new Default("macroflow"); break;
					case 4: algo = new Default("coflow"); break;
					case 5: algo = new Default("hybrid"); break;
					case 7: algo = new Neat("fair"); break;
					case 6: algo = new Neat("pmpt"); break;
					case 8: algo = new MultiUserMf("none"); break;
					case 9: algo = new NeatGlobal("fair"); break;
					case 10: algo = new NeatGlobal("pmpt"); break;
					case 11: algo = new Max3Reducer(); break;
				}
			}else{
				algorithm = readInteger(p, "algorithm");
				switch(algorithm){
					case 0: algo = new MultiUserMf("coflow"); break;
					case 1: algo = new MultiUserMf("macroflow"); break;
					case 2: algo = new MultiUserMf("hybrid"); break;
					case 3: algo = new Default("macroflow"); break;
					case 4: algo = new Default("coflow"); break;
					case 5: algo = new Default("hybrid"); break;
					case 7: algo = new Neat("fair"); break;
					case 6: algo = new Neat("pmpt"); break;
					case 8: algo = new MultiUserMf("none"); break;
					case 9: algo = new NeatGlobal("fair"); break;
					case 10: algo = new NeatGlobal("pmpt"); break;
					case 11: algo = new Max3Reducer(); break;
					case 12: algo = new Hadoop(); break;
					case 13: algo = new ShuffleWatcher(); break;
					case 14: algo = new Sig19Neat(); break;
					case 15: algo = new DoubleFIFO("hybrid"); break;
					case 16: algo = new DoubleFIFOEDGE("hybrid"); break;
				}
			}
			assert(algorithm >= 0);
//			System.out.println("algorithm: " + algorithm);
			fis.close();
		}catch(Exception e){
			e.printStackTrace();
		}
	}
	
	private static boolean readBoolean(Properties p, String key){
		String v = p.getProperty(key); assert(v!=null); return Boolean.parseBoolean(v);
	}
	
	private static int readInteger(Properties p, String key){
		String v = p.getProperty(key); assert(v!=null); return Integer.parseInt(v);
	}
	
	private static double readDouble(Properties p, String key){
		String v = p.getProperty(key); assert(v!=null); return Double.parseDouble(v);
	}
	
	private static String readString(Properties p, String key){
		String v = p.getProperty(key); assert(v!=null); return v;
	}
}
