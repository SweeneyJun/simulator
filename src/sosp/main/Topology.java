package sosp.main;

public class Topology {
	
	private static boolean gaint_switch = false;
	
	public static void loadFromFile(){
		assert(false); // not available
	}
	
	public static void loadGaint(){ // normalized bw: 1
		gaint_switch = true;
		assert(Settings.nHosts>0);
	}
	
	public static void loadTwoLayer(){ // end host bw: 1
		gaint_switch = false;
		assert(Settings.nHosts>0 && Settings.nRacks>0 && Settings.fanIn>0); 
		assert(Settings.nHosts%Settings.nRacks == 0);
	}

	// getPath return the link sequence number [], [UP, DOWN], [UP, UP_RACK, DOWN_RACK, DOWN]
	public static int[] getPath(int s, int t){
		int[] path = null;
		assert(s<Settings.nHosts && t<Settings.nHosts);
		if(gaint_switch){
			final int UP = 0;
				final int DOWN = Settings.nHosts;
			if(s==t)
				path = new int[]{};
			else
				path = new int[]{UP+s, DOWN+t};
		}else{ // 2-layer
			final int UP = 0;
			final int DOWN = Settings.nHosts + Settings.nRacks;
			final int HOST = 0;
			final int RACK = Settings.nHosts;
			int hosts_per_rack = Settings.nHosts / Settings.nRacks;
			if(s==t)
				path = new int[]{};
			else if(s/hosts_per_rack == t/hosts_per_rack) // in the same rack
				path = new int[]{UP+HOST+s, DOWN+HOST+t};
			else
				path = new int[]{UP+HOST+s, UP+RACK+s/hosts_per_rack, DOWN+RACK+t/hosts_per_rack, DOWN+HOST+t};
		}
		return path;
	}

	// getLinkBw initialize the link bandwidth
	public static double[] getLinkBw(){
		double[] link = null;
		if(gaint_switch){
			final int UP = 0;
			final int DOWN = Settings.nHosts;
			link = new double[Settings.nHosts*2];
			for(int i=0;i<Settings.nHosts;++i)
				link[UP+i] = link[DOWN+i] = Settings.speed;
		}else{
			final int UP = 0;
			final int DOWN = Settings.nHosts + Settings.nRacks;
			final int HOST = 0;
			final int RACK = Settings.nHosts;
			double core_bw = (Settings.nHosts/Settings.fanIn) / Settings.nRacks;
			// don't know the speed is right
			link = new double[Settings.nHosts*2 + Settings.nRacks*2];
			for(int i=0;i<Settings.nHosts;++i)
				link[UP+HOST+i] = link[DOWN+HOST+i] = Settings.speed;
			for(int i=0;i<Settings.nRacks;++i)
				link[UP+RACK+i] = link[DOWN+RACK+i] = core_bw*Settings.speed;
		}
		return link;
	}

	// getHostThroughput return the none free bandwidth ratio
	public static double getHostThroughput(double[] freeBw) {
		//assert(gaint_switch);
		double[] totalBw = getLinkBw();
		double total = 0, free = 0;
		for(int i=0;i<Settings.nHosts;++i) {
			total += totalBw[i];
			free += freeBw[i];
		}
		return 1-free/total;
	}
}
