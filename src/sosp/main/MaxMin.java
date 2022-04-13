package sosp.main;

import java.util.ArrayList;

import sosp.main.Settings;
import sosp.network.Flow;

public class MaxMin {
	// zy: what does a mean?
	static int a = 0;

	// 4.13: 发现了Separate场景下第二次getMaxMin会导致所有流的allocatedBw不为0的原因是, 下面某次对flow的遍历会先将flow的allocatedBw置为0
	// 而这个过程中并没有把占用的带宽还回去, 就会导致接下来flow无带宽可分配的情况, 因为不好直接修改这个复杂的getMaxMin, 也不方便在外层跳过这个过程
	// 所以我的想法是修改一下对flow的遍历过程, 在置0前先归还allocatedBw


	// all_flows: all flows divided by dscp;    link_bw: total bandwidth of a given link
	static public void getMaxMin(ArrayList<Flow>[] all_flows, double[] link_bw){
		int dscp = all_flows.length;
//		System.out.println(dscp);
		while(dscp-- > 0){
			++a;
			ArrayList<Flow> flows = all_flows[dscp]; // flows with current dscp
			if (flows.size() == 0) {
				continue;
			}
			ArrayList<Boolean> link_checked = new ArrayList<Boolean>();
			// full or not, of a given link
			for(int i=0; i<link_bw.length; ++i)
				link_checked.add(link_bw[i]<Settings.epsilon);
			
			ArrayList<ArrayList<Flow>> link_flows = new ArrayList<ArrayList<Flow>>(); // flow list of a given link
			for(int i=0; i<link_bw.length; ++i)
				link_flows.add(new ArrayList<Flow>());
			for(Flow flow : flows){

				// 4.13 添加的置0前归还带宽的过程
				for (int node : flow.route) {
					SeparateScheduler.freeBw[node] += flow.allocatedBw;
					SeparateScheduler.totalFreeBw += flow.allocatedBw;
				}


				flow.allocatedBw = 0; // allocated bandwidth = 0
				boolean ok = true;
				for(int i=0; i<flow.route.length;++i)
					ok = ok && !link_checked.get(flow.route[i]);
				if(!ok)
					continue;
				for(int i=0; i<flow.route.length;++i)
					link_flows.get(flow.route[i]).add(flow);
			}
			
			while(true){
				// find the bottleneck link
				int bot_link = -1;
				for(int i=0; i<link_bw.length; ++i){
					if(link_checked.get(i)) // have been full
						continue;
					if(bot_link<0){ // the 1st available
						bot_link = i;
						continue;
					}
					// zy: how does we calculate the bottleneck? bandwidth / flow size?
					double bot = link_bw[bot_link] / link_flows.get(bot_link).size();
					double cur = link_bw[i] / link_flows.get(i).size();
					bot_link = cur<bot ? i : bot_link;
				}
				if(bot_link<0) // no available link
					break;
				// update flow and link
				double bot_bw = link_bw[bot_link] / link_flows.get(bot_link).size();
				ArrayList<Flow> bot_flows = link_flows.get(bot_link);
				for(Flow flow : bot_flows){
					flow.allocatedBw += bot_bw; // add flow bw
					for(int i=0; i<flow.route.length; ++i){ // remove link available bw
						link_bw[flow.route[i]] -= bot_bw;
						assert(link_bw[flow.route[i]] > -Settings.epsilon);
						if(bot_link!=flow.route[i])
							assert(link_flows.get(flow.route[i]).remove(flow));
					}
				}
				bot_flows.clear();
				link_checked.set(bot_link, true);
			}
		}
	}
}
