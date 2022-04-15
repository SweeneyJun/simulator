package sosp.algorithm;


import java.util.ArrayList;
import java.util.Arrays;

import sosp.jobs.Job;
import sosp.jobs.Task;
import sosp.main.Priority;
import sosp.main.Scheduler;
import sosp.main.SeparateScheduler;
import sosp.main.Settings;
import sosp.network.Flow;

public class DoubleFIFOEDGE implements  Algorithm {

    double weight = 1;
    boolean hybrid = false;

    public DoubleFIFOEDGE(String mode){
        if(mode.equalsIgnoreCase("coflow"))
            weight=1;
        else if (mode.equalsIgnoreCase("macroflow"))
            weight=0;
        else if (mode.equalsIgnoreCase("hybrid"))
            hybrid = true;
        else
            assert(false);
    }

    @Override
    public void releaseHost(HostAndTask ht) {
        // do nothing
    }

    @Override
    public ArrayList<Flow>[] getPriority() {
        if(Settings.nPriorities>0)
            return Priority.HybridPriority(hybrid?(SeparateScheduler.countFreestHost_const()>0?1:0):weight);
        else
            return hybrid? (SeparateScheduler.countFreestHost_const()>0?Priority.infinitePrioritiesCoflow():Priority.infinitePrioritiesMf())
                    : (weight>0?Priority.infinitePrioritiesCoflow():Priority.infinitePrioritiesMf());
    }

    /*  TODO 以上是从Algorithm.Default.java中复制的变量和函数, 主要是Reduce阶段使用
        直接使用可能有偏差, 后续看看是否需要修改
        以下部分allocateHostAndTask函数是wcx实现
     */


    @Override
    public HostAndTask allocateHostAndTask() {
        assert(Settings.isSeparate); // 只有存算分离场景下可以调用这个调度算法
        // check FreeBw
        // 4.11: check HostBw的同时别忘了Check SwitchBw!
        SeparateScheduler.totalFreeBw = 0;
        for(int i = 0; i < SeparateScheduler.freeBw.length; ++i){
            if(SeparateScheduler.freeBw[i] < 1e-6){
                SeparateScheduler.freeBw[i] = 0;
                continue;
            }
            SeparateScheduler.totalFreeBw += SeparateScheduler.freeBw[i];
        }
        if(SeparateScheduler.totalFreeBw < 1e-6 || SeparateScheduler.switchFreeBw < 1e-6){
            if(SeparateScheduler.totalFreeBw < 1e-6){
                SeparateScheduler.totalFreeBw = 0;
            }
            if(SeparateScheduler.switchFreeBw < 1e-6){
                SeparateScheduler.switchFreeBw = 0;
            }
            return null;
        }

        // check host
        int num = 0;
        for(int i = 0; i < Settings.nHosts; ++i){
            num += SeparateScheduler.freeSlots[i];
        }
        if(num == 0){
            return null;
        }

        // First choose a Job  在存算分离场景下, fairSelector中的nActiveTask概念已被修改, hasPendingTasks_const和hasPendingMappers_const似乎没必要修改
        // 因为如果修改了的话其实就视为把Input阶段和Map阶段严格区分了, 实际上不应该严格区分
        // 卡在Input阶段的Mapper自然在pendingMapperList里面?
        // TODO 存算分离场景下 smallestJobSelector是否需要修改还未查看
        Job chosenJob = Settings.fairJobScheduler ? Algorithm.fairJobSelector() : Algorithm.smallestJobSelector();
        if(chosenJob == null) {
            return null;
        }

        // 在上述语境下, 选出来的Job可能处于一种情况: 所有MapTask都已经执行过或者正在执行Input过程, 但仍有MapTask还处于Input阶段而导致仍有pendingMapper
        // 此时此Job被选择, 但已经不能提供还未Input的MapTask了, 若此时Job仍处于MapStage, 则应该直接返回null跳出
        // 这个check保护后续部署Input阶段时找不到相应MapTask的危险
        if(chosenJob.mapStageFinishTime < 0 && chosenJob.notInputMapperList.size() == 0) {
            return null;
        }


        // Second choose a task and allocate
        Task chosenTask = null;

        // 在原版DoubleFIFO中, ReduceTask和MapTask部署的机器使用了不同的准则
        // MapTask倾向于选择有更多带宽的Host, Reduce倾向于选择有更多freeSlot的Host, 而这种准则其实不太合理, 实验中发现会导致如下corner case:
        // ReduceTask部署的机器上有其他Job的Input阶段的MapTask, 而MapTask独占带宽导致ReducerTask占用slot却无法工作
        // 所以在这版DoubleFIFOEDGE中, 将ReduceTask和MapTask选择机器的准则改为相同的:
        // 即有空闲slot的情况下选择具有更多带宽的Host
        int chosenComputeHost = -1;
        for (int i = 0; i < Settings.nHosts; ++i) {
            chosenComputeHost = (chosenComputeHost < 0) ? i : chosenComputeHost;
            if (SeparateScheduler.freeSlots[i] == 0){
                continue; // 4.11: 虽然MapInput阶段以带宽为优先目标, 但要警惕有带宽无Slot的情况!
            }
            if (SeparateScheduler.freeBw[chosenComputeHost] < SeparateScheduler.freeBw[i]) {
                chosenComputeHost = i;
            }

        }

        if(chosenJob.mapStageFinishTime < 0) { // 分配MapTask
            assert(chosenJob.notInputMapperList.size() > 0);
            assert (chosenComputeHost >= 0); // 前面已经检查过是否有freeSlot以及freeBw, 如果逻辑正确这里不应该找不到计算节点 .. 4.11: 这里确实会找到计算结点, 但是在极端情况下这个计算结点可能无效!
            // 极端情况是指, 有freeSlot的host没freeBw, 有freeBw的host无freeSlot, 这种host能躲避前面的check, 最终从在找不到合适host的循环中返回一个初始化的值, 即host=0, 然后在这里导入错误, 所以这里还要检查
            if(SeparateScheduler.freeBw[chosenComputeHost] < 1e-4 || SeparateScheduler.switchFreeBw < 1e-4){
                SeparateScheduler.freeBw[chosenComputeHost] = 0;
                return null; // 二次防卫!
            }
            else if (SeparateScheduler.freeSlots[chosenComputeHost] == 0){
                return null;
            }
            chosenTask = chosenJob.notInputMapperList.get(0);
        }
        else{ // 分配ReduceTask
            assert (chosenJob.notInputMapperList.size() == 0 && chosenJob.pendingMapperList.size() == 0); // 防卫
            assert (chosenJob.hasPendingTasks_const());
            assert (chosenComputeHost >= 0);
            if(SeparateScheduler.freeBw[chosenComputeHost] < 1e-4 || SeparateScheduler.switchFreeBw < 1e-4){
                SeparateScheduler.freeBw[chosenComputeHost] = 0;
                return null; // 二次防卫!
            }
            else if (SeparateScheduler.freeSlots[chosenComputeHost] == 0){
                return null;
            }
            chosenTask = chosenJob.pendingReducerList.get(0);
        }
        return new HostAndTask(chosenComputeHost, chosenTask);


//         TODO SeparateScheduler.totalFreeBw = Arrays.stream(SeparateScheduler.freeBw).sum(); 这里更新带宽没用, 因为带宽分配是在这个函数外面
//         在Simulate函数中完成, 故应该在那儿更新

    }


}