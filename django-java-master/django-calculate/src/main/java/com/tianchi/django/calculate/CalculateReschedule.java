package com.tianchi.django.calculate;

import java.util.*;

import com.google.common.collect.*;
import com.tianchi.django.common.utils.ScheduleUtils;
import com.tianchi.django.common.IReschedule;
import com.tianchi.django.common.enums.Resource;
import com.tianchi.django.common.pojo.*;
import com.tianchi.django.common.pojo.associate.GroupRuleAssociate;
import com.tianchi.django.common.utils.*;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.*;
import org.apache.commons.lang3.tuple.ImmutablePair;

import static com.tianchi.django.common.utils.HCollectors.countingInteger;
import static com.tianchi.django.common.utils.HCollectors.entriesToMap;
import static com.tianchi.django.common.utils.HCollectors.pairToMap;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

@Slf4j
@AllArgsConstructor
public class CalculateReschedule implements IReschedule {

    private long start;//程序启动时间戳

    @Override
    public List<RescheduleResult> reschedule(final List<NodeWithPod> nodeWithPods, final Rule rule) {

        List<NodeWithPod> nodeWithPods4CheckAgainst = nodeWithPods.parallelStream().map(NodeWithPod::copy).collect(toList());

        Map<String, List<Pod>> againstPods = searchAgainstPods(nodeWithPods4CheckAgainst, rule);

        return calculate(nodeWithPods4CheckAgainst, againstPods, rule);

    }

    private List<RescheduleResult> calculate(List<NodeWithPod> nodeWithPods, Map<String, List<Pod>> againstPods, Rule rule) {

        Set<String> sourceNodeSns = Sets.newHashSet(againstPods.keySet());

        List<GroupRuleAssociate> groupRuleAssociates = GroupRuleAssociate.fromPods(NodeWithPodUtils.toPods(nodeWithPods));

        Map<String, Integer> allMaxInstancePerNodeLimit = RuleUtils.toAllMaxInstancePerNodeLimit(rule, groupRuleAssociates);

        List<RescheduleResult> rescheduleResults = Lists.newArrayList();

        List<Pod> forsakePods = Lists.newArrayList();//装不下的容器

        for (Map.Entry<String, List<Pod>> entry : againstPods.entrySet()) {

            String sourceSn = entry.getKey();

            //以打散为目的对排序后的pod、node贪心循环
            for (Pod pod : entry.getValue()) {

                boolean forsake = true;//遗弃标识

                for (NodeWithPod nwp : nodeWithPods) {

                    if (ScheduleUtils.ruleOverrunTimeLimit(rule, start)) {//时间上限约束，超时跳出。
                        log.info("overrun time limit");
                        return rescheduleResults;
                    }

                    if (sourceSn.equals(nwp.getNode().getSn())) {
                        continue;
                    }

                    if (sourceNodeSns.contains(nwp.getNode().getSn())) {//有迁移动作的宿主机不在装容器。(明显不优)
                        continue;
                    }

                    if (ScheduleUtils.staticFillOnePod(nwp, pod, allMaxInstancePerNodeLimit)) {
                        forsake = false;
                        rescheduleResults.add(//动态迁移每一个stage是并行执行，每一次执行必须满足调度约束。
                            RescheduleResult.builder()
                                .stage(1).sourceSn(sourceSn).targetSn(nwp.getNode().getSn())
                                .podSn(pod.getPodSn()).cpuIDs(pod.getCpuIDs())
                                .build()
                        );
                        break;
                    }
                }

                if (forsake) {//所有的机器都不满足此容器分配
                    forsakePods.add(pod);
                }
            }
        }

        if (forsakePods.size() > 0) {
            log.error("forsake pod count: {}", forsakePods.size());
        }

        return rescheduleResults;

    }

    /**
     * 找出这个集群中违背规则的所有的容器列表(贪心)。
     */
    private Map<String, List<Pod>> searchAgainstPods(List<NodeWithPod> nodeWithPods, Rule rule) {

        Map<String, List<Pod>> result = Maps.newHashMap();

        //先过滤不满足资源分配的容器，nodeWithPods数据会被修改
        searchResourceAgainstPods(nodeWithPods)
            .forEach((k, v) -> result.compute(k, (key, old) -> old == null ? v : ListUtils.union(v, old)));

        //再过滤不满足布局的容器，nodeWithPods数据会被修改
        searchLayoutAgainstPods(nodeWithPods, rule)
            .forEach((k, v) -> result.compute(k, (key, old) -> old == null ? v : ListUtils.union(v, old)));

        //再过滤不满足cpu分配的容器，nodeWithPods数据会被修改
        searchCgroupAgainstPods(nodeWithPods)
            .forEach((k, v) -> result.compute(k, (key, old) -> old == null ? v : ListUtils.union(v, old)));

        return result;

    }

    //违背资源规则容器准备重新调度。<node_sn,List<Pod>>
    private Map<String, List<Pod>> searchResourceAgainstPods(List<NodeWithPod> nodeWithPods) {
        return nodeWithPods.parallelStream()
            .map(
                nwp -> {

                    List<Pod> againstPods = Lists.newArrayList(), tmpPods = Lists.newArrayList(), normalPods = Lists.newArrayList();

                    //校验资源不满足的容器
                    for (Pod pod : nwp.getPods()) {

                        boolean against = false;

                        for (Resource resource : Resource.class.getEnumConstants()) {

                            int nodeResource = nwp.getNode().value(resource);

                            int podsResource = PodUtils.totalResource(ListUtils.union(tmpPods, ImmutableList.of(pod)), resource);

                            if (nodeResource < podsResource) {
                                againstPods.add(pod);
                                against = true;
                                break;
                            }

                        }

                        if (!against) {
                            tmpPods.add(pod);
                        }

                    }

                    int eniAgainstPodSize = tmpPods.size() - nwp.getNode().getEni();

                    //校验超过eni约束的容器
                    if (eniAgainstPodSize > 0) {
                        againstPods.addAll(tmpPods.subList(0, eniAgainstPodSize));
                        normalPods.addAll(tmpPods.subList(eniAgainstPodSize, tmpPods.size() - 1));
                    } else {
                        normalPods.addAll(tmpPods);
                    }

                    nwp.setPods(normalPods);//贪心判断的正常容器继续放在该机器中

                    return ImmutablePair.of(nwp.getNode().getSn(), againstPods);

                }
            )
            .filter(pair -> CollectionUtils.isNotEmpty(pair.getRight())).collect(pairToMap());

    }

    //违背布局规则容器准备重新调度。<node_sn,List<Pod>>
    private Map<String, List<Pod>> searchLayoutAgainstPods(List<NodeWithPod> nodeWithPods, Rule rule) {

        List<GroupRuleAssociate> groupRuleAssociates = GroupRuleAssociate.fromPods(NodeWithPodUtils.toPods(nodeWithPods));

        Map<String, Integer> maxInstancePerNodes = RuleUtils.toAllMaxInstancePerNodeLimit(rule, groupRuleAssociates);

        return nodeWithPods.parallelStream()
            .map(
                nwp -> {

                    Map<String, Integer> groupCountPreNodeMap = Maps.newHashMap();

                    List<Pod> againstPods = Lists.newArrayList(), normalPods = Lists.newArrayList();

                    for (Pod pod : nwp.getPods()) {

                        int maxInstancePerNode = maxInstancePerNodes.get(pod.getGroup());

                        int oldValue = groupCountPreNodeMap.getOrDefault(pod.getGroup(), 0);

                        if (oldValue == maxInstancePerNode) {
                            againstPods.add(pod);
                            continue;
                        }

                        groupCountPreNodeMap.put(pod.getGroup(), oldValue + 1);

                        normalPods.add(pod);

                    }

                    nwp.setPods(normalPods);//贪心判断的正常容器继续放在该机器中

                    return ImmutablePair.of(nwp.getNode().getSn(), againstPods);

                }
            )
            .filter(pair -> CollectionUtils.isNotEmpty(pair.getRight())).collect(pairToMap());
    }

    //违背cpu绑核分配规则容器准备重新调度。<node_sn,List<Pod>>
    private Map<String, List<Pod>> searchCgroupAgainstPods(List<NodeWithPod> nodeWithPods) {
        return nodeWithPods.parallelStream()
            .map(
                nwp -> {

                    Node node = nwp.getNode();

                    List<Pod> againstPods = Lists.newArrayList();

                    //node中不存在topologies,不校验绑核。
                    if (CollectionUtils.isEmpty(node.getTopologies())) {
                        return ImmutablePair.<String, List<Pod>>nullPair();
                    }

                    //node中不存在topologies,不校验绑核。
                    if (CollectionUtils.isEmpty(nwp.getPods())) {
                        return ImmutablePair.<String, List<Pod>>nullPair();
                    }

                    //这台机器上重叠的cpuId分配
                    Set<Integer> againstCpuIds = NodeWithPodUtils.cpuIDCountMap(nwp).entrySet().stream()
                        .filter(entry -> entry.getValue() > 1).map(Map.Entry::getKey).collect(toSet());

                    Map<Integer, Integer> cpuToSocket = NodeUtils.cpuToSocket(node);

                    Map<Integer, Integer> cpuToCore = NodeUtils.cpuToCore(node);

                    List<Pod> normalPods = Lists.newArrayList();

                    for (Pod pod : nwp.getPods()) {

                        if (CollectionUtils.isEmpty(pod.getCpuIDs())) {//没有分配cpuId的容器
                            againstPods.add(pod);
                            continue;
                        }

                        //贪心选择包含重叠cpu的容器
                        Set<Integer> intersectionCpuIds = Sets.intersection(againstCpuIds, Sets.newHashSet(pod.getCpuIDs()));

                        if (CollectionUtils.isNotEmpty(intersectionCpuIds)) {
                            againstCpuIds.removeAll(pod.getCpuIDs());
                            againstPods.add(pod);
                            continue;
                        }

                        long socketCount = pod.getCpuIDs().stream().map(cpuToSocket::get).distinct().count();

                        if (socketCount > 1) {//跨socket容器
                            againstPods.add(pod);
                            continue;
                        }

                        Map<Integer, Integer> sameCoreMap = pod.getCpuIDs().stream().collect(countingInteger(cpuToCore::get))
                            .entrySet().stream().filter(entry -> entry.getValue() > 1).collect(entriesToMap());

                        if (MapUtils.isNotEmpty(sameCoreMap)) {
                            againstPods.add(pod);
                            continue;
                        }
                        //TODO 已经很复杂了，如果排名拉不开差距在增加sensitiveCpuBind数据校验

                        normalPods.add(pod);

                    }

                    nwp.setPods(normalPods);//贪心判断的正常容器继续放在该机器中

                    return ImmutablePair.of(nwp.getNode().getSn(), againstPods);

                }
            )
            .filter(pair -> !pair.equals(ImmutablePair.<String, List<Pod>>nullPair()))
            .filter(pair -> CollectionUtils.isNotEmpty(pair.getRight())).collect(pairToMap());
    }

}
