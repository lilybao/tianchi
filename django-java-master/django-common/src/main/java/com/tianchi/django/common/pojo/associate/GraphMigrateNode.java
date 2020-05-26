package com.tianchi.django.common.pojo.associate;

import java.util.List;

import com.google.common.collect.Lists;
import lombok.*;
import org.apache.commons.lang3.StringUtils;

/**
 * Created by 烛坤 on 2020/5/22.
 *
 * @author 烛坤
 * @date 2020/05/22
 */
@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class GraphMigrateNode {

    public static final GraphMigrateNode ROOT = GraphMigrateNode.builder().podSn("").nodeSn("").build();

    private String podSn;

    private String nodeSn;

    private List<Integer> cpuIds;

    private OperationType type;

    public static GraphMigrateNode toExpansion(String podSn, String nodeSn, List<Integer> cpuIds) {
        return GraphMigrateNode.builder().podSn(podSn).nodeSn(nodeSn).cpuIds(cpuIds).type(OperationType.EXPANSION).build();
    }

    public static GraphMigrateNode toOffline(String podSn, String nodeSn) {
        return GraphMigrateNode.builder().podSn(podSn).nodeSn(nodeSn).cpuIds(Lists.newArrayList()).type(OperationType.OFFLINE).build();
    }

    public static boolean isRoot(GraphMigrateNode node) {
        return StringUtils.isAllEmpty(node.getPodSn(), node.getNodeSn());
    }

}
