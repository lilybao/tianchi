package com.tianchi.django.common.utils;

import com.google.common.graph.*;

/**
 * Created by 烛坤 on 2020/5/22.
 *
 * @author 烛坤
 * @date 2020/05/22
 */
public class GraphUtils {

    public static <T> MutableGraph<T> to(int expectedNodeCount) {
        return GraphBuilder.directed() //指定为有向图
            .nodeOrder(ElementOrder.<T>insertion()) //节点按插入顺序输出
            //(还可以取值无序unordered()、节点类型的自然顺序natural())
            .expectedNodeCount(expectedNodeCount) //预期节点数
            .allowsSelfLoops(true) //允许自环
            .build();
    }

}
