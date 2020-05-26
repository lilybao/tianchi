### 工程结构
1. django-calculate : 参赛者实现静态布局和动态迁移功能模块
2. django-common : 基本的功能模块
3. django-data : 数据模块
4. django-start : 程序运行模块

参赛者只能修改django-calculate模块代码。其他模块后续会独立出来。

IDEA开发需安装lombok插件，lombok用于精简pojo结构中get、set方法。

### 程序运行
1. com.tianchi.django.CalculateLauncher demo程序启动。
2. com.tianchi.django.ScoreLauncher 数据结果评测启动。(预计5.20开发完)

### 版本

#### 0.0.4
1. 修正静态布局在未放入足够原始数据情况下，评分会降低的问题。
2. 修正动态迁移逻辑demo实践。围绕可能被忽视的三个功能点去实现，且并未以减少分数为目的(可参考静态布局逻辑)。
	* 资源超卖迁移、规则调整迁移、cgroup绑核迁移。
3. 修正第二份数据中reschedule.source数据中容器cpu数据与实际cpu分配数据不一致问题，请重新拉去数据。
4. 实现动态迁移评测逻辑。

#### 0.0.3
1. 增加第二份数据(data_2)用于功能开发和测试，静态布局增加宿主机socket、core、cpu数据，动态迁移增加容器cpu分配数据。
2. 添加通过传参方式指定多目录数据并行计算功能，以便后续数据切换和排名。
3. 静态布局demo中增加根据宿主机topologies结构，为pod分配cpu逻辑。
4. 静态布局增加打分测评逻辑，运行ScoreLauncher，提取在功能类中便于参赛者使用或参考。
5. 开放go-demo权限，go工程进度晚于java工程，大致为java 0.0.1 版本功能，本周将会对齐java-demo，耐心等待。
6. 增加更多注解，便于理解题意。

#### 0.0.2
1. Rule数据结构删除groupMaxInstancePerNodes调度约束，降低赛题难度。
2. Rule数据结构增加scoreWeight字段，避免硬编码。
3. rule.source文件按新数据结构，将赛题中的权重数据填充到数据中。

#### 0.0.1 
实现静态布局最基本功能。