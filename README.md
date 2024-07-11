### 人力考勤数据接入和生成宽表
- 概述：接入计算机中心给的api接口，然后生成人力考勤宽表hr_bigtable_dayrecord给开发二后台使用。
- 入口代码：
1）update50AndBaseAppImpl（更新api接口的表，部分表是按照指定天数更新多少天，部分是全量更新）
2）bigtable50AppImpl（根据api接口和金蝶的表进行join生成宽表）
3）other2hiveAppImpl（后期新接入的api接口数据，部分表是按照指定天数更新多少天，部分是全量更新）
- 需求逻辑对接人：开发二室 王燕（260344）

