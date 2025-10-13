# 测试要点
1) 基础与信息
- pluginInfo 字段正确性（名称、版本、特性字串）。
- connect 参数个数/类型/可选位缺省、poolSize、readReplica 分支；错误 host/port 异常。
- release/releaseAll 的重复调用与句柄失效保护；getHandle/getHandleStatus token 行为。

2) KV 功能
- set/get 正确性，TTL 生效与过期后为 NULL；
- 对错误类型 key（如对 hash 执行 get）抛出 WRONGTYPE 异常；

3) Hash 批量
- 非法输入：非字符串列、行数不等、类型不符。
- batchSet：标量-标量快路径、向量-向量等长校验、batchWin/numThreads 可调、多线程一致性；
- batchHashSet：表必须为 BASIC TABLE 且全 STRING 列；ids 与表行数一致；正确性校验（HGETALL）。
- batchGet：存在/不存在混合返回，NULL flag 校验；
- batchDel：UNLINK/DEL 两路径、batchWin/numThreads 可调。

4) List 批量
- batchPush：keys 与嵌套 STRING 向量等长；rightPush/leftPush 顺序正确；长度与序列验证；
- 非法输入：values 非嵌套 STRING、内层空向量、长度不一致、参数类型不符。

5) run 通用命令
- routeKey 解析：tag=xxx / tag:xxx / key=xxx / 直给哈希标签与含 {...} 的 key；
- 非法输入：routeKey 非 STRING、cmd 非 STRING 向量或为空。

6) 并发与稳定性
- 单作业并发线程安全
- 跨作业并发线程安全

