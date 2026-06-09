# dltrace HTTP 节点配置

## 优先级（高→低）

1. **环境变量** `DLTRACE_NODES` — JSON 字符串 `{"节点名":"http://ip:port/api/v1/metrics",...}`
2. **配置文件** `~/.dltrace_nodes.json` — 同上 JSON 格式
3. **硬编码默认** — 文件内的 `_load_node_config(hardcoded=...)`

## 使用场景

### 加临时节点（环境变量）
```bash
DLTRACE_NODES='{"新节点":"http://192.168.1.100:8899/api/v1/metrics"}' nohup python3 dltrace.py web &
```

### 改默认列表（配置文件）
```bash
# 写到 ~/.dltrace_nodes.json
echo '{"三万八":"http://192.168.3.29:8899/api/v1/metrics","大傻":"http://192.168.3.55:8899/api/v1/metrics"}' > ~/.dltrace_nodes.json
```

### 恢复默认
```bash
rm -f ~/.dltrace_nodes.json
unset DLTRACE_NODES
# 重启 web
```
