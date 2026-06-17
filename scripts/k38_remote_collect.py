import os, json, time

cpu = os.popen("top -bn1 2>/dev/null | grep 'Cpu(s)' | awk '{print $2+$4}'").read().strip()
if not cpu:
    cpu = os.popen("ps -eo pcpu | awk '{s+=$1} END {print s}'").read().strip() or '0'
mem = os.popen("free | grep Mem | awk '{print $3/$2*100}'").read().strip() or '0'
disk = os.popen("df / | tail -1 | awk '{print $2, $3, $5}' | tr -d '%'").read().strip()
disk_parts = disk.split()
disk_total = disk_parts[0] if len(disk_parts) >= 1 else '0'
disk_used = disk_parts[1] if len(disk_parts) >= 2 else '0'
disk_pct = disk_parts[2] if len(disk_parts) >= 3 else '0'
host = os.uname().nodename
# uptime: try /proc/uptime first for seconds
uptime_seconds = ''
try:
    with open('/proc/uptime') as f:
        up = f.read().strip().split()
        if up: uptime_seconds = round(float(up[0]))
except:
    pass
if not uptime_seconds:
    uptime_seconds = int(time.time())  # fallback
uptime_str = os.popen("uptime -p 2>/dev/null").read().strip()
if not uptime_str:
    uptime_str = os.popen("uptime | awk '{for(i=3;i<=NF;i++) printf $i\" \";}'").read().strip()
docker = os.popen("docker ps --format '{{.ID}} {{.Image}} {{.Names}} {{.Status}}' 2>/dev/null | head -20").read().strip()
gpu = os.popen("nvidia-smi --query-gpu=utilization.gpu,temperature.gpu,memory.used,memory.total --format=csv,noheader,nounits 2>/dev/null").read().strip()

print(json.dumps({
    'hostname': host,
    'cpu_pct': round(float(cpu), 1),
    'mem_pct': round(float(mem), 1),
    'disk_pct': round(float(disk_pct), 1),
    'disk_total': disk_total,
    'disk_used': disk_used,
    'uptime': uptime_seconds,
    'uptime_str': uptime_str or '',
    'docker': docker or '',
    'gpu_info': gpu or ''
}))
