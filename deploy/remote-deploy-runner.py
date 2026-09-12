#!/usr/bin/env python3
# ============================================================
# VortexMQ · 远端部署驱动器（在【本地 Windows/Linux】执行）
#
# 用途：通过 SSH/SFTP 把部署包上传到云服务器，并执行 deploy/remote-deploy.sh。
#       Windows 无 sshpass/plink，密码认证场景必须借助 paramiko 才能非交互执行。
#
# 依赖：python -m pip install paramiko
#
# 用法：
#   $env:YZ_SSH_PASSWORD = '实例密码'          # 或改用 --key <私钥路径>
#   python deploy/remote-deploy-runner.py --host <服务器公网IP> --user ubuntu `
#          --bundle vortexmq-deploy.tar.gz --remote-dir /opt/vortexmq
#
# 常用开关：
#   --key <pem>          使用密钥认证（与 YZ_SSH_PASSWORD 二选一）
#   --gateway-port 8080  透传 GATEWAY_PORT 给远端脚本
#   --pg-password xxx    透传 POSTGRES_PASSWORD
#   --with-seed          部署后灌入业务种子数据 + RAG 向量化
#   --skip-upload        跳过上传与解包（包已在服务器上时使用）
#   --preflight-only     只做连通性与环境探测，不部署
#
# 密码不经命令行参数传递（避免落入 shell 历史），仅从环境变量 YZ_SSH_PASSWORD 读取。
# ============================================================
import argparse
import os
import posixpath
import shlex
import sys
import time

try:
    import paramiko
except ImportError:  # pragma: no cover
    sys.exit("缺少依赖 paramiko，请先执行：python -m pip install paramiko")

# Windows 控制台默认 GBK：不要强制改编码（否则 PowerShell 按 GBK 解码会乱码），
# 仅放宽错误处理，避免远端输出含 GBK 不可编码字符时抛 UnicodeEncodeError。
for _stream in (sys.stdout, sys.stderr):
    try:
        _stream.reconfigure(errors="replace")
    except Exception:
        pass


def parse_args():
    ap = argparse.ArgumentParser(description="VortexMQ · 远端部署驱动器")
    ap.add_argument("--host", required=True, help="服务器公网 IP / 域名")
    ap.add_argument("--port", type=int, default=22, help="SSH 端口，默认 22")
    ap.add_argument("--user", default="ubuntu", help="登录用户，默认 ubuntu")
    ap.add_argument("--key", default=None, help="私钥文件路径（与 YZ_SSH_PASSWORD 二选一）")
    ap.add_argument("--bundle", default="vortexmq-deploy.tar.gz", help="本地部署包路径")
    ap.add_argument("--remote-dir", default="/opt/vortexmq", help="远端部署目录")
    ap.add_argument("--gateway-port", default="", help="透传 GATEWAY_PORT")
    ap.add_argument("--pg-password", default="", help="透传 POSTGRES_PASSWORD")
    ap.add_argument("--deepseek-key", default="", help="透传 DEEPSEEK_API_KEY")
    ap.add_argument("--with-seed", action="store_true", help="部署后灌入种子数据 + RAG 向量化")
    ap.add_argument("--skip-upload", action="store_true", help="跳过上传/解包（远端已有代码）")
    ap.add_argument("--preflight-only", action="store_true", help="仅探测环境，不部署")
    ap.add_argument("--background", action="store_true",
                    help="后台执行部署（setsid+nohup），立即返回并写入 <远端目录>/deploy.log")
    ap.add_argument("--tail-log", type=int, default=0, metavar="N",
                    help="打印远端 deploy.log 末 N 行（轮询用，不做其他动作）")
    ap.add_argument("--set-env", action="append", default=[], metavar="K=V",
                    help="透传任意环境变量给远端部署脚本，可重复使用")
    ap.add_argument("--skip-deploy", action="store_true",
                    help="仅上传并解包，不执行部署脚本")
    return ap.parse_args()


class Remote:
    """封装 sudo 语义与实时回显的 SSH 会话。"""

    def __init__(self, args, password):
        self.args = args
        self.password = password
        self.is_root = args.user == "root"
        self.cli = paramiko.SSHClient()
        self.cli.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self.cli.connect(
            args.host, port=args.port, username=args.user,
            password=password, key_filename=args.key,
            timeout=20, banner_timeout=30, auth_timeout=30,
            allow_agent=False, look_for_keys=False,
        )

    def _wrap(self, cmd, sudo):
        if sudo and not self.is_root:
            return "sudo -S -p '' bash -lc " + shlex.quote(cmd)
        return cmd

    def run(self, cmd, sudo=False):
        """执行并实时回显，返回退出码。"""
        print(f"\n\033[36m$ {cmd}\033[0m")
        stdin, stdout, stderr = self.cli.exec_command(self._wrap(cmd, sudo), get_pty=False)
        if sudo and not self.is_root and self.password:
            stdin.write(self.password + "\n")
        stdin.flush()
        ch = stdout.channel
        while True:
            moved = False
            if ch.recv_ready():
                sys.stdout.write(ch.recv(65536).decode("utf-8", "replace"))
                sys.stdout.flush()
                moved = True
            if ch.recv_stderr_ready():
                sys.stderr.write(ch.recv_stderr(65536).decode("utf-8", "replace"))
                sys.stderr.flush()
                moved = True
            if moved:
                continue
            if ch.exit_status_ready():
                break
            time.sleep(0.1)
        while ch.recv_ready():
            sys.stdout.write(ch.recv(65536).decode("utf-8", "replace"))
        while ch.recv_stderr_ready():
            sys.stderr.write(ch.recv_stderr(65536).decode("utf-8", "replace"))
        code = ch.recv_exit_status()
        print(f"\033[90m[exit {code}]\033[0m")
        return code

    def capture(self, cmd, sudo=False):
        stdin, stdout, stderr = self.cli.exec_command(self._wrap(cmd, sudo))
        if sudo and not self.is_root and self.password:
            stdin.write(self.password + "\n")
        stdin.flush()
        out = stdout.read().decode("utf-8", "replace")
        err = stderr.read().decode("utf-8", "replace")
        return out, err, stdout.channel.recv_exit_status()

    def upload(self, local, remote):
        size = os.path.getsize(local)
        print(f"\n\033[36m上传 {local} -> {remote} ({size / 1024:.0f} KiB)\033[0m")
        sftp = self.cli.open_sftp()
        last = {"t": 0.0}

        def cb(done, total):
            now = time.time()
            if now - last["t"] >= 1.0 or done >= total:
                last["t"] = now
                sys.stdout.write(f"\r  进度 {done * 100 // max(total, 1)}% ({done // 1024} KiB)")
                sys.stdout.flush()

        sftp.put(local, remote, callback=cb)
        sys.stdout.write("\n")
        sftp.close()


def main():
    args = parse_args()
    password = os.environ.get("YZ_SSH_PASSWORD") or None
    if not args.key and not password:
        sys.exit("需提供 --key <私钥> 或 环境变量 YZ_SSH_PASSWORD")
    if args.tail_log == 0 and not args.skip_upload and not os.path.exists(args.bundle):
        sys.exit(f"本地部署包不存在：{args.bundle}（先用 git archive 或 tar 打包）")

    print(f"\033[1m==> 连接 {args.user}@{args.host}:{args.port}\033[0m")
    try:
        r = Remote(args, password)
    except paramiko.AuthenticationException:
        sys.exit(f"\033[31m[FAIL] SSH 认证失败：{args.user}@{args.host}"
                 "（用户名/密码/密钥不正确，或密钥尚未绑定到实例）\033[0m")
    except Exception as exc:
        sys.exit(f"\033[31m[FAIL] SSH 连接失败：{exc}\033[0m")
    print("\033[32m[ OK ] SSH 认证成功\033[0m")

    if args.tail_log:
        log_path = posixpath.join(args.remote_dir, "deploy.log")
        out, err, _ = r.capture(f"tail -n {args.tail_log} {log_path} 2>/dev/null || true", sudo=True)
        print(out if out.strip() else "(日志尚未产生)")
        return 0

    print("\n\033[1m[预检] 服务器环境\033[0m")
    probes = (
        ("系统", "lsb_release -ds 2>/dev/null || head -1 /etc/os-release"),
        ("内核/架构", "uname -srm"),
        ("CPU/内存", "nproc; free -h | awk 'NR==2{print $2\", available \"$7}'"),
        ("磁盘", "df -h /opt 2>/dev/null | tail -1 || df -h / | tail -1"),
        ("docker", "docker --version 2>/dev/null || echo 未安装"),
        ("compose", "docker compose version 2>/dev/null || echo 不可用"),
        ("sudo", "sudo -n true 2>/dev/null && echo 免密 || echo 需密码"),
        ("端口", "ss -lnt 2>/dev/null | awk 'NR>1{print $4}' | grep -E ':(80|5433|6379)$' || echo 80/5433/6379空闲"),
    )
    for label, cmd in probes:
        out, err, _ = r.capture(cmd)
        text = (out.strip() or err.strip()) or "(无输出)"
        print(f"  {label:<10}: {text.replace(chr(10), ' | ')}")

    if args.preflight_only:
        print("\n\033[33m[--preflight-only] 探测完成，未执行部署\033[0m")
        return 0

    if not args.skip_upload:
        remote_pkg = "/tmp/" + os.path.basename(args.bundle)
        r.upload(args.bundle, remote_pkg)
        if r.run(f"mkdir -p {shlex.quote(args.remote_dir)}", sudo=True) != 0:
            return 1
        if r.run(f"tar -xzf {remote_pkg} -C {shlex.quote(args.remote_dir)}", sudo=True) != 0:
            return 1
        print("\033[32m[ OK ] 部署包已上传并解包\033[0m")

    if args.skip_deploy:
        print("\033[33m[--skip-deploy] 仅完成上传与解包，未执行部署脚本\033[0m")
        return 0

    envs = []
    for kv in args.set_env:
        envs.append(kv)
    if args.gateway_port:
        envs.append(f"GATEWAY_PORT={shlex.quote(args.gateway_port)}")
    if args.pg_password:
        envs.append(f"POSTGRES_PASSWORD={shlex.quote(args.pg_password)}")
    if args.deepseek_key:
        envs.append(f"DEEPSEEK_API_KEY={shlex.quote(args.deepseek_key)}")
    if args.with_seed:
        envs.append("WITH_SEED=1")

    log_path = posixpath.join(args.remote_dir, "deploy.log")
    prefix = ("env " + " ".join(envs) + " ") if envs else ""

    if args.background:
        inner = (f"( cd {shlex.quote(args.remote_dir)} && {prefix}bash deploy/remote-deploy.sh ) "
                 f"> {log_path} 2>&1; echo \"EXIT=$?\" >> {log_path}")
        cmd = ("setsid nohup bash -c " + shlex.quote(inner)
               + " < /dev/null > /dev/null 2>&1 & echo STARTED")
        if r.run(cmd, sudo=True) != 0:
            return 1
        print("\033[32m[ OK ] 已在服务器后台启动部署（setsid+nohup，SSH 断开也会继续）\033[0m")
        print(f"      日志：{log_path}")
        print("      轮询：python deploy/remote-deploy-runner.py "
              f"--host {args.host} --user {args.user} --remote-dir {args.remote_dir} --tail-log 60")
        return 0

    deploy_cmd = (f"cd {shlex.quote(args.remote_dir)} && "
                  + " ".join(envs + ["bash deploy/remote-deploy.sh"]))
    print("\n\033[1m===== 开始远端部署（首次构建耗时较长）=====\033[0m")
    code = r.run(deploy_cmd, sudo=True)

    print()
    if code == 0:
        print(f"\033[32m\033[1m[部署成功] 控制台 http://{args.host}:8081/ · API http://{args.host}:8000/docs\033[0m")
    else:
        print(f"\033[31m\033[1m[部署失败] 退出码 {code}，请查看上方日志\033[0m")
    return code


if __name__ == "__main__":
    sys.exit(main())
