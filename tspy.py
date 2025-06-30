#!/usr/bin/env python3

import argparse
import sqlite3
import os
import subprocess
import time
import uuid
import signal
from datetime import datetime, timezone, timedelta
from threading import Thread, Lock

parent_dir = os.path.dirname(os.path.abspath(__file__))
logo_file = os.path.join(parent_dir, "logo")
if os.path.exists(logo_file):
    with open(logo_file, "r") as f:
        logo = f.read()
DB_FILE = os.path.expanduser("~/.tspy_queue.db")
JOB_OUT_DIR = os.path.expanduser("~/.tspy_out")
DEFAULT_JOBS = 1
JST = timezone(timedelta(hours=9))


def init_db():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS jobs (
            id TEXT PRIMARY KEY,
            command TEXT NOT NULL,
            status TEXT NOT NULL,
            created_at TEXT NOT NULL,
            started_at TEXT,
            finished_at TEXT,
            rc INTEGER,
            out_file TEXT,
            err_file TEXT,
            cwd TEXT,
            pid INTEGER,
            priority INTEGER DEFAULT 0,
            paused INTEGER DEFAULT 0,
            gpu TEXT
        )
    """)
    for stmt in [
        "ALTER TABLE jobs ADD COLUMN pid INTEGER",
        "ALTER TABLE jobs ADD COLUMN priority INTEGER DEFAULT 0",
        "ALTER TABLE jobs ADD COLUMN paused INTEGER DEFAULT 0",
        "ALTER TABLE jobs ADD COLUMN gpu TEXT"
    ]:
        try:
            c.execute(stmt)
        except sqlite3.OperationalError:
            pass
    conn.commit()
    conn.close()
    if not os.path.exists(JOB_OUT_DIR):
        os.makedirs(JOB_OUT_DIR)


def add_job(cmd, cwd=None, priority=0, gpu=None):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    job_id = str(uuid.uuid4())[:8]
    now = datetime.now(JST).isoformat()
    out_file = os.path.join(JOB_OUT_DIR, f"{job_id}.out")
    err_file = os.path.join(JOB_OUT_DIR, f"{job_id}.err")
    c.execute("""INSERT INTO jobs (id, command, status, created_at, started_at, finished_at, rc, out_file, err_file, cwd, pid, priority, paused, gpu)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
              (job_id, cmd, "queued", now, None, None, None, out_file, err_file, cwd, None, priority, 0, str(gpu) if gpu is not None else None))
    conn.commit()
    conn.close()
    gpu_str = f"GPU {gpu}" if gpu is not None else "CPU"
    print(f"Job {job_id} added (priority {priority}, {gpu_str}).")
    print(f"  Output log: {out_file}")
    print(f"  Error log:  {err_file}")


def list_jobs():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("""SELECT id, status, command, created_at, started_at, finished_at, rc, cwd, pid, priority, paused, gpu FROM jobs ORDER BY created_at""")
    rows = c.fetchall()
    print(f"{'ID':<8} {'STATUS':<10} {'PRI':<3} {'RC':<3} {'PAUSED':<6} {'PID':<6} {'GPU':<6} {'CREATED':<20} {'STARTED':<20} {'FINISHED':<20} {'CWD':<16} COMMAND")
    for row in rows:
        jid, status, cmd, created, started, finished, rc, cwd, pid, priority, paused, gpu = row
        gpu_disp = gpu if gpu is not None else "CPU"
        print(f"{jid:<8} {status:<10} {str(priority):<3} {str(rc) if rc is not None else '-':<3} "
              f"{('yes' if paused else 'no'):<6} {str(pid) if pid else '-':<6} {gpu_disp:<6} "
              f"{created[:19]:<20} {(started or '-')[:19]:<20} {(finished or '-')[:19]:<20} {str(cwd or '-')[:16]} {cmd}")
    conn.close()


def show_output(job_id, err=False):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("SELECT out_file, err_file FROM jobs WHERE id = ?", (job_id,))
    row = c.fetchone()
    conn.close()
    if not row:
        print(f"No such job: {job_id}")
        return
    fname = row[1] if err else row[0]
    if os.path.exists(fname):
        with open(fname) as f:
            print(f.read(), end='')
    else:
        print("(No output yet)")


def remove_job(job_id):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute(
        "SELECT status, pid, out_file, err_file FROM jobs WHERE id = ?", (job_id,))
    row = c.fetchone()
    if not row:
        print(f"No such job: {job_id}")
        conn.close()
        return
    status, pid, out_file, err_file = row
    if status == "running" and pid:
        try:
            os.kill(pid, signal.SIGTERM)
            print(f"Sent SIGTERM to job {job_id} (PID {pid})")
        except Exception as e:
            print(f"Failed to kill process {pid}: {e}")
    c.execute("DELETE FROM jobs WHERE id = ?", (job_id,))
    conn.commit()
    conn.close()
    for fname in (out_file, err_file):
        if fname and os.path.exists(fname):
            os.remove(fname)
    print(f"Job {job_id} removed.")


def remove_all_jobs(force=False):
    if not force:
        confirm = input(
            "Are you sure you want to remove ALL jobs and their logs? [y/N]: ")
        if confirm.lower() not in ["y", "yes"]:
            print("Aborted.")
            return
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("SELECT id, status, pid, out_file, err_file FROM jobs")
    rows = c.fetchall()
    for row in rows:
        job_id, status, pid, out_file, err_file = row
        if status == "running" and pid:
            try:
                os.kill(pid, signal.SIGTERM)
                print(f"Sent SIGTERM to job {job_id} (PID {pid})")
            except Exception as e:
                print(f"Failed to kill process {pid}: {e}")
        for fname in (out_file, err_file):
            if fname and os.path.exists(fname):
                os.remove(fname)
    c.execute("DELETE FROM jobs")
    conn.commit()
    conn.close()
    print(f"All jobs and their logs have been removed.")


def update_job_status(job_id, status=None, pid=None, paused=None):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    setters = []
    params = []
    if status is not None:
        setters.append("status = ?")
        params.append(status)
    if pid is not None:
        setters.append("pid = ?")
        params.append(pid)
    if paused is not None:
        setters.append("paused = ?")
        params.append(paused)
    params.append(job_id)
    if setters:
        c.execute(f"UPDATE jobs SET {', '.join(setters)} WHERE id = ?", params)
    conn.commit()
    conn.close()


def set_job_paused(job_id, paused):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("UPDATE jobs SET paused = ? WHERE id = ?",
              (1 if paused else 0, job_id))
    conn.commit()
    conn.close()


def get_job_pid(job_id):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("SELECT pid FROM jobs WHERE id = ?", (job_id,))
    row = c.fetchone()
    conn.close()
    return row[0] if row else None


def pause_job(job_id):
    pid = get_job_pid(job_id)
    if not pid:
        print(f"No running process found for job {job_id}")
        return
    try:
        os.kill(pid, signal.SIGSTOP)
        set_job_paused(job_id, True)
        print(f"Job {job_id} (PID {pid}) paused.")
    except Exception as e:
        print(f"Failed to pause job {job_id} (PID {pid}): {e}")


def resume_job(job_id):
    pid = get_job_pid(job_id)
    if not pid:
        print(f"No running process found for job {job_id}")
        return
    try:
        os.kill(pid, signal.SIGCONT)
        set_job_paused(job_id, False)
        print(f"Job {job_id} (PID {pid}) resumed.")
    except Exception as e:
        print(f"Failed to resume job {job_id} (PID {pid}): {e}")


def kill_job(job_id):
    pid = get_job_pid(job_id)
    if not pid:
        print(f"No running process found for job {job_id}")
        return
    try:
        os.kill(pid, signal.SIGTERM)
        print(f"Job {job_id} (PID {pid}) killed (SIGTERM sent).")
    except Exception as e:
        print(f"Failed to kill job {job_id} (PID {pid}): {e}")


def worker(jobs=DEFAULT_JOBS):
    lock = Lock()
    running = {}
    running_gpus = {}

    print(logo)
    print(f"Starting worker with {jobs} concurrent jobs...")

    def job_runner(job_row):
        job_id, command, out_file, err_file, cwd, gpu = job_row[:6]
        old_cwd = os.getcwd()
        rc = 1
        pid = None
        now = datetime.now(JST).isoformat()
        update_job_status(job_id, status="running")
        env = os.environ.copy()
        if gpu is not None:
            env["CUDA_VISIBLE_DEVICES"] = gpu
        try:
            if cwd:
                os.chdir(os.path.expanduser(cwd))
            with open(out_file, "w") as outf, open(err_file, "w") as errf:
                proc = subprocess.Popen(
                    command, shell=True, stdout=outf, stderr=errf, preexec_fn=os.setsid, env=env)
                pid = proc.pid
                update_job_status(job_id, pid=pid)
                running[job_id] = proc
                if gpu is not None:
                    running_gpus[gpu] = job_id
                rc = proc.wait()
        except Exception as e:
            with open(out_file, "w") as outf, open(err_file, "w") as errf:
                errf.write(f"Failed to run job: {e}\n")
        finally:
            os.chdir(old_cwd)
        status = "done" if rc == 0 else "failed"
        now = datetime.now(JST).isoformat()
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        c.execute("UPDATE jobs SET status = ?, finished_at = ?, rc = ?, pid = NULL WHERE id = ?",
                  (status, now, rc, job_id))
        conn.commit()
        conn.close()
        with lock:
            running.pop(job_id, None)
            if gpu is not None and running_gpus.get(gpu) == job_id:
                running_gpus.pop(gpu, None)

    while True:
        with lock:
            active_jobs = len(running)
            used_gpus = set(running_gpus.keys())
        if active_jobs < jobs:
            conn = sqlite3.connect(DB_FILE)
            c = conn.cursor()
            c.execute(
                "SELECT id, command, out_file, err_file, cwd, gpu FROM jobs "
                "WHERE status = 'queued' AND paused = 0 "
                "ORDER BY priority DESC, created_at")
            rows = c.fetchall()
            conn.close()
            launch_cnt = 0
            for row in rows:
                job_id, _, _, _, _, gpu = row
                if gpu is not None and gpu in used_gpus:
                    continue
                t = Thread(target=job_runner, args=(row,))
                t.daemon = True
                t.start()
                launch_cnt += 1
                if gpu is not None:
                    used_gpus.add(gpu)
                if active_jobs + launch_cnt >= jobs:
                    break
        time.sleep(2)


def main():
    parser = argparse.ArgumentParser(
        description="Simple task spooler in Python (tspy) with GPU/CPU pinning, parallelism, priority, pause/resume/kill."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    add_parser = subparsers.add_parser(
        'add', help='Add a command to the queue')
    add_parser.add_argument('cmd', type=str, help='The command to run')
    add_parser.add_argument('--cwd', type=str, default=None,
                            help='Working directory for the job')
    add_parser.add_argument('--priority', type=int,
                            default=0, help='Job priority (higher runs first)')
    add_parser.add_argument('--gpu', type=str, default=None,
                            help='GPU device id to use (e.g., 0, 1, ...). Omit to run on CPU.')

    status_parser = subparsers.add_parser(
        'status', help='List jobs and status')

    output_parser = subparsers.add_parser('output', help='Show job stdout')
    output_parser.add_argument('jobid', type=str, help='Job ID')

    error_parser = subparsers.add_parser('error', help='Show job stderr')
    error_parser.add_argument('jobid', type=str, help='Job ID')

    remove_parser = subparsers.add_parser(
        'remove', help='Remove a job (and its output)')
    group = remove_parser.add_mutually_exclusive_group(required=True)
    group.add_argument('jobid', type=str, nargs='?', help='Job ID')
    group.add_argument('--all', action='store_true',
                       help='Remove all jobs and logs')
    remove_parser.add_argument('-f', '--f', action='store_true',
                               help='Force remove without confirm (useful with --all)')

    worker_parser = subparsers.add_parser(
        'worker', help='Run the worker (process queued jobs)')
    worker_parser.add_argument(
        '-j', '--jobs', type=int, default=DEFAULT_JOBS, help='Number of concurrent jobs (default 1)')

    pause_parser = subparsers.add_parser('pause', help='Pause a running job')
    pause_parser.add_argument('jobid', type=str, help='Job ID')

    resume_parser = subparsers.add_parser('resume', help='Resume a paused job')
    resume_parser.add_argument('jobid', type=str, help='Job ID')

    kill_parser = subparsers.add_parser('kill', help='Kill a running job')
    kill_parser.add_argument('jobid', type=str, help='Job ID')

    args = parser.parse_args()
    init_db()

    if args.command == "add":
        add_job(args.cmd, args.cwd, args.priority, args.gpu)
    elif args.command == "status":
        list_jobs()
    elif args.command == "output":
        show_output(args.jobid, err=False)
    elif args.command == "error":
        show_output(args.jobid, err=True)
    elif args.command == "remove":
        if args.all:
            remove_all_jobs(force=args.f)
        elif args.jobid:
            remove_job(args.jobid)
        else:
            print("Specify a job ID or --all.")
    elif args.command == "worker":
        worker(jobs=args.jobs)
    elif args.command == "pause":
        pause_job(args.jobid)
    elif args.command == "resume":
        resume_job(args.jobid)
    elif args.command == "kill":
        kill_job(args.jobid)


if __name__ == "__main__":
    main()
