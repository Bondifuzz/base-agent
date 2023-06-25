import multiprocessing as mp
from threading import Thread
from time import sleep
from typing import List
import pytest
import shutil
import os

from base_agent.app.ps.manager import ProcessManager
from base_agent.app.ps.manager.errors import ElevationRequired, ProcessLaunchError
from base_agent.app.ps.monitors.base import Monitor, MonitorThread
from base_agent.app.ps.monitors.errors import (
    DiskLimitExceeded,
    RamLimitExceeded,
    TimeLimitExceeded,
    UsageError,
)

from .conftest import eat_disk, eat_ram, eat_time
from base_agent.app.ps.monitors.monitors import DiskMonitor, RamMonitor, TimeMonitor


def test_ps_manager_run_builtin_utility():

    """
    Description
        Run process manager one time

    Succeeds
        If no errors occurred
    """

    manager = ProcessManager()
    manager.run_process(["sleep", "1"])


def test_ps_manager_run_process_twice():

    """
    Description
        Run process manager many times

    Succeeds
        If no errors occurred
    """

    manager = ProcessManager()
    manager.run_process(["sleep", "1"])
    manager.run_process(["sleep", "1"])
    manager.run_process(["sleep", "1"])


def test_ps_manager_stdin():

    """
    Description
        Pass stdin to process

    Succeeds
        If stdin was passed successfully
    """

    text = "12345"
    outfile = "out.txt"

    manager = ProcessManager()
    manager.run_process(["echo", "-n", text], stdout_file=outfile)

    assert os.path.isfile(outfile)
    with open(outfile, "rb") as f:
        content = f.read()

    assert text == content.decode()


def test_ps_manager_stdout_stderr():

    """
    Description
        Save stdout and stderr of process to files

    Succeeds
        If output of process was saved successfully
    """

    outfile = "out.txt"
    errfile = "err.txt"

    manager = ProcessManager()
    cmd = ["ls", "/etc", "no-such-file"]
    manager.run_process(cmd, stdout_file=outfile, stderr_file=errfile)

    assert os.path.isfile(outfile)
    assert os.path.isfile(errfile)
    assert os.path.getsize(outfile) > 0
    assert os.path.getsize(errfile) > 0


def test_ps_manager_run_custom_binary():

    """
    Description
        Run binary using absolute path

    Succeeds
        If process started successfully
    """

    abspath = shutil.which("sleep")
    assert abspath is not None

    manager = ProcessManager()
    manager.run_process([abspath, "1"])


def test_ps_manager_run_non_exec_binary():

    """
    Description
        Pass non-executable binary to process manager

    Succeeds
        If process manager added exec permission
        and process started successfully
    """

    abspath = shutil.which("sleep")
    assert abspath is not None

    shutil.copy(abspath, ".")
    os.chmod("./sleep", 0o600)
    assert os.system("./sleep 1") != 0

    manager = ProcessManager()
    exit_code = manager.run_process(["./sleep", "1"])
    assert exit_code == 0


def test_ps_manager_binary_not_found():

    """
    Description
        Pass non-existent binary to process manager

    Succeeds
        If exception raised
    """

    manager = ProcessManager()
    with pytest.raises(ProcessLaunchError):
        manager.run_process(["no-such-command", "1"])


@pytest.mark.skipif(os.getuid() != 0, reason="Root permissions required")
def test_ps_manager_drop_permissions():

    """
    Description
        Start process manager with dropped permissions

    Succeeds
        If permissions were dropped successfully
    """

    folder = "test"
    filename = f"{folder}/some-file"

    os.mkdir(folder)
    os.chmod(folder, 0o700)

    with open(filename, "wb") as f:
        f.write(b"123")

    os.chmod(filename, 0o600)

    manager = ProcessManager()
    exit_code = manager.run_process(["ls", folder], drop_perms=True)
    assert exit_code != 0

    exit_code = manager.run_process(["cat", filename], drop_perms=True)
    assert exit_code != 0

    exit_code = manager.run_process(["touch", filename], drop_perms=True)
    assert exit_code != 0


def test_ps_manager_drop_permissions_non_root():

    """
    Description
        Start process manager with dropped permissions
        from non-root user. It must fail, because only
        root user is able to call setuid, setgid

    Succeeds
        If exception was raised
    """

    manager = ProcessManager()
    with pytest.raises(ElevationRequired):
        manager.run_process(["ls"], drop_perms=True)


def test_ps_manager_shutdown():

    """
    Description
        Start process manager and
        force it to shutdown during run

    Succeeds
        If no errors occurred and
        process manager was stopped correctly
    """

    manager = ProcessManager()
    assert not manager.is_running()

    def thread_func():
        exit_code = manager.run_process(["sleep", "5"])
        assert exit_code != 0

    th = Thread(target=thread_func)
    th.start()

    sleep(0.5)
    assert manager.is_running()
    assert th.is_alive()
    manager.shutdown()
    th.join()

    assert not manager.is_running()


def test_ps_manager_monitor_usage_errors():

    """
    Description
        Use monitors with invalid values

    Succeeds
        If usage errors are raised
    """

    with pytest.raises(UsageError):
        MonitorThread(0, 1, 1)

    with pytest.raises(UsageError):
        MonitorThread(1, 0, 1)

    with pytest.raises(UsageError):
        MonitorThread(1, 1, 0)

    t = TimeMonitor(time_limit=10, boundary_value=0.1, boundary_interval=0.1)

    with pytest.raises(UsageError):
        t._get_sleep_interval(-1)

    with pytest.raises(UsageError):
        t._get_sleep_interval(11)


def test_ps_manager_one_monitor():

    """
    Description
        Start monitors one-by-one and consume resources.
        But not exceed them. Wait, until process finishes

    Succeeds
        If no errors occurred
    """

    manager = ProcessManager()
    mon = TimeMonitor(10, 1, 1)
    manager.run_process(["sleep", "1"], monitors=[mon])

    mon = RamMonitor(100 * 10 ** 6, 10 ** 6, 1)
    manager.run_process(["sleep", "1"], monitors=[mon])

    mon = DiskMonitor(10 ** 6, 10 ** 5, 1)
    manager.run_process(["sleep", "1"], monitors=[mon])


def test_ps_manager_many_monitors():

    """
    Description
        Start many monitors and consume resources.
        But not exceed them. Wait, until process finishes

    Succeeds
        If no errors occurred and
        all monitors cancelled monitoring
    """

    mons: List[Monitor] = [
        TimeMonitor(10, 1, 1),
        RamMonitor(100 * 10 ** 6, 10 ** 6, 1),
        DiskMonitor(10 ** 6, 10 ** 5, 1),
    ]

    manager = ProcessManager()
    manager.run_process(["sleep", "1"], monitors=mons)

    for mon in mons:
        mon.join()
        mon.verify()


def test_ps_manager_many_monitors_one_triggered():

    """
    Description
        Start many monitors and exceed time resources

    Succeeds
        If time monitor triggered and
        other monitors cancelled monitoring
    """

    mons: List[Monitor] = [
        TimeMonitor(1, 0.1, 0.1),
        RamMonitor(100 * 10 ** 6, 10 ** 6, 1),
        DiskMonitor(10 ** 6, 10 ** 5, 1),
    ]

    manager = ProcessManager()
    with pytest.raises(TimeLimitExceeded):
        manager.run_process(["sleep", "2"], monitors=mons)

    with pytest.raises(TimeLimitExceeded):
        for mon in mons:
            mon.join()
            mon.verify()


def test_ps_monitor_formula():
    mon = TimeMonitor(1600, 1, 1599 / 16000)
    assert mon.get_formula() == "y = -(x-800.0)^2/16000.0+40.0"


def test_time_monitor_trigger():

    """
    Description
        Start monitor and exceed time resources

    Succeeds
        If time monitor triggered
    """

    mon = TimeMonitor(1, 0.1, 0.1)
    process = mp.Process(target=eat_time, args=(2,))
    process.start()

    sleep(0.1)
    mon.start(process.pid)
    sleep(2)

    process.terminate()
    process.join()

    with pytest.raises(TimeLimitExceeded):
        mon.verify()


def test_ram_monitor_trigger():

    """
    Description
        Start monitor and exceed ram resources

    Succeeds
        If ram monitor triggered
    """

    mon = RamMonitor(80 * 10 ** 6, 10 ** 6, 0.1)
    process = mp.Process(target=eat_ram, args=(80 * 10 ** 6, 2, 10 ** 6))
    process.start()

    sleep(0.1)
    mon.start(process.pid)
    sleep(2)

    process.terminate()
    process.join()

    with pytest.raises(RamLimitExceeded):
        mon.verify()


def test_disk_monitor_trigger():

    """
    Description
        Start monitor and exceed disk resources

    Succeeds
        If disk monitor triggered
    """

    mon = DiskMonitor(10000, 1000, 0.1)
    process = mp.Process(target=eat_disk, args=(20000, 2, 1000))
    process.start()

    sleep(0.1)
    mon.start(process.pid)
    sleep(2)

    process.terminate()
    process.join()

    with pytest.raises(DiskLimitExceeded):
        mon.verify()


def test_ps_manager_simultaneous_trigger():

    """
    Description
        Start many monitors which can trigger simultaneously

    Succeeds
        If one of monitors triggered.
        Other monitors did't affect each other
    """

    mons = [
        TimeMonitor(1, 0.1, 0.1),
        TimeMonitor(1, 0.1, 0.1),
        TimeMonitor(1, 0.1, 0.1),
        TimeMonitor(1, 0.1, 0.1),
        TimeMonitor(1, 0.1, 0.1),
        TimeMonitor(1, 0.1, 0.1),
    ]

    manager = ProcessManager()
    with pytest.raises(TimeLimitExceeded):
        manager.run_process(["sleep", "2"], monitors=mons)

def test_monitor_no_process():

    """
    Description
        Start monitor with non-existent pid

    Succeeds
        If monitor exited silently and didn't triggered
    """

    mons: List[Monitor] = [
        TimeMonitor(10, 1, 1),
        RamMonitor(100 * 10 ** 6, 10 ** 6, 1),
        DiskMonitor(10 ** 6, 10 ** 5, 1),
    ]

    for mon in mons:
        mon.start(77777)
        mon.join(5)
        mon.verify()



