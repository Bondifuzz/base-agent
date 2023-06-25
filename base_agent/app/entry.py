import asyncio
from base64 import b64decode
import functools
from typing import Awaitable, Callable, List
from logging import getLogger
import threading
import json
import os
import sys

from .kubernetes.manager import UserContainerManager
from kubernetes_asyncio.client import V1ContainerState

from .transfer import FileTransfer

from .utils import TimeMeasure, random_string, rfc3339

from signal import (
    Signals,
    getsignal,
    signal,
    SIGINT,
    SIGTERM,
    SIGHUP,
)

from .message_queue.instance import MQAppState, mq_init

from .storage.s3.initializer import BucketCheck
from .storage.s3 import ObjectStorage

from .output import AgentOutput, CrashBase, Metrics, Status
from .settings import AppSettings, load_app_settings
from .abstract import Agent

from .errors import (
    FuzzerAbortedError,
    InternalError,
    AgentError,
)


def object_storage_init(settings: AppSettings):

    bucket_fuzzers = settings.object_storage.buckets.fuzzers
    bucket_data = settings.object_storage.buckets.data

    checks = [
        BucketCheck(bucket_fuzzers, check_read=True, check_write=False),
        BucketCheck(bucket_data, check_read=True, check_write=True),
    ]

    return ObjectStorage(settings, checks)


def signal_handler(signums: List[Signals]):
    def _decorator(func: Callable[[], Awaitable]):
        def get_handler(task: asyncio.Task):
            _lock = threading.Lock()
            _catched = False

            def _signal_handler(signum, _):
                nonlocal _catched, _lock
                with _lock:
                    if _catched:
                        return

                    _logger = getLogger("signal_handler")
                    _logger.warning("Caught signal: %s", Signals(signum).name)
                    _logger.warning("Terminating fuzzing session")

                    _catched = True
                    task.cancel()
                    
            return _signal_handler
        
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            _handlers = []
            
            loop = asyncio.get_event_loop()
            task = loop.create_task(func(*args, **kwargs))
            new_handler = get_handler(task)
            try:
                for signum in signums:
                    _handlers.append(getsignal(signum))
                    signal(signum, new_handler)
                
                return await task
            finally:
                for signum, old_handler in zip(signums, _handlers):
                    signal(signum, old_handler)

        return wrapper
    return _decorator


async def _dump_ok(mq_state: MQAppState, settings: AppSettings, output: AgentOutput, measure: TimeMeasure):
    await mq_state.producers.sch_run_result.produce(
        user_id=settings.fuzzer.user_id,
        project_id=settings.fuzzer.project_id,
        pool_id=settings.fuzzer.pool_id,
        fuzzer_id=settings.fuzzer.id,
        fuzzer_rev=settings.fuzzer.rev,
        fuzzer_engine=settings.fuzzer.engine,
        fuzzer_lang=settings.fuzzer.lang,

        session_id=settings.fuzzer.session_id,
        agent_mode=settings.agent.mode,

        start_time=rfc3339(measure.start_time),
        finish_time=rfc3339(measure.finish_time),
        agent_result=output.dict(),
    )

async def _dump_err(mq_state: MQAppState, settings: AppSettings, e: AgentError, measure: TimeMeasure):
    try:
        with open(settings.paths.metrics, "r") as f:
            metrics = Metrics(**json.loads(f.read()))
    except:
        metrics = Metrics(tmpfs=0, memory=0)

    status = Status(code=e.code, message=e.message, details=e.details)
    await mq_state.producers.sch_run_result.produce(
        user_id=settings.fuzzer.user_id,
        project_id=settings.fuzzer.project_id,
        pool_id=settings.fuzzer.pool_id,
        fuzzer_id=settings.fuzzer.id,
        fuzzer_rev=settings.fuzzer.rev,
        fuzzer_engine=settings.fuzzer.engine,
        fuzzer_lang=settings.fuzzer.lang,

        session_id=settings.fuzzer.session_id,
        agent_mode=settings.agent.mode,

        start_time=rfc3339(measure.start_time),
        finish_time=rfc3339(measure.finish_time),
        agent_result=AgentOutput(
            status=status,
            metrics=metrics,
            crashes_found=0,
        ).dict(),
    )

async def _send_crash(
    mq_state: MQAppState,
    settings: AppSettings,
    transfer: FileTransfer,
    run_id: str,
    created: str,
    crash: CrashBase,
):
    if len(crash.input) > settings.fuzzer.crash_max_size:
        input_id = run_id + random_string(10)
        input_bytes = b64decode(crash.input)
        transfer.upload_crash(input_id, input_bytes)
        crash.input_id = input_id
        crash.input = None
                
    getLogger("entry").info("Send crash:\n%s", json.dumps(crash.dict(), indent=4))
    await mq_state.producers.cra_new_crash.produce(
        user_id=settings.fuzzer.user_id,
        project_id=settings.fuzzer.project_id,
        pool_id=settings.fuzzer.pool_id,
        fuzzer_id=settings.fuzzer.id,
        fuzzer_rev=settings.fuzzer.rev,
        fuzzer_engine=settings.fuzzer.engine,
        fuzzer_lang=settings.fuzzer.lang,
        crash=crash.dict(),
        created=created,
    )


@signal_handler([SIGINT, SIGTERM, SIGHUP])
async def agent_entry_inner(agent: Agent):
    logger = getLogger("entry")
    # TODO: rewrite entry

    try:
        logger.info("Reading settings")
        settings = load_app_settings()
        
        logger.info("Creating ObjectStorage and MessageQueue objects")
        mq_app = await mq_init(settings)
        mq_state: MQAppState = mq_app.state
        await mq_app.start()
        
    except asyncio.CancelledError:
        return 0

    except:
        logger.exception("Initialization failed")
        return 1


    try:
        object_storage = object_storage_init(settings) # TODO: async s3
        container_mgr = await UserContainerManager.create(settings)

        logger.info("Checking fuzzer container...")
        for _ in range(5):
            cont_status = await container_mgr.read_fuzzer_container()
            cont_state: V1ContainerState = cont_status.state
            if cont_state.running is not None:
                break
                
            await asyncio.sleep(1)
        else:
            logger.error("Failed - fuzzer container not started")
            logger.error(cont_status)
            raise InternalError()
        logger.info("Checking fuzzer container... Ok")

        # TODO: do we need to check metrics at start?

        run_id = random_string(40) # TODO: discuss
        logger.info("Running mode: %s, run_id: %s", settings.agent.mode, run_id)
        agent_mode = agent.select_mode(
            settings=settings,
            container_mgr=container_mgr,
            object_storage=object_storage,
            run_id=run_id,
        )

        os.chdir(settings.paths.volume_disk)

        measure = TimeMeasure() # TODO: delete?
        logger.info("Running fuzzer")
        try:
            with measure.measuring():
                await agent_mode.run()

        except Exception as e:
            if isinstance(e, asyncio.CancelledError):
                e = FuzzerAbortedError()

            elif not isinstance(e, AgentError):
                logger.exception("Unhandled exception", exc_info=e)
                e = InternalError()

            elif isinstance(e, InternalError):
                logger.exception("Internal exception", exc_info=e)
                
            agent_mode.status = Status(
                code=e.code,
                message=e.message,
                details=e.details,
            )

        logger.info(f"Running complete with code: {agent_mode.status.code}")

        logger.info(f"Parsing metrics...")
        with open(settings.paths.metrics, "r") as f:
            metrics = Metrics(**json.loads(f.read()))
        logger.info(f"Parsing metrics... Ok")

        logger.info(f"Uploading fuzzer files...")
        await agent_mode.finish()
        logger.info(f"Uploading fuzzer files... Ok")

        logger.info(f"Sending founded crashes, count: {len(agent_mode.crashes)}...")
        created = rfc3339(measure.finish_time)
        for crash in agent_mode.crashes:
            await _send_crash(
                mq_state=mq_state,
                settings=settings,
                transfer=agent_mode.transfer,
                run_id=run_id,
                created=created,
                crash=crash,
            )
        logger.info(f"All crashes sended")

        logger.info(f"Sending fuzzer statistics")
        res = AgentOutput(
            status=agent_mode.status,
            crashes_found=len(agent_mode.crashes),
            statistics=agent_mode.fuzz_statistics,
            metrics=metrics,
        )
           
        await _dump_ok(mq_state, settings, res, measure)
        logger.info("Run succeeded\n%s", json.dumps(res.dict(), indent=4))

    except AgentError as e:
        logger.exception("Agent crashed with error!")
        await _dump_err(mq_state, settings, e, measure)
        return 2

    except asyncio.CancelledError:
        pass

    except Exception as e:
        logger.exception("Agent crashed with unhandled error!")
        e = InternalError()
        await _dump_err(mq_state, settings, e, measure)
        return 3

    finally:
        await mq_app.shutdown()
    
    return 0


def agent_entry(agent: Agent):
    exit_code = asyncio.run(agent_entry_inner(agent))
    sys.exit(exit_code)
