"""Fresh SDK readers for independently serialized parallel tasks."""

import os
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from importlib import import_module
from multiprocessing import get_context

import pyarrow as pa
from dal_obscura_plugin_api import ExecutionContext, ScanTask, TableHandle


def parallel_rows(factory, handle, tasks, context):
    def read(task):
        plugin = factory(TableHandle.from_json(handle.to_json()), context)
        try:
            schema, batches = plugin.execute(ScanTask.from_json(task.to_json()), context)
            try:
                return pa.Table.from_batches(list(batches), schema=schema).to_pylist()
            finally:
                batches.close()
        finally:
            plugin.close()

    with ThreadPoolExecutor(max_workers=4) as pool:
        return [r for group in pool.map(read, tasks) for r in group]


def _worker_rows(factory_module, factory_name, handle_json, task_json, worker_key):
    if "AWS_ACCESS_KEY_ID" in os.environ:
        os.environ["AWS_ACCESS_KEY_ID"] = worker_key
    factory = getattr(import_module(factory_module), factory_name)
    context = ExecutionContext(datetime.now(timezone.utc) + timedelta(minutes=2), "process-worker")
    plugin = factory(TableHandle.from_json(handle_json), context)
    try:
        schema, batches = plugin.execute(ScanTask.from_json(task_json), context)
        try:
            rows = pa.Table.from_batches(batches, schema=schema).to_pylist()
        finally:
            batches.close()
    finally:
        plugin.close()
    return os.getpid(), rows


def distributed_rows(factory, handle, tasks):
    """Each passive task crosses a process boundary into a fresh worker."""
    with ProcessPoolExecutor(
        max_workers=len(tasks), mp_context=get_context("spawn"), max_tasks_per_child=1
    ) as pool:
        futures = [
            pool.submit(
                _worker_rows,
                factory.__module__,
                factory.__name__,
                handle.to_json(),
                task.to_json(),
                "dal-worker-" + str(index),
            )
            for index, task in enumerate(tasks)
        ]
        results = [future.result(timeout=90) for future in futures]
    assert len({pid for pid, _ in results}) == len(tasks)
    return [row for _, rows in results for row in rows]
