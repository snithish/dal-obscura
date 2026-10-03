"""Fresh SDK readers for independently serialized parallel tasks."""

from concurrent.futures import ThreadPoolExecutor

import pyarrow as pa
from dal_obscura_plugin_api import ScanTask, TableHandle


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
