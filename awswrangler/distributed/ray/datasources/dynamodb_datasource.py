"""Ray DynamoDBDatasource Module."""
from typing import TYPE_CHECKING, Any, Dict, List

import boto3
from boto3.dynamodb.table import BatchWriter
from ray.data.block import Block, BlockAccessor, BlockMetadata
from ray.data.datasource.datasource import Datasource, WriteResult
from ray.types import ObjectRef

from awswrangler.distributed.ray import ray_remote
from awswrangler.dynamodb._utils import _validate_items

if TYPE_CHECKING:
    from mypy_boto3_dynamodb.service_resource import Table


class DynamoDBDatasource(Datasource[Any]):  # pylint: disable=abstract-method
    """Datasource for writing data blocks to a DynamoDB table."""

    def _write_block(
        self,
        table: "Table",
        writer: BatchWriter,
        block: BlockAccessor[Dict[str, Any]],
        **writer_args: Any,
    ) -> None:
        frame = block.to_pandas()
        items: List[Dict[str, Any]] = [v.dropna().to_dict() for _, v in frame.iterrows()]

        _validate_items(items=items, dynamodb_table=table)

        for item in items:
            writer.put_item(Item=item)

    def do_write(  # type: ignore[override]
        self,
        blocks: List[ObjectRef[Block[Dict[str, Any]]]],
        metadata: List[BlockMetadata],
        table_name: str,
        ray_remote_args: Dict[str, Any],
        **write_args: Any,
    ) -> List[ObjectRef[WriteResult]]:
        """Create and return write tasks for the datasource."""
        _write_block_to_dynamodb = self._write_block

        def write_block(block: Block[Dict[str, Any]]) -> None:
            dynamodb_resource = boto3.resource("dynamodb")
            table = dynamodb_resource.Table(table_name)

            with table.batch_writer() as writer:
                _write_block_to_dynamodb(
                    table,
                    writer,
                    BlockAccessor.for_block(block),
                    **write_args,
                )

        write_block_fn = ray_remote(**ray_remote_args)(write_block)

        write_tasks = []
        for block in blocks:
            write_task = write_block_fn(block)
            write_tasks.append(write_task)

        return write_tasks
