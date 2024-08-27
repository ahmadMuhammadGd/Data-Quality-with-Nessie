from minio import Minio
from minio.commonconfig import CopySource
from airflow.hooks.base import BaseHook

class MinIOHook(BaseHook):
    def __init__(self, endpoint: str, access_key: str, secret_key: str, secure: bool = False):
        self.endpoint = endpoint
        self.access_key = access_key
        self.secret_key = secret_key
        self.secure = secure
        self.client = self.get_minio_client()

    def get_minio_client(self):
        return Minio(
            self.endpoint,
            access_key=self.access_key,
            secret_key=self.secret_key,
            secure=self.secure,
        )
    
    def move_object(self, source_bucket: str, target_bucket: str, prefix: str):
        """
        Moves all objects from the source bucket to the target bucket.

        Args:
            source_bucket (str): The name of the source bucket.
            target_bucket (str): The name of the target bucket.
            prefix (str): The prefix to filter objects in the source bucket.

        Raises:
            Exception: If there is an error during object copy or removal.

        Logs:
            Information about the moved objects.
        """
        objects = self.client.list_objects(bucket_name=source_bucket, prefix=prefix, recursive=True)
        for obj in objects:
            obj_name = obj.object_name
            self.client.copy_object(bucket_name=target_bucket, object_name=obj_name, source=CopySource(source_bucket, obj_name))
            self.client.remove_object(bucket_name=source_bucket, object_name=obj_name)
            self.log.info(f'Moved "{obj_name}" to "{target_bucket}"')

    def pick_object(self, bucket_name: str, prefix: str = None, strategy:str='fifo'):
        """
        Picks an object from the specified bucket based on the given strategy.

        Args:
            bucket_name (str): The name of the bucket.
            prefix (str, optional): The prefix to filter objects in the bucket. Defaults to None.
            strategy (str, optional): The strategy to use for picking the object. 
                                    Can be "fifo" (first in, first out) or "lifo" (last in, first out). Defaults to "fifo".

        Returns:
            minio.objects.Object or None: The picked object based on the strategy, or None if no objects are found.

        Raises:
            ValueError: If an invalid strategy is provided.
        """
        objects = list(self.client.list_objects(
            bucket_name, 
            prefix=prefix, 
            recursive=True
            )
        )
        if not objects:
            return None
        if strategy == 'fifo':
            return min(objects, key=lambda obj: obj.last_modified)
        if strategy == 'lifo':
            return max(objects, key=lambda obj: obj.last_modified)
        else:
            raise ValueError(f'Strategy can be "fifo" or "lifo". "{strategy}" is not a valid option.')
