from botocore.exceptions import ClientError, EndpointConnectionError
import functools


class ObjectStorageInitError(Exception):
    pass


class ObjectStorageError(Exception):
    pass


class ObjectNotFoundError(ObjectStorageError):
    pass


class UploadLimitError(ObjectStorageError):
    pass


def maybe_unknown_error(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            res = func(*args, **kwargs)
        except (ClientError, EndpointConnectionError) as e:
            raise ObjectStorageError(str(e)) from e

        return res

    return wrapper


def maybe_not_found(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            res = func(*args, **kwargs)
        except ClientError as e:
            if e.response["ResponseMetadata"]["HTTPStatusCode"] == 404:
                raise ObjectNotFoundError("Object not found in storage") from e
            raise e

        return res

    return wrapper
