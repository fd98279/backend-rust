from pydantic import BaseModel


class QueryResponse(BaseModel):
    """
    A Pydantic model representing a response containing a presigned URL.

    Attributes:
        PresignedURL (str): A presigned URL that provides temporary access to a resource,
            typically used for secure, time-limited access to private S3 objects.
    """
    PresignedURL: str
