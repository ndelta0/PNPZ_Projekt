import asyncio
import logging
import os
import sys
import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any

import grpc
from fastapi import FastAPI, Header, HTTPException, Request, status
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        if hasattr(record, "request_id"):
            payload["request_id"] = record.request_id
        if hasattr(record, "method"):
            payload["method"] = record.method
        if hasattr(record, "path"):
            payload["path"] = record.path
        if hasattr(record, "status_code"):
            payload["status_code"] = record.status_code
        if hasattr(record, "duration_ms"):
            payload["duration_ms"] = record.duration_ms
        return str(payload)


def configure_logging() -> logging.Logger:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())

    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(os.getenv("LOG_LEVEL", "INFO").upper())

    return logging.getLogger("dummy_backend")


logger = configure_logging()


class KeyValueItem(BaseModel):
    key: str = Field(min_length=1, max_length=256)
    value: str = Field(default="")


class KeyValueItemUpdate(BaseModel):
    value: str = Field(default="")


class KeyValueItemResponse(BaseModel):
    key: str
    value: str


class GrpcDbConfig(BaseModel):
    host: str = Field(default_factory=lambda: os.getenv("DB_GRPC_HOST", "dummy_db"))
    port: int = Field(default_factory=lambda: int(os.getenv("DB_GRPC_PORT", "50051")))


def load_config() -> GrpcDbConfig:
    return GrpcDbConfig()


def _import_generated_proto() -> Any:
    """
    The DB implementation is in Go, so the generated Python stubs may not exist
    in this repository. To keep this backend runnable, we generate Python stubs
    at runtime if the proto file is present.

    This keeps the backend self-contained for development and container usage.
    """
    try:
        from dummy_db_pb2 import (  # type: ignore
            DeleteRequest,
            GetRequest,
            KeyValue,
            ListRequest,
            SetRequest,
        )
        from dummy_db_pb2_grpc import KeyValueStoreStub  # type: ignore

        return {
            "DeleteRequest": DeleteRequest,
            "GetRequest": GetRequest,
            "KeyValue": KeyValue,
            "ListRequest": ListRequest,
            "SetRequest": SetRequest,
            "KeyValueStoreStub": KeyValueStoreStub,
        }
    except Exception:
        pass

    proto_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "proto"))
    if not os.path.exists(proto_dir):
        raise RuntimeError("gRPC proto-generated Python stubs are missing and the proto directory was not found.")

    generated_dir = os.path.join(os.path.dirname(__file__), "_generated")
    os.makedirs(generated_dir, exist_ok=True)

    proto_file = os.path.join(proto_dir, "dummydb.proto")
    from grpc_tools import protoc

    args = [
        "protoc",
        f"-I{proto_dir}",
        f"--python_out={generated_dir}",
        f"--grpc_python_out={generated_dir}",
        proto_file,
    ]
    result = protoc.main(args)
    if result != 0:
        raise RuntimeError("Failed to generate Python gRPC stubs from dummydb.proto")

    if generated_dir not in sys.path:
        sys.path.insert(0, generated_dir)

    from dummydb_pb2 import DeleteRequest, GetRequest, KeyValue, ListRequest, SetRequest  # type: ignore
    from dummydb_pb2_grpc import KeyValueStoreStub  # type: ignore

    return {
        "DeleteRequest": DeleteRequest,
        "GetRequest": GetRequest,
        "KeyValue": KeyValue,
        "ListRequest": ListRequest,
        "SetRequest": SetRequest,
        "KeyValueStoreStub": KeyValueStoreStub,
    }


PROTO = _import_generated_proto()


@dataclass
class GrpcStoreClient:
    host: str
    port: int

    def __post_init__(self) -> None:
        self.channel = grpc.aio.insecure_channel(f"{self.host}:{self.port}")
        self.stub = PROTO["KeyValueStoreStub"](self.channel)

    async def close(self) -> None:
        await self.channel.close()

    async def list_items(self) -> list[KeyValueItemResponse]:
        response = await self.stub.List(PROTO["ListRequest"]())
        if getattr(response, "error", ""):
            raise RuntimeError(response.error)
        return [KeyValueItemResponse(key=item.key, value=item.value) for item in response.items]

    async def get_item(self, key: str) -> KeyValueItemResponse | None:
        response = await self.stub.Get(PROTO["GetRequest"](key=key))
        if getattr(response, "error", ""):
            raise RuntimeError(response.error)
        if not response.found:
            return None
        return KeyValueItemResponse(key=key, value=response.value)

    async def set_item(self, key: str, value: str) -> None:
        response = await self.stub.Set(PROTO["SetRequest"](key=key, value=value))
        if getattr(response, "error", ""):
            raise RuntimeError(response.error)

    async def delete_item(self, key: str) -> bool:
        response = await self.stub.Delete(PROTO["DeleteRequest"](key=key))
        if getattr(response, "error", ""):
            raise RuntimeError(response.error)
        return bool(response.ok)


def get_request_id(x_request_id: str | None = Header(default=None)) -> str:
    return x_request_id or str(uuid.uuid4())


@asynccontextmanager
async def lifespan(app: FastAPI):
    config = load_config()
    app.state.grpc_client = GrpcStoreClient(config.host, config.port)
    logger.info(
        "backend_starting",
        extra={"method": "startup", "path": f"{config.host}:{config.port}", "request_id": "-"},
    )
    try:
        yield
    finally:
        await app.state.grpc_client.close()
        logger.info(
            "backend_stopping",
            extra={"method": "shutdown", "path": f"{config.host}:{config.port}", "request_id": "-"},
        )


app = FastAPI(title="Dummy Backend", version="1.0.0", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.middleware("http")
async def request_logging_middleware(request: Request, call_next):
    request_id = request.headers.get("x-request-id", str(uuid.uuid4()))
    start = asyncio.get_event_loop().time()
    try:
        response = await call_next(request)
        duration_ms = round((asyncio.get_event_loop().time() - start) * 1000, 2)
        response.headers["X-Request-ID"] = request_id
        logger.info(
            "request_completed",
            extra={
                "request_id": request_id,
                "method": request.method,
                "path": request.url.path,
                "status_code": response.status_code,
                "duration_ms": duration_ms,
            },
        )
        return response
    except Exception:
        duration_ms = round((asyncio.get_event_loop().time() - start) * 1000, 2)
        logger.exception(
            "request_failed",
            extra={
                "request_id": request_id,
                "method": request.method,
                "path": request.url.path,
                "duration_ms": duration_ms,
            },
        )
        raise


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.get("/items", response_model=list[KeyValueItemResponse])
async def list_items(request: Request):
    client: GrpcStoreClient = request.app.state.grpc_client
    try:
        return await client.list_items()
    except RuntimeError as exc:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(exc)) from exc


@app.get("/items/{key}", response_model=KeyValueItemResponse)
async def get_item(key: str, request: Request):
    client: GrpcStoreClient = request.app.state.grpc_client
    try:
        item = await client.get_item(key)
        if item is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Item not found")
        return item
    except HTTPException:
        raise
    except RuntimeError as exc:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(exc)) from exc


@app.post("/items", response_model=KeyValueItemResponse, status_code=status.HTTP_201_CREATED)
async def create_item(payload: KeyValueItem, request: Request):
    client: GrpcStoreClient = request.app.state.grpc_client
    try:
        await client.set_item(payload.key, payload.value)
        return KeyValueItemResponse(key=payload.key, value=payload.value)
    except RuntimeError as exc:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(exc)) from exc


@app.put("/items/{key}", response_model=KeyValueItemResponse)
async def update_item(key: str, payload: KeyValueItemUpdate, request: Request):
    client: GrpcStoreClient = request.app.state.grpc_client
    try:
        await client.set_item(key, payload.value)
        return KeyValueItemResponse(key=key, value=payload.value)
    except RuntimeError as exc:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(exc)) from exc


@app.delete("/items/{key}")
async def delete_item(key: str, request: Request):
    client: GrpcStoreClient = request.app.state.grpc_client
    try:
        deleted = await client.delete_item(key)
        if not deleted:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Item not found")
        return {"deleted": True, "key": key}
    except HTTPException:
        raise
    except RuntimeError as exc:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(exc)) from exc