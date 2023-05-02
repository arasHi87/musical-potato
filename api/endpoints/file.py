import schemas
from fastapi import APIRouter, UploadFile, status
from storage import storage

router = APIRouter()


@router.post(
    "/",
    status_code=status.HTTP_201_CREATED,
    response_model=schemas.File,
    name="file:create_file",
)
async def create_file(file: UploadFile) -> schemas.File:
    return await storage.save_file(file)


@router.get("/", status_code=status.HTTP_200_OK, name="file:get_file")
async def read_file(filename: str) -> str:
    return await storage.read_file(filename)


@router.put("/", status_code=status.HTTP_200_OK, name="file:update_file")
async def update_file(file: UploadFile) -> schemas.File:
    return await storage.update_file(file)
