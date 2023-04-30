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
