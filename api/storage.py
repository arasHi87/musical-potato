import hashlib
from pathlib import Path
from typing import List, Tuple

import aiofiles
import numpy as np
import schemas
from config import settings
from fastapi import HTTPException, UploadFile
from loguru import logger


class Storage:
    def __init__(self):
        self.block_path: List[Path] = [
            Path(settings.UPLOAD_PATH) / f"{settings.FOLDER_PREFIX}-{i}"
            for i in range(settings.NUM_DISKS)
        ]
        self.__create_block()

    def __create_block(self):
        for path in self.block_path:
            logger.warning(f"Creating folder: {path}")
            path.mkdir(parents=True, exist_ok=True)

    async def __partition_data(
        self, data: bytes
    ) -> Tuple[List[np.ndarray], np.ndarray]:
        # divide block NUM_DISK-1 and get the maximum length of the block
        data_blocks = np.array_split(
            np.frombuffer(data, dtype=np.uint8), settings.NUM_DISKS - 1
        )
        max_length = max(map(len, data_blocks))

        # padding 0 to the block in data_blocks to max_length
        # and create a parity block use to store parity data
        data_blocks = [
            np.pad(block, (0, max_length - len(block)), mode="constant")
            for block in data_blocks
        ]
        parity_block = np.zeros((max_length,), dtype=np.uint8)

        # calculate parity block
        for i in range(settings.NUM_DISKS - 1):
            parity_block ^= data_blocks[i]

        # return data_blocks and parity_block
        # for the top NUM_DISKS-1 blocks are data blocks
        # the last block is parity block
        return data_blocks, parity_block

    async def __write_file(self, file: UploadFile) -> schemas.File:
        data = await file.read()
        checksum = hashlib.md5(data).hexdigest()

        # partition data to NUM_DISKS-1 blocks and a parity block
        data_blocks, parity_block = await self.__partition_data(data)

        # write data to disk
        # the top NUM_DISKS-1 blocks are data blocks
        # the last block is parity block
        data_blocks.append(parity_block)
        for i in range(settings.NUM_DISKS):
            path = self.block_path[i] / file.filename
            async with aiofiles.open(path, "wb") as fp:
                await fp.write(data_blocks[i])

        return schemas.File(
            name=file.filename,
            size=len(data),
            checksum=checksum,
            content=data.decode("utf-8"),
            content_type=file.content_type,
        )

    def __delete_file(self, filename: str, missing_ok: bool = False) -> None:
        # delete all files, include data and parity
        for i in range(settings.NUM_DISKS):
            path = self.block_path[i] / filename
            path.unlink(missing_ok=missing_ok)

    def __parity_verify(
        self, data_blocks: List[np.ndarray], parity_block: np.ndarray
    ) -> bool:
        # calculate parity block
        verify_block = np.zeros((max(map(len, data_blocks)),), dtype=np.uint8)
        for i in range(settings.NUM_DISKS - 1):
            verify_block ^= data_blocks[i]

        # check if parity block is equal to the last block
        return np.array_equal(parity_block, verify_block)

    def __file_exists(self, filename: str) -> bool:
        # only check if file exists on all blocks
        for i in range(settings.NUM_DISKS):
            path = Path(self.block_path[i]) / filename
            if not path.exists():
                return False
        return True

    async def file_integrity(self, filename: str) -> bool:
        """
        file integrated must satisfy following conditions:
            1. all data blocks must exist
            2. parity block must exist
            3. parity verify must success

        if one of the above conditions is not satisfied
        the file does not exist
        and the file is considered to be damaged
        so we need to delete the file
        """

        # check if all data blocks and parity block exist
        if not self.__file_exists(filename):
            self.__delete_file(filename, missing_ok=True)
            return False

        # read data from disk and check parity
        data_blocks = []
        for block in self.block_path:
            async with aiofiles.open(block / filename, "rb") as fp:
                data_blocks.append(np.frombuffer(await fp.read(), dtype=np.uint8))
        if not self.__parity_verify(data_blocks[:-1], data_blocks[-1]):
            self.__delete_file(filename)
            return False

        # file is integrated
        return True

    async def create_file(self, file: UploadFile) -> schemas.File:
        # check if file exists
        if await self.file_integrity(file.filename):
            logger.warning(f"File already exists: {file.filename}")
            raise HTTPException(status_code=409, detail="File already exists")
        return await self.__write_file(file)

    async def read_file(self, filename: str) -> bytes:
        # check if file exists
        if not await self.file_integrity(filename):
            logger.warning(f"File not found: {filename}")
            raise HTTPException(status_code=404, detail="File not found")

        # read data from disk
        data_blocks = []
        for i in range(settings.NUM_DISKS):
            path = self.block_path[i] / filename
            data_blocks.append(path.read_bytes().rstrip(b"\x00"))

        # return data
        return b"".join(data_blocks[:-1]).decode(encoding="utf-8")

    async def update_file(self, file: UploadFile) -> schemas.File:
        # check if file exists
        if not await self.file_integrity(file.filename):
            logger.warning(f"File not found: {file.filename}")
            raise HTTPException(status_code=404, detail="File not found")
        return await self.__write_file(file)

    async def delete_file(self, filename: str) -> None:
        # check if file exists
        if not await self.file_integrity(filename):
            logger.warning(f"File not found: {filename}")
            raise HTTPException(status_code=404, detail="File not found")
        self.__delete_file(filename)


storage: Storage = Storage()
