import hashlib
import os
import shutil
from typing import List, Tuple

import numpy as np
import schemas
from config import settings
from fastapi import HTTPException, UploadFile
from loguru import logger


class Storage:
    def __init__(self):
        self.storage_path = os.path.join(settings.UPLOAD_PATH, settings.FOLDER_PREFIX)
        self.block_path: List[str] = [
            f"{self.storage_path}-{i}" for i in range(settings.NUM_DISKS)
        ]
        self.__create_storage_folder()

    def __create_storage_folder(self):
        # create 3 sub folders use to store file blocks
        for path in self.block_path:
            if not os.path.exists(path):
                # create storage folder if not exists
                logger.info(f"Creating storage folder: {path}")
                os.makedirs(path)
            else:
                # clear storage folder if exists
                logger.warning(f"Storage folder already exists: {path}")
                self.__clear_folder(path)

    def __clear_folder(self, folder: str):
        logger.warning(f"Clearing folder: {folder}")

        # clear folder
        for filename in os.listdir(folder):
            file_path = os.path.join(folder, filename)
            try:
                if os.path.isfile(file_path) or os.path.islink(file_path):
                    os.unlink(file_path)
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path)
            except OSError as err:
                logger.error(f"Failed to delete {file_path} because {err}")

    async def __write_file(self, block: int, data: bytes, filename: str):
        path = os.path.join(self.block_path[block], filename)
        with open(path, "wb") as f:
            f.write(data)

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

        return data_blocks, parity_block

    def exists(self, filename: str) -> bool:
        return os.path.exists(os.path.join(self.block_path[2], filename))

    async def save_file(self, file: UploadFile) -> schemas.File:
        # check if file exists
        if self.exists(file.filename):
            logger.warning(f"File already exists: {file.filename}")
            raise HTTPException(status_code=409, detail="File already exists")

        data = await file.read()
        checksum = hashlib.md5(data).hexdigest()

        # partition data to NUM_DISKS-1 blocks and a parity block
        data_blocks, parity_block = await self.__partition_data(data)

        # write data and parity to disk
        for i in range(settings.NUM_DISKS - 1):
            await self.__write_file(i, data_blocks[i], file.filename)
        await self.__write_file(settings.NUM_DISKS - 1, parity_block, file.filename)
        return schemas.File(
            name=file.filename,
            size=len(data),
            checksum=checksum,
            content=data.decode("utf-8"),
            content_type=file.content_type,
        )

    async def read_file(self, filename: str) -> bytes:
        # check if file exists
        if not self.exists(filename):
            logger.warning(f"File not found: {filename}")
            raise HTTPException(status_code=404, detail="File not found")

        # read data from disk
        data_blocks = []
        for i in range(settings.NUM_DISKS):
            path = os.path.join(self.block_path[i], filename)
            with open(path, "rb") as f:
                data_blocks.append(f.read().rstrip(b"\x00"))

        # return data
        return b"".join(data_blocks[:-1]).decode(encoding="utf-8")

    async def update_file(self, file: UploadFile) -> schemas.File:
        # check if file exists
        if not self.exists(file.filename):
            logger.warning(f"File not found: {file.filename}")
            raise HTTPException(status_code=404, detail="File not found")

        data = await file.read()
        checksum = hashlib.md5(data).hexdigest()

        # partition data to NUM_DISKS-1 blocks and a parity block
        data_blocks, parity_block = await self.__partition_data(data)

        # write data and parity to disk
        for i in range(settings.NUM_DISKS - 1):
            await self.__write_file(i, data_blocks[i], file.filename)
        await self.__write_file(settings.NUM_DISKS - 1, parity_block, file.filename)

        return schemas.File(
            name=file.filename,
            size=len(data),
            checksum=checksum,
            content=data.decode("utf-8"),
            content_type=file.content_type,
        )

    async def delete_file(self, filename: str) -> None:
        # check if file exists
        if not self.exists(filename):
            logger.warning(f"File not found: {filename}")
            raise HTTPException(status_code=404, detail="File not found")

        # delete all files, include data and parity
        for i in range(settings.NUM_DISKS):
            path = os.path.join(self.block_path[i], filename)
            os.remove(path)


storage: Storage = Storage()
