import hashlib
import os
import shutil
from typing import List

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

    async def save_file(self, file: UploadFile) -> schemas.File:
        # check if file exists
        if os.path.exists(os.path.join(self.block_path[2], file.filename)):
            logger.warning(f"File already exists: {file.filename}")
            raise HTTPException(status_code=409, detail="File already exists")

        data = await file.read()
        checksum = hashlib.md5(data).hexdigest()

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


storage: Storage = Storage()
