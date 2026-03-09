import pymongo
import pandas as pd
import time
import logging
import os
import pickle
from threading import Thread
import threading
from pymongo.errors import PyMongoError
from datetime import datetime, timedelta

from constants import (
    ROW_MARKER_COLUMN_NAME,
    CHANGE_STREAM_OPERATION_MAP,
    CHANGE_STREAM_OPERATION_MAP_WHEN_INIT,
    TYPES_TO_CONVERT_TO_STR,
    TEMP_PREFIX_DURING_INIT,
    DATA_FILES_PATH,
    DELTA_SYNC_CACHE_PARQUET_FILE_NAME,
    DELTA_SYNC_RESUME_TOKEN_FILE_NAME,
# added the two new files to save the initial sync status and last parquet file number
    INIT_SYNC_STATUS_FILE_NAME,
    LAST_PARQUET_FILE_NUMBER,
    DTYPE_KEY,
    TYPE_KEY,
)
from utils import to_string, get_parquet_full_path_filename, get_temp_parquet_full_path_filename, get_table_dir
from push_file_to_lz import push_file_to_lz
#from flags import get_init_flag
from init_sync import init_sync
import schemas
import schema_utils
from file_utils import FileType, read_from_file, write_to_file
from metrics_database import get_metrics_database


class BatchSizeManager:
    """
    Manages dynamic batch size scaling for delta sync.
    
    Scales up by 10x when high throughput is detected (5 consecutive full batches in 5 minutes).
    Scales down after 5 minutes of inactivity or 5 consecutive under-limit writes.
    """
    
    def __init__(self, default_size: int = 100, scale_factor: int = 10, logger=None):
        self.default_size = default_size
        self.scale_factor = scale_factor
        self.current_size = default_size
        self.is_scaled = False
        self.logger = logger
        
        # Tracking for scale-up: timestamps of full batch writes
        self.full_batch_timestamps = []
        
        # Tracking for scale-down
        self.under_limit_count = 0
        self.last_activity_time = time.time()
    
    def get_batch_size(self) -> int:
        """Get current batch size, checking for inactivity reset."""
        if self.is_scaled and (time.time() - self.last_activity_time) >= 300:
            self._scale_down("inactivity timeout (5 minutes)")
        return self.current_size
    
    def record_write(self, row_count: int):
        """Record a parquet write and check for scale up/down conditions."""
        self.last_activity_time = time.time()
        
        if self.is_scaled:
            # In scaled mode: check for scale-down conditions
            if row_count < self.default_size:
                self.under_limit_count += 1
                if self.under_limit_count >= 5:
                    self._scale_down(f"5 consecutive under-limit writes ({row_count} < {self.default_size})")
            else:
                self.under_limit_count = 0
        else:
            # In normal mode: check for scale-up conditions
            if row_count >= self.default_size:
                self.full_batch_timestamps.append(time.time())
                # Remove timestamps older than 5 minutes
                cutoff = time.time() - 300
                self.full_batch_timestamps = [t for t in self.full_batch_timestamps if t > cutoff]
                
                if len(self.full_batch_timestamps) >= 5:
                    self._scale_up()
            else:
                # Under-limit write in normal mode resets the consecutive count
                self.full_batch_timestamps = []
    
    def _scale_up(self):
        """Scale up batch size by the scale factor."""
        old_size = self.current_size
        self.current_size = self.default_size * self.scale_factor
        self.is_scaled = True
        self.full_batch_timestamps = []
        self.under_limit_count = 0
        
        if self.logger:
            self.logger.info(
                f"Scaling UP batch size from {old_size} to {self.current_size} "
                f"(5 consecutive full batches detected in 5 minutes)"
            )
    
    def _scale_down(self, reason: str):
        """Scale down batch size to default."""
        old_size = self.current_size
        self.current_size = self.default_size
        self.is_scaled = False
        self.full_batch_timestamps = []
        self.under_limit_count = 0
        
        if self.logger:
            self.logger.info(
                f"Scaling DOWN batch size from {old_size} to {self.current_size} ({reason})"
            )


def listening(collection_name: str):
    logger = logging.getLogger(f"{__name__}[{collection_name}]")
    
    try:
        _listening_impl(collection_name, logger)
    except Exception as e:
        logger.error(f"Unhandled exception in listening for {collection_name}: {e}", exc_info=True)
        raise


def _listening_impl(collection_name: str, logger: logging.Logger):
    db_name = os.getenv("MONGO_DB_NAME")
    logger.debug(f"db_name={db_name}")
    logger.debug(f"collection={collection_name}")
    # moved listening method so that it is called after the env variables are loaded
    time_threshold_in_sec = float(os.getenv("TIME_THRESHOLD_IN_SEC"))
    post_init_flush_done = False
    
    # Initialize dynamic batch size manager
    batch_manager = BatchSizeManager(
        default_size=int(os.getenv("DELTA_SYNC_BATCH_SIZE", 100)),
        scale_factor=10,
        logger=logger
    )

    # table_dir = get_table_dir(collection_name) #Never used
    resume_token = read_from_file(
        collection_name, DELTA_SYNC_RESUME_TOKEN_FILE_NAME, FileType.PICKLE
    )
    if resume_token:
        logger.info("interrupted incremental sync detected, continuing from saved checkpoint")
        logger.debug(f"resume_token value: {resume_token}")

    #MongoDB connection and data info
    client = pymongo.MongoClient(
        os.getenv("MONGO_CONN_STR"),
        # 0 or None = no driver‑side socket timeout
        socketTimeoutMS=None,
        # (optionally) set a sane connect timeout instead of a read timeout
        connectTimeoutMS=20000,
    )
    db = client[db_name]
    collection = db[collection_name]

    #cursor = collection.watch(full_document="updateLookup", resume_after=resume_token, max_await_time_ms=20000)

    # use df  - enables variable schemas
    # and consistent as resume_token is updated when file is pushed to LZ
    accumulative_df: pd.DataFrame = None
    init_sync_stat_flag = None
    last_sync_time: float | None = None

    # start init sync after we get cursor from Change Stream
    Thread(target=init_sync, args=(collection_name,)).start()
    logger.info(f"start listening to change stream for collection {collection_name}")
    
    # New main loop logic
    while True:
        # Build watch options each time we open a new stream
        watch_kwargs = dict(
            full_document="updateLookup",
            max_await_time_ms=20000,
        )
        if resume_token:
            watch_kwargs["resume_after"] = resume_token

        try:
            with collection.watch(**watch_kwargs) as stream:
                logger.info(
                    "opened change stream for %s%s",
                    collection_name,
                    " (resuming from checkpoint)" if resume_token else "",
                )
                logger.debug(f"resume_token: {resume_token}")
                last_action_time = datetime.now()
                # Use try_next so we can flush on time threshold even without new events
                while True:
                    before = time.time()
                    change = stream.try_next()
                    after = time.time()

                    if change is None:
                        if (datetime.now() - last_action_time >= timedelta(minutes=5)):
                            logger.info("no change; try_next() round-trip took %.3fs", after - before)
                            last_action_time = datetime.now()
                        # No new events in this await interval; consider time-based flush
                        if (
                            accumulative_df is not None
                            and init_sync_stat_flag == "Y"
                            and last_sync_time is not None
                        ):
                            accumulative_df, last_sync_time = process_accumulative_df(
                                accumulative_df,
                                collection_name,
                                init_sync_stat_flag,
                                last_sync_time,
                                time_threshold_in_sec,
                                resume_token,
                                logger,
                                batch_manager,
                            )
                        continue

                    # ---- We have a real change document here ----

                    if init_sync_stat_flag != "Y":
                        init_sync_stat_flag = read_from_file(
                            collection_name,
                            INIT_SYNC_STATUS_FILE_NAME,
                            FileType.PICKLE,
                        )

                    if init_sync_stat_flag == "Y" and not post_init_flush_done:
                        __post_init_flush(collection_name, logger)
                        post_init_flush_done = True

                    logger.debug("original change from Change Stream:")
                    logger.debug(change)

                    operationType = change["operationType"]
                    if operationType not in CHANGE_STREAM_OPERATION_MAP:
                        logger.error("ERROR: unsupported operation found: %s", operationType)
                        continue

                    if operationType == "delete":
                        doc: dict = change["documentKey"]
                    else:
                        doc: dict = change["fullDocument"]

                    df = pd.DataFrame([doc])

                    # Always update resume_token on every processed change
                    resume_token = change["_id"]
                    logger.debug("resume_token: %s", resume_token)

                    schema_utils.process_dataframe(collection_name, df, sync_type='delta')

                    if init_sync_stat_flag != "Y":
                        logger.debug(
                            "collection %s still initializing, use UPSERT instead of INSERT",
                            collection_name,
                        )
                        row_marker_value = CHANGE_STREAM_OPERATION_MAP_WHEN_INIT[
                            operationType
                        ]
                    else:
                        row_marker_value = CHANGE_STREAM_OPERATION_MAP[operationType]

                    df.insert(0, ROW_MARKER_COLUMN_NAME, [row_marker_value])
                    
                    # Record document fetched metric for delta sync
                    try:
                        metrics_db = get_metrics_database()
                        metrics_db.record_documents_fetched(
                            collection_name=collection_name,
                            sync_type='delta',
                            document_count=1
                        )
                    except Exception as e:
                        logger.debug(f"Failed to record documents fetched metric: {e}")

                    # Merge into accumulative_df until batch size/time threshold
                    if accumulative_df is not None:
                        accumulative_df = pd.concat(
                            [accumulative_df, df], ignore_index=True
                        )
                        logger.info(f"change stream: received {operationType}, accumulated {accumulative_df.shape[0]} documents pending sync")
                    else:
                        accumulative_df = df
                        last_sync_time = time.time()
                        logger.info(f"change stream: received {operationType}, started accumulating (1 document)")

                    accumulative_df, last_sync_time = process_accumulative_df(
                        accumulative_df,
                        collection_name,
                        init_sync_stat_flag,
                        last_sync_time,
                        time_threshold_in_sec,
                        resume_token,
                        logger,
                        batch_manager,
                    )

                # End inner while True

        except (
            pymongo.errors.ConnectionFailure,
            pymongo.errors.CursorNotFound,
            pymongo.errors.OperationFailure,
        ) as exc:
            # Detect non-resumable ChangeStreamHistoryLost / stale resume token.
            is_non_resumable = (
                isinstance(exc, pymongo.errors.OperationFailure)
                and (
                    exc.code == 286  # ChangeStreamHistoryLost
                    or exc.has_error_label("NonResumableChangeStreamError")
                )
            )

            if is_non_resumable:
                logger.error(
                    "Non-resumable Change Stream error (ChangeStreamHistoryLost) for collection %s: %s. "
                    "Clearing resume token and restarting from latest position.",
                    collection_name,
                    exc,
                    exc_info=True,
                )

                # Drop the bad resume token so the next loop does *not* send resume_after.
                resume_token = None

                # Persist that change so a restart doesn't reuse the stale token.
                write_to_file(
                    None,
                    collection_name,
                    DELTA_SYNC_RESUME_TOKEN_FILE_NAME,
                    FileType.PICKLE,
                )

                # Optional: clear any in-memory batch since we've lost continuity anyway.
                accumulative_df = None
                last_sync_time = None
            else:
                # Resumable errors: keep the last known resume_token.
                logger.warning(
                    "Resumable change stream error for collection %s: %s; "
                    "will reopen from last checkpoint",
                    collection_name,
                    exc,
                    exc_info=True,
                )
                logger.debug(f"resume_token: {resume_token}")

            # Outer while True will rebuild watch_kwargs and reopen.
            # Slight backoff to avoid tight reconnect loop
            time.sleep(2)
            continue

        # If we ever exit the inner loop *without* an exception:
        # check stream.alive to see if the server closed the cursor.
        if not stream.alive:
            logger.warning(
                "change stream closed by server for collection %s; reopening from last checkpoint",
                collection_name,
            )
            logger.debug(f"resume_token: {resume_token}")
            # Outer while True will reopen
            continue

##>> enhancement to check time elapsed even if no event comes - no waiting indefinitely for a change
def process_accumulative_df(accumulative_df, collection_name, init_sync_stat_flag, last_sync_time, time_threshold_in_sec, resume_token, logger, batch_manager):
    # Get current dynamic batch size (also checks for inactivity reset)
    current_batch_size = batch_manager.get_batch_size()
    
    if not init_sync_stat_flag == "Y":
        # During init sync, use default batch size (no scaling during init)
        default_batch_size = int(os.getenv("DELTA_SYNC_BATCH_SIZE", 100))
        if (accumulative_df is not None
            and (
                (accumulative_df.shape[0] >= default_batch_size)
            )
        ):
            prefix = TEMP_PREFIX_DURING_INIT
            parquet_full_path_filename = get_temp_parquet_full_path_filename(
                collection_name, prefix=prefix
            )
            logger.info(f"writing TEMP parquet file: {parquet_full_path_filename}")
            accumulative_df.to_parquet(parquet_full_path_filename)
            accumulative_df = None
    else:        
        if (accumulative_df is not None
        ):
            if(
                (accumulative_df.shape[0] >= current_batch_size)
                or ((time.time() - last_sync_time) >= time_threshold_in_sec)
            ):
                prefix = ""
                last_parquet_file_num = read_from_file(
                    collection_name, LAST_PARQUET_FILE_NUMBER, FileType.PICKLE
                )
                if not last_parquet_file_num:
                    last_parquet_file_num = 0

                parquet_full_path_filename = get_parquet_full_path_filename(collection_name, last_parquet_file_num)

                logger.info(f"writing parquet file: {parquet_full_path_filename}")
                row_count = len(accumulative_df)
                accumulative_df.to_parquet(parquet_full_path_filename)
                accumulative_df = None

                push_file_to_lz(parquet_full_path_filename, collection_name)
                
                # Record parquet file metric
                try:
                    file_size = os.path.getsize(parquet_full_path_filename)
                    metrics_db = get_metrics_database()
                    metrics_db.record_parquet_file(
                        collection_name=collection_name,
                        file_name=os.path.basename(parquet_full_path_filename),
                        file_size_bytes=file_size,
                        row_count=row_count,
                        sync_type='delta'
                    )
                except Exception as e:
                    logger.debug(f"Failed to record parquet file metric: {e}")
                
                # Record write for dynamic batch size scaling
                batch_manager.record_write(row_count)
                logger.debug(f"batch size status: current={batch_manager.current_size}, scaled={batch_manager.is_scaled}")
                
            #    resume_token = change["_id"]
                logger.info("saving checkpoint: resume_token written to file")
                logger.debug(f"resume_token value: {resume_token}")
                write_to_file(
                    resume_token,
                    collection_name,
                    DELTA_SYNC_RESUME_TOKEN_FILE_NAME,
                    FileType.PICKLE,
                )
                last_parquet_file_num +=  1
                logger.info(f"saving checkpoint: parquet file #{last_parquet_file_num}")
                write_to_file(
                    last_parquet_file_num,
                    collection_name,
                    LAST_PARQUET_FILE_NUMBER,
                    FileType.PICKLE,
            )
    return accumulative_df, last_sync_time

def __post_init_flush(table_name: str, logger):
    if not logger:
        logger = logging.getLogger(f"{__name__}[{table_name}]")
    logger.info(f"begin post init flush of delta change for collection {table_name}")
    current_dir = os.path.dirname(os.path.abspath(__file__))
    table_dir = get_table_dir(table_name)
    if not os.path.exists(table_dir):
        return
    temp_parquet_filename_list = sorted(
        [
            filename
            for filename in os.listdir(table_dir)
            if os.path.splitext(filename)[1] == ".parquet"
            and os.path.splitext(filename)[0].startswith(TEMP_PREFIX_DURING_INIT)
        ]
    )
    for temp_parquet_filename in temp_parquet_filename_list:
        temp_parquet_full_path = os.path.join(table_dir, temp_parquet_filename)
        # changed to get last parquet file number from LZ for resilience
        #new_parquet_full_path = get_parquet_full_path_filename(table_name)
        last_parquet_file_num = read_from_file(
            table_name, LAST_PARQUET_FILE_NUMBER, FileType.PICKLE
        )
        if not last_parquet_file_num:
            last_parquet_file_num = 0
        new_parquet_full_path = get_parquet_full_path_filename(table_name, last_parquet_file_num)   
        logger.debug("renaming temp parquet file")
        logger.debug(f"old name: {temp_parquet_full_path}")
        logger.debug(f"new name: {new_parquet_full_path}")
        logger.info(
            f"renaming parquet file from {temp_parquet_full_path} to {new_parquet_full_path}"
        )
        os.rename(temp_parquet_full_path, new_parquet_full_path)
        push_file_to_lz(new_parquet_full_path, table_name)
        # write last parquet file number to file
        last_parquet_file_num +=  1
        logger.info(f"saving checkpoint: parquet file #{last_parquet_file_num}")
        write_to_file(
            last_parquet_file_num,
            table_name,
            LAST_PARQUET_FILE_NUMBER,
            FileType.PICKLE,
        )
