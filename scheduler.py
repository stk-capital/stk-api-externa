import asyncio
from datetime import datetime, timedelta
import os
from motor.motor_asyncio import AsyncIOMotorClient
from services.langchain_services import execute_graph
import pymongo
import logging
from bson import ObjectId  # Add this import

import os
from dotenv import load_dotenv
load_dotenv()


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def run_scheduled_graphs():
    # Use the utility module for database connections
    from util.mongodb_utils import get_async_database
    db = get_async_database("crewai_db")
    
    # Old connection method:
    # client = AsyncIOMotorClient(os.getenv("MONGO_DB_URL"))
    # db = client.crewai_db
    #client = pymongo.MongoClient(os.getenv("MONGO_DB_URL"))

    while True:
        now = datetime.utcnow()
        logger.info(f"Checking for schedules at {now}")
        
        # schedules = list(db.schedules.find({
        #     "next_run": {"$lte": now}
        # }))
        schedules = await db.schedules.find({
            "next_run": {"$lte": now}
        }).to_list(None)

        for schedule in schedules:
            logger.info(f"Executing schedule: {schedule['_id']}")
            graph = await db.graphs.find_one({"_id": ObjectId(schedule["graph_id"])})
            if graph:
                try:
                    execution_log = []
                    async for result in execute_graph(graph, schedule["initial_input"]):
                        execution_log.append(result)
                    
                    await db.execution_logs.insert_one({
                        "schedule_id": schedule["_id"],
                        "graph_id": graph["_id"],
                        "execution_time": now,
                        "log": execution_log
                    })
                    logger.info(f"Execution completed for schedule {schedule['_id']}")
                except Exception as e:
                    logger.error(f"Error executing graph for schedule {schedule['_id']}: {str(e)}")
                    await db.execution_logs.insert_one({
                        "schedule_id": schedule["_id"],
                        "graph_id": graph["_id"],
                        "execution_time": now,
                        "error": str(e)
                    })

            # Update next run time
            next_run = calculate_next_run(schedule["frequency"], schedule["date"], schedule["time"])
            await db.schedules.update_one(
                {"_id": schedule["_id"]},
                {"$set": {"last_run": now, "next_run": next_run}}
            )

        await asyncio.sleep(30)  # Check every minute

def calculate_next_run(frequency: str, date: str, time_str: str) -> datetime:
    now = datetime.utcnow()
    
    # Remove seconds if present in the time string
    time_str = ':'.join(time_str.split(':')[:2])
    
    try:
        schedule_time = datetime.strptime(f"{date} {time_str}", "%Y-%m-%d %H:%M")
    except ValueError:
        # If the date is already a datetime object, just combine it with the time
        if isinstance(date, datetime):
            hour, minute = map(int, time_str.split(':'))
            schedule_time = date.replace(hour=hour, minute=minute, second=0, microsecond=0)
        else:
            raise ValueError(f"Invalid date or time format: date={date}, time={time_str}")
    
    if frequency == "once":
        return schedule_time if schedule_time > now else None
    elif frequency == "daily":
        if schedule_time <= now:
            schedule_time += timedelta(days=1)
    elif frequency == "weekly":
        while schedule_time <= now:
            schedule_time += timedelta(days=7)
    elif frequency == "monthly":
        while schedule_time <= now:
            month = schedule_time.month + 1
            year = schedule_time.year + month // 12
            month = month % 12 or 12
            day = min(schedule_time.day, (datetime(year, month+1, 1) - timedelta(days=1)).day)
            schedule_time = schedule_time.replace(year=year, month=month, day=day)
    
    return schedule_time

async def execute_schedule_now(schedule_id: str):
    # Use the utility module for database connections
    from util.mongodb_utils import get_async_database
    db = get_async_database("crewai_db")
    
    # Old connection method:
    # client = AsyncIOMotorClient(os.getenv("MONGO_DB_URL"))
    # db = client.crewai_db

    schedule = await db.schedules.find_one({"_id": ObjectId(schedule_id)})
    if not schedule:
        raise ValueError("Schedule not found")

    graph = await db.graphs.find_one({"_id": ObjectId(schedule["graph_id"])})
    if not graph:
        raise ValueError("Graph not found")

    now = datetime.utcnow()
    logger.info(f"Manually executing graph {graph['_id']} for schedule {schedule['_id']}")
    execution_log = []
    try:
        async for result in execute_graph(graph, schedule["initial_input"]):
            execution_log.append(result)
        
        # Store the execution log
        await db.execution_logs.insert_one({
            "schedule_id": schedule["_id"],
            "graph_id": graph["_id"],
            "execution_time": now,
            "log": execution_log,
            "manual_execution": True
        })
        logger.info(f"Manual execution completed for graph {graph['_id']}")
    except Exception as e:
        logger.error(f"Error executing graph {graph['_id']}: {str(e)}")
        await db.execution_logs.insert_one({
            "schedule_id": schedule["_id"],
            "graph_id": graph["_id"],
            "execution_time": now,
            "error": str(e),
            "manual_execution": True
        })
    
    return execution_log

async def start_scheduler():
    logger.info("Starting scheduler")
    await run_scheduled_graphs()