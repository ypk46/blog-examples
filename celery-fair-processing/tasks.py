import logging
from time import sleep

import redis
from celery import Celery, Task

logger = logging.getLogger(__name__)

app = Celery(
    "tasks",
    broker="redis://localhost:6379/0",
    broker_connection_retry_on_startup=True,
)

app.conf.update(
    task_acks_late=True,
    worker_prefetch_multiplier=1,
    worker_concurrency=1,
)

app.conf.broker_transport_options = {
    "priority_steps": list(range(10)),
    "sep": ":",
    "queue_order_strategy": "priority",
}

redis_client = redis.StrictRedis(host="localhost", port=6379, db=1)


def calculate_priority(tenant_id):
    """
    Calculate the priority of the task based on the number of tasks
    for the tenant.
    """
    key = f"tenant:{tenant_id}:task_count"
    task_count = int(redis_client.get(key) or 0)
    return min(10, task_count // 10)


class TenantAwareTask(Task):
    def on_success(self, retval, task_id, args, kwargs):
        tenant_id = kwargs.get("tenant_id")

        if tenant_id:
            key = f"tenant:{tenant_id}:task_count"
            redis_client.decr(key, 1)

        return super().on_success(retval, task_id, args, kwargs)


@app.task(name="tasks.send_email", base=TenantAwareTask)
def send_email(tenant_id, task_data):
    """
    Simulate sending an email.
    """
    sleep(1)
    key = f"tenant:{tenant_id}:task_count"
    task_count = int(redis_client.get(key) or 0)
    logger.info("Tenant %s tasks: %s", tenant_id, task_count)


if __name__ == "__main__":
    tenant_id = 1
    for _ in range(100):
        priority = calculate_priority(tenant_id)
        send_email.apply_async(
            kwargs={"tenant_id": tenant_id, "task_data": {}}, priority=priority
        )
        key = f"tenant:{tenant_id}:task_count"
        redis_client.incr(key, 1)

    tenant_id = 2
    for _ in range(10):
        priority = calculate_priority(tenant_id)
        send_email.apply_async(
            kwargs={"tenant_id": tenant_id, "task_data": {}}, priority=priority
        )
        key = f"tenant:{tenant_id}:task_count"
        redis_client.incr(key, 1)
