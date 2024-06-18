import time

from mc_automation_tools.base_requests import send_get_request, send_delete_request


def delete_layer_from_mapproxy(layers_ids: list, mapproxy_url: str):
    """
    This method will create query params dict from the deletion list
    for the delete method of mapproxy config api

    param: layers_ids: layer ids list for config deletion
    """
    is_deleted = True
    params = {"layerNames": layers_ids}
    try:
        resp = send_delete_request(url=mapproxy_url, params=params)
        return is_deleted if resp.status_code == 200 else False
    except Exception as e:
        return e


def create_tasks_deletion_list(tasks_dict: dict):
    """
    This method will create job's tasks list for deletion
    param: tasks_dict: get tasks by job id response for deletion
    """
    task_to_delete = []
    if tasks_dict:
        for task in tasks_dict:
            task_id = task.get("id")
            task_to_delete.append(task_id)
        return task_to_delete
    else:
        return task_to_delete


def get_tasks_and_job_by_product_id(job_manager_url: str, product_id: str, token: str):
    """
    This function will return the job id and its tasks by giving product_id
    """
    try:
        job_manager_params = {"resourceId": product_id, "shouldReturnTasks": "true",
                              "shouldReturnAvailableActions": "false"}
        # job_manager_url = job_manager_url

        resp = send_get_request(url=job_manager_url, params=job_manager_params)
        if resp.text != '[]':
            response_content = resp.json()[0]

            job_id = response_content["id"]
            job_tasks = response_content["tasks"]
            tasks_list = create_tasks_deletion_list(tasks_dict=job_tasks)
            return {"job_id": job_id, "tasks": tasks_list}
        else:
            return {}

    except Exception as e:
        return e


def delete_job_tasks(job_and_tasks: dict, job_manager_url: str):
    """
    This method will create delete request of job's tasks to the job manager url
    param: job_and_tasks: get job's tasks request response
    param: job_manager_url: job manager request url
    """
    is_deleted = True
    task_state = []
    if job_and_tasks:
        job_id = job_and_tasks.get("job_id")
        task_list = job_and_tasks.get("tasks")
        for task_id in task_list:
            # params = {"jobId": job_id}
            try:
                task_deletion_url = job_manager_url + f"{job_id}" + "/tasks" + f"/{task_id}"
                resp = send_delete_request(url=task_deletion_url)
                is_deleted = is_deleted if resp.status_code == 200 else False
                task_state.append({"task_id": task_id, "is_deleted": is_deleted})
                # time.sleep(5)
            except Exception as e:
                return e
        task_state = [d for d in task_state if d.get("is_deleted") is True]
        return False if len(task_state) != len(task_list) else True


def delete_job_by_id(job_id: str, job_manager_url: str, token: str):
    """
    This method will create delete job request from job manager
    param: job_id : the job_id for deletion
    param: job_manager_url: job manager requests url
    """
    is_deleted = True
    try:
        # params = {"jobId": job_id, "token": token}
        job_manager_url = job_manager_url + f"{job_id}"
        resp = send_delete_request(url=job_manager_url)
        return is_deleted if resp.status_code == 200 else False

    except Exception as e:
        return e


def delete_job_task_by_ids(job_manager_url: str, product_id: str, token: str):
    job_tasks = get_tasks_and_job_by_product_id(job_manager_url=job_manager_url, product_id=product_id, token=token)
    job_id = job_tasks.get("job_id")
    # tasks = job_tasks.get("tasks")
    if job_tasks:
        tasks_delete_state = delete_job_tasks(job_and_tasks=job_tasks, job_manager_url=job_manager_url)
        job_delete_state = delete_job_by_id(job_id=job_id, job_manager_url=job_manager_url, token=token)
        return {"job_deleted": job_delete_state, "task_deleted": tasks_delete_state}
    else:
        return {"job_deleted": f"No job was found to delete for {product_id}",
                "task_deleted": f"No Tasks was found to delete for {product_id}"}


def delete_record_by_id(record_id: str, catalog_manager_url: str):
    """
    This method will remove layer record from the catalog
    param: record_id: record id to be deleted from the catalog
    param: catalog manager requests url

    """
    is_deleted = True
    # params = {"token": token}
    try:
        deletion_url = catalog_manager_url + record_id
        resp = send_delete_request(url=deletion_url)
        return is_deleted if resp.status_code == 200 else False
    except Exception as e:
        return e
