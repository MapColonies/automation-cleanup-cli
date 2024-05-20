from mc_automation_tools.base_requests import send_get_request, send_delete_request


def delete_layer_from_mapproxy(layers_ids: list, mapproxy_url: str, token: str):
    """
    This method will create query params dict from the deletion list
    for the delete method of mapproxy config api

    param: layers_ids: layer ids list for config deletion
    """
    is_deleted = True
    params = {"layerNames[]": layers_ids, "token": token}
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
                              "shouldReturnAvailableActions": "false", "token": token}
        resp = send_get_request(url=job_manager_url, params=job_manager_params)
        response_content = resp.json()[0]
        job_id = response_content["id"]
        job_tasks = response_content["tasks"]
        tasks_list = create_tasks_deletion_list(tasks_dict=job_tasks)
        return {"job_id": job_id, "tasks": tasks_list}
    except Exception as e:
        return e


def delete_job_tasks(job_and_tasks: dict, job_manager_url: str, token: str):
    """
    This method will create delete request of job's tasks to the job manager url
    param: job_and_tasks: get job's tasks request response
    param: job_manager_url: job manager request url
    """
    is_deleted = True
    if job_and_tasks:
        job_id = job_and_tasks.get("job_id")
        tasks = job_and_tasks.get("tasks")
        task_list = create_tasks_deletion_list(tasks_dict=tasks)
        for task in task_list:
            params = {"jobId": job_id, "taskId": task, "token": token}
            try:
                resp = send_delete_request(url=job_manager_url, params=params)
                return is_deleted if resp.status_code == 200 else False
            except Exception as e:
                return e


def delete_job_by_id(job_id: str, job_manager_url: str, token: str):
    """
    This method will create delete job request from job manager
    param: job_id : the job_id for deletion
    param: job_manager_url: job manager requests url
    """
    is_deleted = True
    try:
        params = {"jobId": job_id, "token": token}
        resp = send_delete_request(url=job_manager_url, params=params)
        return is_deleted if resp.status_code == 200 else False

    except Exception as e:
        return e


def delete_job_task_by_ids(job_manager_url: str, product_id: str, token: str):
    job_tasks = get_tasks_and_job_by_product_id(job_manager_url=job_manager_url, product_id=product_id, token=token)
    job_id = job_tasks.get("job_id")
    # tasks = job_tasks.get("tasks")
    tasks_delete_state = delete_job_tasks(job_and_tasks=job_tasks, job_manager_url=job_manager_url, token=token)
    job_delete_state = delete_job_by_id(job_id=job_id, job_manager_url=job_manager_url, token=token)
    return {"job_deleted": job_delete_state, "task_deleted": tasks_delete_state}


def delete_record_by_id(record_id: str, catalog_manager_url: str, token: str):
    """
    This method will remove layer record from the catalog
    param: record_id: record id to be deleted from the catalog
    param: catalog manager requests url

    """
    is_deleted = True
    params = {"recordId": record_id, "token": token}
    try:
        resp = send_delete_request(url=catalog_manager_url, params=params)
        return is_deleted if resp.status_code == 200 else False
    except Exception as e:
        return e
