from mc_automation_tools.base_requests import send_get_request, send_delete_request


def create_query_params(layers_ids: list):
    """
    This method will create query params dict from the deletion list
    for the delete method of mapproxy config api

    param: layers_ids: layer ids list for config deletion
    """
    query_params = {"layerNames[]": layers_ids}
    return query_params


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


def get_tasks_and_job_by_product_id(job_manager_url: str, product_id: str):
    """
    This function will return the job id and its tasks by giving product_id
    """
    try:
        job_manager_params = {"resourceId": product_id, "shouldReturnTasks": "true",
                              "shouldReturnAvailableActions": "false"}
        resp = send_get_request(url=job_manager_url, params=job_manager_params)
        response_content = resp.json()[0]
        job_id = response_content["id"]
        job_tasks = response_content["tasks"]
        tasks_list = create_tasks_deletion_list(tasks_dict=job_tasks)
        return {"job_id": job_id, "tasks": tasks_list}
    except Exception as e:
        return e


def delete_job_tasks(job_and_tasks: dict, job_manager_url: str):
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
            params = {"jobId": job_id, "taskId": task}
            try:
                resp = send_delete_request(url=job_manager_url, params=params)
                is_deleted = is_deleted if resp.status_code == 200 else False

            except Exception as e:
                return e
        return is_deleted


def delete_job_by_id(job_id: str, job_manager_url: str):
    """
    This method will create delete job request from job manager
    param: job_id : the job_id for deletion
    param: job_manager_url: job manager request url
    """
    is_deleted = True
    try:
        params = {"jobId": job_id}
        resp = send_delete_request(url=job_manager_url, params=params)
        is_deleted = is_deleted if resp.status_code == 200 else False

    except Exception as e:
        return e
    return is_deleted
