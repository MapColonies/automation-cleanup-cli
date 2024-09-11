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
        job_manager_url = job_manager_url + f"{job_id}"
        resp = send_delete_request(url=job_manager_url)
        return is_deleted if resp.status_code == 200 else False

    except Exception as e:
        return e


def config_job_variables(job):
    """
    This method config the job variables
    :param job: job to delete
    """
    job_id = job["id"]
    job_tasks = job["tasks"]
    tasks_list = create_tasks_deletion_list(tasks_dict=job_tasks)
    return {"job_id": job_id, "tasks": tasks_list}


def delete_jobs_tasks_by_ids(job_manager_url: str, product_id: str, token: str):
    jobs = get_tasks_and_job_by_product_id(job_manager_url=job_manager_url, product_id=product_id)
    jobs_status_dict = {}
    if jobs:
        for job in jobs:
            job_id = job["id"]
            job_tasks = job["tasks"]
            tasks_list = create_tasks_deletion_list(tasks_dict=job_tasks)
            job_tasks = config_job_variables(job)
            if job_tasks:
                tasks_delete_state = delete_job_tasks(job_and_tasks=job_tasks, job_manager_url=job_manager_url)
                job_delete_state = delete_job_by_id(job_id=job_tasks.get("job_id"), job_manager_url=job_manager_url, token=token)
                jobs_status_dict.update({job["id"] : {"job_deleted" : job_delete_state, "task_deleted" : tasks_delete_state}})
            else:
                job_delete_state = delete_job_by_id(job_id=job_tasks.get("job_id"), job_manager_url=job_manager_url, token=token)
                jobs_status_dict.update({job: {"job_deleted" : job_delete_state, "task_deleted" : "No Tasks was found to delete for the job"}})
        return jobs_status_dict
    else:
        return {"job_deleted" : "No jobs was found to delete"}


def get_tasks_and_job_by_product_id(job_manager_url: str, product_id: str):
    """
    This function will return list of jobs to delete
    """
    try:
        job_manager_params = {"resourceId": product_id, "shouldReturnTasks": "true",
                              "shouldReturnAvailableActions": "false"}

        resp = send_get_request(url=job_manager_url, params=job_manager_params)
        if resp.text != '[]':
            response_content = resp.json()
            return response_content
        else:
            return {}

    except Exception as e:
        return e


def delete_record_by_id(record_id: str, catalog_manager_url: str):
    """
    This method will remove layer record from the catalog
    param: record_id: record id to be deleted from the catalog
    param: catalog manager requests url

    """
    is_deleted = True
    try:
        deletion_url = catalog_manager_url + record_id
        resp = send_delete_request(url=deletion_url)
        return is_deleted if resp.status_code == 200 else False
    except Exception as e:
        return e
