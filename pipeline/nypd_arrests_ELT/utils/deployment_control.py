"""
To pause future deployment schedules when criteria is met.
"""

import json
from prefect_shell import ShellOperation


def get_deployment_details():

    flow_name = "flow-elt-arrests"
    deployment_name = "deploy-nypd-arrests-pipeline"

    deployment_param = f"{flow_name}/{deployment_name}"

    result = ShellOperation(
        commands=[
            f'prefect deployment inspect "{deployment_param}" --output json'
        ],
        stream_output=False
    ).run()

    raw_json = "".join(result)
    cleaned_json = raw_json.replace("\\\n", "").replace("\n", "")
    data = json.loads(cleaned_json)

    return data


def pause_deployment_schedule(data):

    flow_name = "flow-elt-arrests"

    deployment_details = {
        "name": data['name'],
        "id": data['id'],
        "schedule_id": data['schedules'][0]['id'],
    }

    try:

        ShellOperation(
            commands=[
                f"prefect deployment schedule pause "
                f'"{flow_name}/{deployment_details["name"]}" '
                f'"{deployment_details["schedule_id"]}"'
            ]
        ).run()

        print(
            f"Deployment schedule paused: "
            f"{deployment_details['name']}"
        )

        return deployment_details

    except Exception as e:

        print(f"Error: {e}")

        return None
