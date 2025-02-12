# Copyright 2021 The Kubeflow Authors. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""GCP launcher for batch prediction jobs based on the AI Platform SDK."""

from . import job_remote_runner
from .utils import artifact_util, json_util
import json

ARTIFACT_PROPERTY_KEY_BIGQUERY_OUTPUT_TABLE = 'bigqueryOutputTable'
ARTIFACT_PROPERTY_KEY_BIGQUERY_OUTPUT_DATASET = 'bigqueryOutputDataset'
ARTIFACT_PROPERTY_KEY_GCS_OUTPUT_DIRECTORY = 'gcsOutputDirectory'
ARTIFACT_PROPERTY_KEY_UNMANAGED_CONTAINER_MODEL = 'unmanaged_container_model'


def create_batch_prediction_job_with_client(job_client, parent, job_spec):
  return job_client.create_batch_prediction_job(
      parent=parent, batch_prediction_job=job_spec)


def get_batch_prediction_job_with_client(job_client, job_name):
  return job_client.get_batch_prediction_job(name=job_name)


def insert_artifact_into_payload(executor_input, payload):
  job_spec = json.loads(payload)
  artifact = json.loads(executor_input).get('inputs', {}).get(
      'artifacts', {}).get(ARTIFACT_PROPERTY_KEY_UNMANAGED_CONTAINER_MODEL,
                           {}).get('artifacts')
  if artifact:
    job_spec[
        ARTIFACT_PROPERTY_KEY_UNMANAGED_CONTAINER_MODEL] = json_util.camel_case_to_snake_case_recursive(
            artifact[0].get('metadata', {}))
    job_spec[ARTIFACT_PROPERTY_KEY_UNMANAGED_CONTAINER_MODEL]['artifact_uri'] = artifact[0].get('uri')
  return json.dumps(job_spec)


def create_batch_prediction_job(
    type,
    project,
    location,
    payload,
    gcp_resources,
    executor_input,
):
  """Create and poll batch prediction job status till it reaches a final state.

  This follows the typical launching logic:
  1. Read if the batch prediction job already exists in gcp_resources
     - If already exists, jump to step 3 and poll the job status. This happens
     if the launcher container experienced unexpected termination, such as
     preemption
  2. Deserialize the payload into the job spec and create the batch prediction
  job
  3. Poll the batch prediction job status every
  job_remote_runner._POLLING_INTERVAL_IN_SECONDS seconds
     - If the batch prediction job is succeeded, return succeeded
     - If the batch prediction job is cancelled/paused, it's an unexpected
     scenario so return failed
     - If the batch prediction job is running, continue polling the status

  Also retry on ConnectionError up to
  job_remote_runner._CONNECTION_ERROR_RETRY_LIMIT times during the poll.
  """
  remote_runner = job_remote_runner.JobRemoteRunner(type, project, location,
                                                    gcp_resources)

  # Create batch prediction job if it does not exist
  job_name = remote_runner.check_if_job_exists()
  if job_name is None:
    job_name = remote_runner.create_job(
        create_batch_prediction_job_with_client,
        insert_artifact_into_payload(executor_input, payload))

  # Poll batch prediction job status until "JobState.JOB_STATE_SUCCEEDED"
  get_job_response = remote_runner.poll_job(
      get_batch_prediction_job_with_client, job_name)

  vertex_uri_prefix = f'https://{location}-aiplatform.googleapis.com/v1/'
  artifact_util.update_output_artifact(
      executor_input, 'batchpredictionjob',
      vertex_uri_prefix + get_job_response.name, {
          artifact_util.ARTIFACT_PROPERTY_KEY_RESOURCE_NAME:
              get_job_response.name,
          ARTIFACT_PROPERTY_KEY_BIGQUERY_OUTPUT_TABLE:
              get_job_response.output_info.bigquery_output_table,
          ARTIFACT_PROPERTY_KEY_BIGQUERY_OUTPUT_DATASET:
              get_job_response.output_info.bigquery_output_dataset,
          ARTIFACT_PROPERTY_KEY_GCS_OUTPUT_DIRECTORY:
              get_job_response.output_info.gcs_output_directory
      })
