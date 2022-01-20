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
"""Test google-cloud-pipeline-components to ensure they compile correctly."""

import json
import os
from google_cloud_pipeline_components.experimental import dataproc
import kfp
from kfp.v2 import compiler
import unittest


class ComponentsCompileTest(unittest.TestCase):
  """Pipeline compilation tests cases for Dataproc Batch components."""

  def setUp(self):
    super(ComponentsCompileTest, self).setUp()
    self._project = 'test-project'
    self._location = 'test-location'
    self._batch_id = 'test-batch-id'
    self._labels = {'foo': 'bar', 'fizz': 'buzz'}
    self._main_python_file_uri = 'testPythonFileUri'
    self._main_jar_file_uri = 'testJarFileUri'
    self._main_r_file_uri = 'testRFileUri'
    self._query_file_uri = 'testQueryFileUri'
    self._python_file_uris = ['testPythonFileUri1', 'testPythonFileUri2']
    self._jar_file_uris = ['testJarFileUri1', 'testJarFileUri2']
    self._file_uris = ['testFileUri1', 'testFileUri2']
    self._archive_uris = ['testArchiveUri1', 'testArchiveUri2']
    self._script_variables = {'foo': 'bar', 'fizz': 'buzz'}
    self._package_path = os.path.join(
        os.getenv('TEST_UNDECLARED_OUTPUTS_DIR'), 'pipeline.json')

  def tearDown(self):
    super(ComponentsCompileTest, self).tearDown()
    if os.path.exists(self._package_path):
      os.remove(self._package_path)

  def test_dataproc_create_pyspark_batch_op_compile(self):
    """Compile a test pipeline using the Dataproc PySparkBatch component."""
    @kfp.dsl.pipeline(name='create-pyspark-batch-test')
    def pipeline():
      dataproc.DataprocPySparkBatchOp(
          project=self._project,
          location=self._location,
          batch_id=self._batch_id,
          main_python_file_uri=self._main_python_file_uri,
          python_file_uris=self._python_file_uris,
          jar_file_uris=self._jar_file_uris,
          file_uris=self._file_uris,
          archive_uris=self._archive_uris,
          labels=self._labels)

    compiler.Compiler().compile(
        pipeline_func=pipeline, package_path=self._package_path)
    with open(self._package_path) as f:
      executor_output_json = json.load(f, strict=False)

    with open('testdata/dataproc_create_pyspark_batch_component_pipeline.json') as ef:
      expected_executor_output_json = json.load(ef, strict=False)

    # Ignore the kfp SDK & schema version during comparison
    del executor_output_json['sdkVersion']
    del executor_output_json['schemaVersion']
    self.assertDictEqual(executor_output_json, expected_executor_output_json)

  def test_dataproc_create_spark_batch_op_compile(self):
    """Compile a test pipeline using the Dataproc SparkBatch component."""
    @kfp.dsl.pipeline(name='create-spark-batch-test')
    def pipeline():
      dataproc.DataprocSparkBatchOp(
          project=self._project,
          location=self._location,
          batch_id=self._batch_id,
          main_jar_file_uri=self._main_jar_file_uri,
          jar_file_uris=self._jar_file_uris,
          file_uris=self._file_uris,
          archive_uris=self._archive_uris,
          labels=self._labels)

    compiler.Compiler().compile(
        pipeline_func=pipeline, package_path=self._package_path)
    with open(self._package_path) as f:
      executor_output_json = json.load(f, strict=False)

    with open('testdata/dataproc_create_spark_batch_component_pipeline.json') as ef:
      expected_executor_output_json = json.load(ef, strict=False)

    # Ignore the kfp SDK & schema version during comparison
    del executor_output_json['sdkVersion']
    del executor_output_json['schemaVersion']
    self.assertDictEqual(executor_output_json, expected_executor_output_json)

  def test_dataproc_create_spark_r_batch_op_compile(self):
    """Compile a test pipeline using the Dataproc SparkRBatch component."""
    @kfp.dsl.pipeline(name='create-spark-r-batch-test')
    def pipeline():
      dataproc.DataprocSparkRBatchOp(
          project=self._project,
          location=self._location,
          batch_id=self._batch_id,
          main_r_file_uri=self._main_r_file_uri,
          file_uris=self._file_uris,
          archive_uris=self._archive_uris,
          labels=self._labels)

    compiler.Compiler().compile(
        pipeline_func=pipeline, package_path=self._package_path)
    with open(self._package_path) as f:
      executor_output_json = json.load(f, strict=False)

    with open('testdata/dataproc_create_spark_r_batch_component_pipeline.json') as ef:
      expected_executor_output_json = json.load(ef, strict=False)

    # Ignore the kfp SDK & schema version during comparison
    del executor_output_json['sdkVersion']
    del executor_output_json['schemaVersion']
    self.assertDictEqual(executor_output_json, expected_executor_output_json)

  def test_dataproc_create_spark_sql_batch_op_compile(self):
    """Compile a test pipeline using the Dataproc SparkSqlBatch component."""
    @kfp.dsl.pipeline(name='create-spark-sql-batch-test')
    def pipeline():
      dataproc.DataprocSparkSqlBatchOp(
          project=self._project,
          location=self._location,
          batch_id=self._batch_id,
          query_file_uri=self._query_file_uri,
          jar_file_uris=self._jar_file_uris,
          script_variables=self._script_variables,
          labels=self._labels)

    compiler.Compiler().compile(
        pipeline_func=pipeline, package_path=self._package_path)
    with open(self._package_path) as f:
      executor_output_json = json.load(f, strict=False)

    with open('testdata/dataproc_create_spark_sql_batch_component_pipeline.json') as ef:
      expected_executor_output_json = json.load(ef, strict=False)

    # Ignore the kfp SDK & schema version during comparison
    del executor_output_json['sdkVersion']
    del executor_output_json['schemaVersion']
    self.assertDictEqual(executor_output_json, expected_executor_output_json)
