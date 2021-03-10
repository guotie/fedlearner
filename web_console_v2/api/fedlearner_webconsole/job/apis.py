# Copyright 2020 The FedLearner Authors. All Rights Reserved.
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
# coding: utf-8
import json
import time

from flask_restful import Resource, reqparse

from fedlearner_webconsole.exceptions import (
    NotFoundException, InternalException
)
from fedlearner_webconsole.job.metrics import JobMetricsBuilder
from fedlearner_webconsole.job.models import Job
from fedlearner_webconsole.k8s_client import get_client
from fedlearner_webconsole.proto import common_pb2
from fedlearner_webconsole.rpc.client import RpcClient
from fedlearner_webconsole.utils.es import es
from fedlearner_webconsole.utils.kibana import KibanaUtils
from fedlearner_webconsole.workflow.models import Workflow


class JobApi(Resource):
    def get(self, job_id):
        job = Job.query.filter_by(id=job_id).first()
        if job is None:
            raise NotFoundException()
        return {'data': job.to_dict()}

    # TODO: manual start jobs


class PodLogApi(Resource):
    def get(self, pod_name):
        parser = reqparse.RequestParser()
        parser.add_argument('start_time', type=int, location='args',
                            required=True,
                            help='start_time is required and must be timestamp')
        parser.add_argument('max_lines', type=int, location='args',
                    required=True,
                    help='max_lines is required')
        data = parser.parse_args()
        start_time = data['start_time']
        max_lines = data['max_lines']
        return {'data': es.query_log('filebeat-*', '', pod_name,
                                     start_time,
                                     int(time.time() * 1000))[:max_lines][::-1]}


class JobLogApi(Resource):
    def get(self, job_name):
        parser = reqparse.RequestParser()
        parser.add_argument('start_time', type=int, location='args',
                            required=True,
                            help='project_id is required and must be timestamp')
        parser.add_argument('max_lines', type=int, location='args',
                            required=True,
                            help='max_lines is required')
        data = parser.parse_args()
        start_time = data['start_time']
        max_lines = data['max_lines']
        return {'data': es.query_log('filebeat-*', job_name,
                                     'fedlearner-operator',
                                     start_time,
                                     int(time.time() * 1000))[:max_lines][::-1]}


class PodContainerApi(Resource):
    def get(self, job_id, pod_name):
        job = Job.query.filter_by(id=job_id).first()
        if job is None:
            raise NotFoundException()
        k8s = get_client()
        base = k8s.get_base_url()
        container_id = k8s.get_webshell_session(job.project.get_namespace(),
                                                pod_name,
                                                'tensorflow')
        return {'data': {'id': container_id, 'base': base}}


class JobMetricsApi(Resource):
    def get(self, job_id):
        job = Job.query.filter_by(id=job_id).first()
        if job is None:
            raise NotFoundException()

        metrics = JobMetricsBuilder(job).plot_metrics()

        # Metrics is a list of dict. Each dict can be rendered by frontend with
        #   mpld3.draw_figure('figure1', json)
        return {'data': metrics}


class PeerJobMetricsApi(Resource):
    def get(self, workflow_id, participant_id, job_name):
        workflow = Workflow.query.filter_by(id=workflow_id).first()
        if workflow is None:
            raise NotFoundException()
        project_config = workflow.project.get_config()
        party = project_config.participants[participant_id]
        client = RpcClient(project_config, party)
        resp = client.get_job_metrics(workflow.name, job_name)
        if resp.status.code != common_pb2.STATUS_SUCCESS:
            raise InternalException(resp.status.msg)

        metrics = json.loads(resp.metrics)

        # Metrics is a list of dict. Each dict can be rendered by frontend with
        #   mpld3.draw_figure('figure1', json)
        return {'data': metrics}


class KibanaMetricsApi(Resource):
    TSVB = ('Rate', 'Ratio', 'Numeric')
    TIMELION = ('Time',)

    def get(self, job_id):
        job = Job.query.filter_by(id=job_id).first()
        parser = reqparse.RequestParser()
        parser.add_argument('type', type=str, location='args',
                            required=True,
                            choices=('Rate', 'Ratio', 'Numeric', 'Time'),
                            help='Visualization type is required.')
        parser.add_argument('interval', type=str, location='args',
                            default='',
                            help='Time bucket interval length, '
                                 'defaults to automated by Kibana.')
        parser.add_argument('x_axis_field', type=str, location='args',
                            required=True,
                            help='Time field (X axis) is required.')
        parser.add_argument('query', type=str, location='args',
                            help='Additional query string to the graph.')
        parser.add_argument('start_time', type=int, location='args',
                            default=-1,
                            help='Earliest <x_axis_field> time of data.'
                                 'Unix timestamp.')
        parser.add_argument('end_time', type=int, location='args',
                            default=-1,
                            help='Latest <x_axis_field> time of data.'
                                 'Unix timestamp.')
        # (Joined) Rate visualization is fixed and only interval, query and
        # x_axis_field can be modified
        # Ratio visualization
        parser.add_argument('numerator', type=str, location='args',
                            help='Numerator is required in Ratio '
                                 'visualization.')
        parser.add_argument('denominator', type=str, location='args',
                            help='Denominator is required in Ratio '
                                 'visualization.')
        # Numeric visualization
        parser.add_argument('aggregator', type=str, location='args',
                            default='Average',
                            choices=('Average', 'Sum', 'Max', 'Min', 'Variance',
                                     'Std. Deviation', 'Sum of Squares'),
                            help='Aggregator type is required in Numeric '
                                 'visualization.')
        parser.add_argument('value_field', type=str, location='args',
                            help='The field to be aggregated on is required '
                                 'in Numeric visualization.')
        args = parser.parse_args()
        if args['type'] in KibanaMetricsApi.TSVB:
            iframe_src_list = KibanaUtils.create_tsvb(job, args)
            return iframe_src_list
        if args['type'] in KibanaMetricsApi.TIMELION:
            iframe_src_list = KibanaUtils.create_timelion(job, args)
            return iframe_src_list


def initialize_job_apis(api):
    api.add_resource(JobApi, '/jobs/<int:job_id>')
    api.add_resource(PodLogApi,
                     '/pods/<string:pod_name>/log')
    api.add_resource(JobLogApi,
                     '/jobs/<string:job_name>/log')
    api.add_resource(JobMetricsApi,
                     '/jobs/<int:job_id>/metrics')
    api.add_resource(PodContainerApi,
                     '/jobs/<int:job_id>/pods/<string:pod_name>/container')
    api.add_resource(PeerJobMetricsApi,
                     '/workflows/<int:workflow_id>/peer_workflows'
                     '/<int:participant_id>/jobs/<string:job_name>/metrics')
    api.add_resource(KibanaMetricsApi,
                     '/jobs/<int:job_id>/kibana')
