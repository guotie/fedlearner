import request from 'libs/request';
import {
  WorkflowForkPayload,
  WorkflowInitiatePayload,
  WorkflowTemplate,
  WorkflowAcceptPayload,
  Workflow,
  WorkflowState,
  WorkflowExecutionDetails,
  WorkflowTemplatePayload,
} from 'typings/workflow';

export function fetchWorkflowTemplateList(params?: {
  isLeft?: boolean;
  groupAlias?: string;
}): Promise<{ data: WorkflowTemplate[] }> {
  return request('/v2/workflow_templates', {
    params,
    removeFalsy: true,
    snake_case: true,
  });
}

export function getWorkflowTemplateById(id: ID) {
  return request(`/v2/workflow_templates/${id}`);
}

export function createWorkflowTemplate(payload: WorkflowTemplatePayload) {
  return request.post('/v2/workflow_templates', payload);
}

export function fetchWorkflowList(params?: {
  project?: ID;
  keyword?: string;
}): Promise<{ data: Workflow[] }> {
  return request('/v2/workflows', {
    params,
    removeFalsy: true,
    snake_case: true,
  });
}

export function getPeerWorkflowsConfig(
  id: string | number,
): Promise<{ data: Record<string, WorkflowExecutionDetails> }> {
  return request(`/v2/workflows/${id}/peer_workflows`);
}

export function getWorkflowDetailById(
  id: string | number,
): Promise<{ data: WorkflowExecutionDetails }> {
  return request(`/v2/workflows/${id}`);
}

export function initiateAWorkflow(payload: WorkflowInitiatePayload) {
  return request.post('/v2/workflows', payload);
}

export function acceptNFillTheWorkflowConfig(id: ID, payload: WorkflowAcceptPayload) {
  return request.put(`/v2/workflows/${id}`, payload);
}

export function runTheWorkflow(id: ID) {
  return request.patch(`/v2/workflows/${id}`, {
    target_state: WorkflowState.RUNNING,
  });
}

export function stopTheWorkflow(id: ID) {
  return request.patch(`/v2/workflows/${id}`, {
    target_state: WorkflowState.STOPPED,
  });
}

export function forkTheWorkflow(payload: WorkflowForkPayload) {
  return request.post(`/v2/workflows`, payload);
}

export function fetchJobLogs(
  jobName: string,
  params?: { startTime: DateTime; maxLines: number },
): Promise<{ data: string[] }> {
  return request(`/v2/jobs/${jobName}/log`, { params, snake_case: true });
}

export function toggleWofklowForkable(id: ID, val: boolean) {
  return request.patch(`/v2/workflows/${id}`, {
    forkable: val,
  });
}

export function fetchPodLogs(
  podName: string,
  params?: { startTime: DateTime; maxLines: number },
): Promise<{ data: string[] }> {
  return request(`/v2/pods/${podName}/log`, { params, snake_case: true });
}

export function fetchJobMpld3Metrics(id: ID): Promise<{ data: any[] }> {
  return request(`/v2/jobs/${id}/metrics`);
}
