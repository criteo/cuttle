// @flow
import type { PageId } from "./state";
import type Project from "./datamodel/project";
import type { Workflow } from "./datamodel/workflow";

type Status = "success" | "error";
type Dispatch = (action: Action) => void;

export type Action = INIT | NAVIGATE | LOAD_PROJECT_DATA;

// Actions
type INIT = { type: "INIT" };
export const init = (): INIT => ({ type: "INIT" });

// Action that should be dispatched by the "redux-url router"
type NAVIGATE = { type: "NAVIGATE", pageId: PageId };
export const navigToPage = (pageId: PageId): NAVIGATE => ({
  type: "NAVIGATE",
  pageId
});

type LOAD_PROJECT_DATA = {
  type: "LOAD_PROJECT_DATA",
  status?: Status,
  globalErrorMessage?: string,
  data?: Project
};

export const loadProjectData = (dispatch: Dispatch) =>
  fetch("/api/project_definition").then(
    response => {
      response.json().then(
        (project_definition: Project) =>
          dispatch({
            type: "LOAD_PROJECT_DATA",
            status: "success",
            data: project_definition
          }),
        () =>
          dispatch({
            type: "LOAD_PROJECT_DATA",
            status: "error",
            globalErrorMessage: "Cannot parse Project definition data"
          })
      );
    },
    () =>
      dispatch({
        type: "LOAD_PROJECT_DATA",
        status: "error",
        globalErrorMessage: "Cannot load Project definition data"
      })
  );

type LOAD_WORKFLOW_DATA = {
  type: "LOAD_WORKFLOW_DATA",
  status?: Status,
  globalErrorMessage?: string,
  data?: Workflow
};

export const loadWorkflowData = (dispatch: Dispatch) =>
  fetch("/api/workflow_definition").then(
    response => {
      response.json().then(
        (workflow_definition: Workflow) =>
          dispatch({
            type: "LOAD_WORKFLOW_DATA",
            status: "success",
            data: workflow_definition
          }),
        () =>
          dispatch({
            type: "LOAD_WORKFLOW_DATA",
            status: "error",
            globalErrorMessage: "Cannot parse Workflow definition data"
          })
      );
    },
    () =>
      dispatch({
        type: "LOAD_WORKFLOW_DATA",
        status: "error",
        globalErrorMessage: "Cannot load Workflow definition data"
      })
  );
