"""Tercen client wrapper.

Wraps the Python SDK's ``TercenClient`` with the credential-loading
boilerplate every entry point needs and a couple of project-/workflow-
listing helpers.

The Tercen Python SDK exposes a ``ProjectService`` and a
``WorkflowService``; we add bare-bones project-folder iteration on top
so the driver can scan one project for all its workflows in one call.
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Iterator, Optional

from tercen.client.factory import TercenClient


@dataclass
class TercenAuth:
    """Source of truth for the Tercen connection. Either a token or a
    username/password pair; populated from env vars by ``from_env``."""
    service_uri: str
    token: Optional[str]
    username: Optional[str]
    password: Optional[str]

    @classmethod
    def from_env(cls) -> "TercenAuth":
        uri = os.environ.get("TERCEN_URI")
        if not uri:
            raise RuntimeError(
                "TERCEN_URI is not set. Point it at the Tercen instance, "
                "e.g. https://pamgene.tercen.com:443"
            )
        token = os.environ.get("TERCEN_TOKEN")
        username = os.environ.get("TERCEN_USERNAME")
        password = os.environ.get("TERCEN_PASSWORD")
        if not token and not (username and password):
            raise RuntimeError(
                "Tercen credentials missing: set TERCEN_TOKEN, or "
                "TERCEN_USERNAME + TERCEN_PASSWORD."
            )
        return cls(uri, token, username, password)


def connect(auth: TercenAuth) -> TercenClient:
    """Build a connected ``TercenClient``. The SDK's ``userService``
    needs either a token (preferred — what tercenctl produces) or a
    username/password pair.

    The token path uses the SDK's session-reuse hook; without that, the
    SDK falls back to ``connect(username, password)`` which establishes
    a fresh session.
    """
    client = TercenClient(auth.service_uri)
    if auth.token:
        # The SDK exposes a session token via the userService — re-using
        # an existing token avoids triggering a password login and works
        # with the same `tercenctl context renew` token the operator
        # binaries use.
        client.userService.session = {"token": {"token": auth.token}}
    else:
        client.userService.connect(auth.username, auth.password)
    return client


def list_workflows_in_project(client: TercenClient, project_id: str) -> Iterator[object]:
    """Yield every workflow in the given project. The SDK exposes a
    ``WorkflowService`` with a ``findWorkflowByOwnerAndProject`` /
    ``findWorkflowByProject`` style finder; we use the project-scoped
    variant for simplicity.

    Implementation note: the exact finder name varies by SDK version.
    If this errors, swap to ``workflowService.findByProject`` or the
    paginated equivalent. The fallback path delegates to a raw
    ``projectService.get(project_id)`` and walks ``project.workflows``.
    """
    workflows = []
    # Preferred: SDK provides a finder.
    finder_names = [
        "findWorkflowByOwnerAndProject",
        "findWorkflowByProject",
        "findByProject",
    ]
    for name in finder_names:
        finder = getattr(client.workflowService, name, None)
        if finder is None:
            continue
        try:
            # Most SDK finders accept a single ``project_id`` and
            # return a list; pagination, if any, is internal.
            workflows = finder(project_id)
            break
        except TypeError:
            continue
    if not workflows:
        # Fallback: walk the project document. Slower but ubiquitous.
        proj = client.projectService.get(project_id)
        workflows = getattr(proj, "workflows", []) or []
    yield from workflows
