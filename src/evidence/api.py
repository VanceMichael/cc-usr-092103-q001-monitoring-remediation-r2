"""HTTP JSON API（仅标准库）。

鉴权约定（演示后台）：请求头 ``X-Actor`` 标识操作人、``X-Role`` 标识角色，
角色取值见 :class:`src.evidence.access.Role`。

所有写接口都要求 ``command_id`` 做幂等键；无论受理还是拒绝，
响应体都包含可核验 ``receipt``。
"""

from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse

from . import access
from .access import PermissionDenied
from .app import build_service
from .errors import (
    DomainError,
    DuplicateReport,
    NotFound,
    RectificationPerfunctory,
)
from .replay import build_replay
from .service import RemediationService
from .store import ChainTampered, ConcurrentModification

_SERVICE_LOCK = threading.Lock()


def _result_payload(result) -> dict:
    return {
        "status": "accepted",
        "events": [
            {"case_seq": e.case_seq, "type": e.event_type,
             "event_hash": e.event_hash, "occurred_at": e.occurred_at}
            for e in result.events
        ],
        "receipt": result.receipt,
    }


class ApiHandler(BaseHTTPRequestHandler):
    server_version = "EcoEvidence/1.0"

    # ---- 基础收发 ----

    def _send(self, code: int, body: dict) -> None:
        data = json.dumps(body, ensure_ascii=False, indent=2).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _read_json(self) -> dict:
        length = int(self.headers.get("Content-Length", "0"))
        if not length:
            return {}
        return json.loads(self.rfile.read(length).decode("utf-8"))

    def _identity(self, body: dict):
        actor = self.headers.get("X-Actor") or body.pop("actor", None)
        role = self.headers.get("X-Role") or body.pop("role", None)
        if not actor or not role:
            raise PermissionDenied("缺少 X-Actor / X-Role 请求头")
        return actor, access.Role(role)

    @property
    def svc(self) -> RemediationService:
        return self.server.svc  # type: ignore[attr-defined]

    def log_message(self, fmt, *args):  # noqa: A003
        pass

    # ---- 路由 ----

    def do_GET(self):  # noqa: N802
        try:
            path = urlparse(self.path).path.rstrip("/") or "/"
            if path == "/api/health":
                self._send(200, {"ok": True, "global_head": self.svc.store.global_head})
            elif path == "/api/registry/summary":
                self._send(200, {"summary": self.svc.registry.summary(),
                                 "digest": self.svc.registry.digest()})
            elif path.startswith("/api/cases/") and path.endswith("/replay"):
                case_id = path.split("/")[3]
                view = build_replay(self.svc.store, self.svc.registry, case_id)
                self._send(200, view.to_dict())
            elif path.startswith("/api/cases/") and path.endswith("/bundle"):
                case_id = path.split("/")[3]
                self._send(200, self.svc.store.export_case_bundle(case_id))
            elif path.startswith("/api/cases/"):
                case_id = path.split("/")[3]
                events = self.svc.store.case_events(case_id)
                self._send(200, {"case_id": case_id, "count": len(events),
                                 "case_head": self.svc.store.case_head(case_id),
                                 "events": [e.to_line() and json.loads(e.to_line())
                                            for e in events]})
            else:
                self._send(404, {"error": "not_found", "path": path})
        except KeyError as exc:
            self._send(404, {"error": "not_found", "detail": str(exc)})
        except ChainTampered as exc:
            self._send(409, {"error": "chain_tampered", "detail": str(exc)})
        except Exception as exc:  # noqa: BLE001
            self._send(500, {"error": "internal", "detail": str(exc)})

    def do_POST(self):  # noqa: N802
        try:
            body = self._read_json()
            path = urlparse(self.path).path.rstrip("/")
            self._route_post(path, body)
        except PermissionDenied as exc:
            self._send(403, {"error": "permission_denied", "detail": str(exc)})
        except (DuplicateReport, RectificationPerfunctory) as exc:
            self._send(409, {"error": type(exc).__name__, "detail": str(exc),
                             "receipt": getattr(exc, "receipt", None),
                             "missing": getattr(exc, "missing", None)})
        except ConcurrentModification as exc:
            self._send(412, {"error": "concurrent_modification", "detail": str(exc)})
        except NotFound as exc:
            self._send(404, {"error": "not_found", "detail": str(exc)})
        except DomainError as exc:
            self._send(422, {"error": "domain_rule", "detail": str(exc)})
        except ValueError as exc:
            self._send(400, {"error": "bad_request", "detail": str(exc)})
        except ChainTampered as exc:
            self._send(409, {"error": "chain_tampered", "detail": str(exc)})
        except Exception as exc:  # noqa: BLE001
            self._send(500, {"error": "internal", "detail": str(exc)})

    def _route_post(self, path: str, body: dict) -> None:
        parts = [p for p in path.split("/") if p]
        s = self.svc

        if path == "/api/receipts/verify":
            token = body["receipt_token"]
            receipt = s.receipts.decode(token)
            self._send(200, {"valid": s.receipts.verify(receipt), "receipt": receipt})
            return

        actor, role = self._identity(body)
        cid = body.get("command_id")
        if not cid and path != "/api/registry/import":
            raise ValueError("写接口必须提供 command_id 幂等键")

        if path == "/api/registry/import":
            entries = body["entries"]
            result = s.import_registry_bundle(entries, actor=actor)
            self._send(201, _result_payload(result)); return

        if path == "/api/registry/withdraw-qualification":
            result = s.withdraw_qualification(
                body["org_id"], reason=body["reason"], actor=actor, role=role,
                at=body.get("at"))
            self._send(201, _result_payload(result)); return

        if path == "/api/samples":
            result = s.register_sample(
                case_id=body["case_id"], sample_ref=body["sample_ref"],
                org_id=body["org_id"], person_id=body["person_id"],
                instrument_id=body["instrument_id"], sampled_at=body["sampled_at"],
                raw_data_ref=body["raw_data_ref"], finding=body.get("finding", ""),
                actor=actor, role=role, command_id=cid)
            self._send(201, _result_payload(result)); return

        # /api/cases/{id}/...
        if len(parts) >= 4 and parts[0] == "api" and parts[1] == "cases":
            case_id = parts[2]
            action = parts[3]
            if action == "self-check":
                result = s.report_self_check(
                    case_id, phase=body["phase"], content=body["content"],
                    reported_at=body.get("reported_at"), actor=actor, role=role,
                    command_id=cid)
            elif action == "late-entries":
                result = s.report_late_entry(
                    case_id, ref_event_hash=body["ref_event_hash"],
                    reason=body["reason"], facts=body.get("facts", {}),
                    actor=actor, role=role, command_id=cid)
            elif action == "corrections":
                result = s.correct_fact(
                    case_id, target_event_hash=body["target_event_hash"],
                    field_name=body["field_name"], new_value=body["new_value"],
                    reason=body["reason"], actor=actor, role=role, command_id=cid)
            elif action == "faults":
                result = s.classify_reporting_fault(
                    case_id, kind=body["kind"], detail=body["detail"],
                    basis_event_hash=body.get("basis_event_hash"),
                    actor=actor, role=role, command_id=cid)
            elif action == "orders" and len(parts) == 4:
                result = s.issue_rectification_order(
                    case_id, items=body["items"], actor=actor, role=role,
                    command_id=cid)
            elif action == "transfers" and len(parts) == 4:
                result = s.transfer_suspected_crime(
                    case_id, to_agency=body["to_agency"], basis=body["basis"],
                    actor=actor, role=role, command_id=cid)
            elif action == "sweep-overdue":
                results = s.sweep_overdue()
                self._send(200, {"status": "accepted",
                                 "marked": len(results),
                                 "receipts": [r.receipt for r in results]})
                return
            elif len(parts) == 6 and parts[5] == "submissions" and parts[3] == "orders":
                result = s.submit_rectification(
                    case_id, order_id=parts[4], measures=body["measures"],
                    actor=actor, role=role, command_id=cid)
            elif len(parts) == 6 and parts[5] == "verify" and parts[3] == "orders":
                result = s.verify_rectification(
                    case_id, order_id=parts[4], passed=bool(body["passed"]),
                    comment=body.get("comment", ""), reviewer=actor, role=role,
                    expected_case_hash=body.get("expected_case_hash"),
                    command_id=cid)
            elif len(parts) == 6 and parts[5] == "result" and parts[3] == "transfers":
                result = s.record_transfer_result(
                    case_id, transfer_id=parts[4], result=body["result"],
                    docket_no=body.get("docket_no", ""), note=body.get("note", ""),
                    actor=actor, role=role, command_id=cid)
            elif action == "close":
                result = s.close_case(case_id, actor=actor, role=role, command_id=cid)
            else:
                self._send(404, {"error": "not_found", "path": path}); return
            self._send(201, _result_payload(result))
            return

        self._send(404, {"error": "not_found", "path": path})


def serve(host: str = "127.0.0.1", port: int = 8080, *, store_path=None,
          registry_fixture=None, clock=None) -> ThreadingHTTPServer:
    from .clock import Clock
    server = ThreadingHTTPServer((host, port), ApiHandler)
    server.svc = build_service(store_path=store_path, registry_fixture=registry_fixture,
                               clock=clock or Clock())
    return server
