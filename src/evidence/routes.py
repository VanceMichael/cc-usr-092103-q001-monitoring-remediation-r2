"""HTTP 路由：把 REST 请求映射到 EvidenceService。

所有写接口支持 Idempotency-Key 请求头（或 body 内 idempotency_key）：
重复上报返回首次回执，链上只留一条事件。
"""

from __future__ import annotations

import json
import re
from http.server import BaseHTTPRequestHandler
from urllib.parse import parse_qs, urlparse

from .domain import DomainError
from .service import EvidenceService


def _first(query: dict, name: str):
    values = query.get(name)
    return values[0] if values else None


def _required(query: dict, name: str):
    value = _first(query, name)
    if value is None:
        raise DomainError(f"缺少查询参数: {name}")
    return value


class Router:
    def __init__(self, service: EvidenceService):
        self.service = service
        self._routes = self._build()

    def _build(self):
        s = self.service
        return [
            ("GET", r"/health", lambda m, q, b, h: (200, {"status": "ok"})),
            ("GET", r"/context", self.context_info),
            ("POST", r"/sampling-records", self.create_sampling),
            ("GET", r"/sampling-records/(?P<rid>[^/]+)",
             lambda m, q, b, h: (200, s.sampling_record(m["rid"]))),
            ("POST", r"/cases", self.create_case),
            ("GET", r"/cases/(?P<cid>[^/]+)",
             lambda m, q, b, h: (200, s.case_state(m["cid"]))),
            ("GET", r"/cases/(?P<cid>[^/]+)/timeline",
             lambda m, q, b, h: (200, s.timeline(m["cid"], at=_first(q, "at")))),
            ("GET", r"/cases/(?P<cid>[^/]+)/summary",
             lambda m, q, b, h: (200, s.summary(m["cid"]))),
            ("GET", r"/cases/(?P<cid>[^/]+)/replay",
             lambda m, q, b, h: (200, s.replay(m["cid"], at=_first(q, "at")))),
            ("GET", r"/cases/(?P<cid>[^/]+)/verify-chain",
             lambda m, q, b, h: (200, s.verify_chain(m["cid"]))),
            ("POST", r"/cases/(?P<cid>[^/]+)/self-checks", self.submit_self_check),
            ("POST", r"/cases/(?P<cid>[^/]+)/corrections", self.correct_record),
            ("POST", r"/cases/(?P<cid>[^/]+)/reviews", self.submit_review),
            ("POST", r"/cases/(?P<cid>[^/]+)/dispositions", self.issue_disposition),
            ("POST", r"/instruments/(?P<iid>[^/]+)/calibrations", self.record_calibration),
            ("POST", r"/institutions/(?P<iid>[^/]+)/qualification-exit",
             self.qualification_exit),
            ("GET", r"/reference/institutions/(?P<iid>[^/]+)/qualification",
             lambda m, q, b, h: (200, s.qualification_at(
                 m["iid"], _required(q, "at"),
                 knowledge_at=_first(q, "knowledge_at")))),
            ("GET", r"/reference/operators/(?P<oid>[^/]+)/authorization",
             lambda m, q, b, h: (200, s.authorization_at(
                 m["oid"], _required(q, "at"), scope=_first(q, "scope"),
                 knowledge_at=_first(q, "knowledge_at")))),
            ("GET", r"/reference/instruments/(?P<iid>[^/]+)/status",
             lambda m, q, b, h: (200, s.instrument_at(
                 m["iid"], _required(q, "at"),
                 knowledge_at=_first(q, "knowledge_at")))),
            ("GET", r"/receipts/(?P<rid>[^/]+)/verify",
             lambda m, q, b, h: (200, s.verify_receipt(m["rid"]))),
            ("GET", r"/verify-chain",
             lambda m, q, b, h: (200, s.verify_chain())),
            ("POST", r"/admin/check-overdue",
             lambda m, q, b, h: (200, s.check_overdue(now=b.get("now")))),
        ]

    @property
    def compiled(self):
        return [(m, re.compile(f"^{p}$"), h) for m, p, h in self._routes]

    # ------------------------------------------------------------ 处理器

    @staticmethod
    def _idem(headers: dict, body: dict):
        return body.get("idempotency_key") or headers.get("idempotency-key")

    def context_info(self, m, q, b, h):
        ref = self.service.ref
        return 200, {
            "generated_at": ref.generated_at,
            "institutions": sorted(ref.institutions),
            "operators": sorted(ref.operators),
            "instruments": sorted(ref.instruments),
            "stages": ref.stage_names(),
            "dispositions": sorted(ref.permission_matrix),
        }

    def create_sampling(self, m, q, b, h):
        return 201, self.service.record_sampling(
            institution_id=b["institution_id"], operator_id=b["operator_id"],
            instrument_id=b["instrument_id"], sampled_at=b["sampled_at"],
            scope=b["scope"], items=b.get("items", []),
            raw_data_hash=b["raw_data_hash"],
            actor=b.get("actor", "anonymous"), role=b.get("role", "机构人员"),
            record_id=b.get("record_id"), idempotency_key=self._idem(h, b))

    def create_case(self, m, q, b, h):
        return 201, self.service.open_case(
            sampling_record_id=b["sampling_record_id"], reason=b["reason"],
            actor=b.get("actor", "anonymous"), role=b.get("role", "执法员"),
            case_id=b.get("case_id"), idempotency_key=self._idem(h, b))

    def submit_self_check(self, m, q, b, h):
        return 201, self.service.submit_self_check(
            case_id=m["cid"], stage=b["stage"], occurred_at=b["occurred_at"],
            content=b.get("content", {}),
            actor=b.get("actor", "anonymous"), role=b.get("role", "机构人员"),
            is_backfill=bool(b.get("is_backfill")),
            backfill_reason=b.get("backfill_reason"),
            references=b.get("references"), idempotency_key=self._idem(h, b))

    def correct_record(self, m, q, b, h):
        return 201, self.service.correct_record(
            case_id=m["cid"], target_event_id=b["target_event_id"],
            reason=b["reason"], correction=b.get("correction", {}),
            actor=b.get("actor", "anonymous"), role=b.get("role", "机构人员"),
            idempotency_key=self._idem(h, b))

    def submit_review(self, m, q, b, h):
        return 201, self.service.submit_review(
            case_id=m["cid"], decision=b["decision"],
            expected_round=int(b["expected_round"]),
            actor=b.get("actor", "anonymous"), role=b.get("role", "复核员"),
            comments=b.get("comments", ""), idempotency_key=self._idem(h, b))

    def issue_disposition(self, m, q, b, h):
        known = {"type", "action", "reason", "actor", "role", "idempotency_key"}
        extra = {k: v for k, v in b.items() if k not in known}
        return 201, self.service.issue_disposition(
            case_id=m["cid"], disposition_type=b["type"],
            actor=b.get("actor", "anonymous"), role=b.get("role", "执法员"),
            reason=b.get("reason", ""), action=b.get("action"),
            idempotency_key=self._idem(h, b), **extra)

    def record_calibration(self, m, q, b, h):
        return 201, self.service.record_calibration(
            instrument_id=m["iid"], calibrated_at=b["calibrated_at"],
            valid_until=b["valid_until"], certificate_no=b["certificate_no"],
            actor=b.get("actor", "anonymous"), role=b.get("role", "机构人员"),
            idempotency_key=self._idem(h, b))

    def qualification_exit(self, m, q, b, h):
        return 201, self.service.qualification_exit(
            institution_id=m["iid"], qualification_id=b["qualification_id"],
            effective_at=b["effective_at"], legal_basis=b["legal_basis"],
            actor=b.get("actor", "anonymous"), role=b.get("role", "执法负责人"),
            idempotency_key=self._idem(h, b))

    # ------------------------------------------------------------ 分发

    def dispatch(self, method: str, raw_path: str, body: dict, headers: dict):
        parsed = urlparse(raw_path)
        query = parse_qs(parsed.query)
        for route_method, pattern, handler in self.compiled:
            if route_method != method:
                continue
            match = pattern.match(parsed.path)
            if match:
                return handler(match.groupdict(), query, body, headers)
        return 404, {"error": "not_found", "message": "路由不存在",
                     "details": {"path": parsed.path}}


def make_handler(service: EvidenceService):
    router = Router(service)

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def _handle(self, method: str):
            try:
                length = int(self.headers.get("Content-Length") or 0)
                body = json.loads(self.rfile.read(length) or b"{}") if length else {}
                headers = {k.lower(): v for k, v in self.headers.items()}
                status, payload = router.dispatch(method, self.path, body, headers)
            except DomainError as exc:
                status, payload = exc.status, exc.to_dict()
            except (KeyError, ValueError) as exc:
                status = 422
                payload = {"error": "validation_failed",
                           "message": f"请求参数不完整或不合法: {exc}",
                           "details": {}}
            except Exception as exc:  # noqa: BLE001 - 兜底，避免连接悬挂
                status, payload = 500, {"error": "internal_error",
                                        "message": str(exc), "details": {}}
            data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def do_GET(self):
            self._handle("GET")

        def do_POST(self):
            self._handle("POST")

        def log_message(self, *args):  # 静默访问日志
            pass

    return Handler
