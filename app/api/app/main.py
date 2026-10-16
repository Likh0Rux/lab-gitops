"""Focus API - la tua stanza dello spirito e del tempo.

Tracker di studio con pomodori, calendario pianificato dai tutor, classi e appunti allegati.

L'autenticazione la fa oauth2-proxy (login OIDC con Dex): qui arrivano solo richieste
di utenti già autenticati, con identità e gruppi negli header X-Forwarded-*.
L'API è raggiungibile SOLO da oauth2-proxy (NetworkPolicy), altrimenti chiunque
potrebbe falsificare questi header.
"""
import json
import os
import re
import time
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path

import redis
from fastapi import Depends, FastAPI, File, Header, HTTPException, Query, Request, Response, UploadFile
from fastapi.responses import FileResponse
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Histogram, generate_latest
from pydantic import BaseModel, Field
from sqlalchemy import (DateTime, ForeignKey, Integer, String, UniqueConstraint, create_engine, delete, func,
                        inspect, or_, select, text)
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column, sessionmaker

# ================================================================ configurazione
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+psycopg://{u}:{p}@{h}:5432/{d}".format(
        u=os.getenv("POSTGRES_USER", "focus"), p=os.getenv("POSTGRES_PASSWORD", ""),
        h=os.getenv("POSTGRES_HOST", "focus-db"), d=os.getenv("POSTGRES_DB", "focus")),
)
VALKEY_URL = os.getenv("VALKEY_URL", "redis://:{p}@{h}:6379/0".format(
    p=os.getenv("VALKEY_PASSWORD", ""), h=os.getenv("VALKEY_HOST", "focus-valkey")))
ATTACHMENTS_DIR = Path(os.getenv("ATTACHMENTS_DIR", "/data/attachments"))   # emptyDir oggi, volume Longhorn RWX domani
MAX_UPLOAD_MB = int(os.getenv("MAX_UPLOAD_MB", "10"))
ALLOWED_EXT = {".pdf", ".png", ".jpg", ".jpeg", ".txt", ".md"}
STATS_TTL = 30            # secondi di cache per le statistiche personali
CLASS_PREFIX = "classe-"  # i gruppi LDAP "classe-*" sono le classi

engine = create_engine(DATABASE_URL, pool_pre_ping=True, pool_size=3, max_overflow=2)
SessionLocal = sessionmaker(engine)
cache = redis.Redis.from_url(VALKEY_URL, decode_responses=True)

# ================================================================ metriche Prometheus
# Le label hanno valori limitati (niente username o materie): una label con valori
# liberi farebbe esplodere il numero di serie (problema di "cardinalità").
HTTP_REQUESTS = Counter("focus_http_requests_total", "Richieste HTTP", ["method", "route", "status"])
HTTP_LATENCY = Histogram("focus_http_request_duration_seconds", "Latenza delle richieste", ["route"])
POMODORI = Counter("focus_pomodori_completed_total", "Pomodori completati", ["origin"])   # libero | pianificato
POMODORI_MIN = Counter("focus_pomodori_minutes_total", "Minuti di studio registrati")
SCHEDULED = Counter("focus_schedule_items_created_total", "Voci di calendario create", ["target"])  # tutti | classe | studente
ATTACHMENTS = Counter("focus_attachments_uploaded_total", "Appunti caricati")
CACHE = Counter("focus_cache_requests_total", "Richieste alla cache delle statistiche", ["result"])  # hit | miss


# ================================================================ modelli
class Base(DeclarativeBase):
    pass


class Pomodoro(Base):
    __tablename__ = "pomodori"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    username: Mapped[str] = mapped_column(String(64), index=True)
    subject: Mapped[str] = mapped_column(String(64))
    minutes: Mapped[int] = mapped_column(Integer)
    completed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))


class AppUser(Base):
    """Utenti che hanno fatto almeno un accesso: servono al tutor per scegliere i destinatari."""
    __tablename__ = "utenti"
    username: Mapped[str] = mapped_column(String(64), primary_key=True)
    email: Mapped[str] = mapped_column(String(128), default="")
    role: Mapped[str] = mapped_column(String(16))
    classes: Mapped[str] = mapped_column(String(256), default="")      # es. "classe-a,classe-b"
    last_seen: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))


class Scheduled(Base):
    """Pomodoro pianificato da un tutor.
    Destinatario: assignee (uno studente) oppure target_class (una classe) oppure nessuno dei due = tutti."""
    __tablename__ = "pianificati"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    subject: Mapped[str] = mapped_column(String(64))
    start_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    minutes: Mapped[int] = mapped_column(Integer)
    note: Mapped[str] = mapped_column(String(200), default="")
    assignee: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    target_class: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    created_by: Mapped[str] = mapped_column(String(64))


class ScheduledDone(Base):
    """Completamento di un pomodoro pianificato da parte di uno studente."""
    __tablename__ = "pianificati_completati"
    __table_args__ = (UniqueConstraint("schedule_id", "username"),)
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    schedule_id: Mapped[int] = mapped_column(ForeignKey("pianificati.id", ondelete="CASCADE"), index=True)
    username: Mapped[str] = mapped_column(String(64))
    completed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))


class Attachment(Base):
    """Appunti allegati a una voce del calendario. Il file sta su disco, qui solo i metadati."""
    __tablename__ = "allegati"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    schedule_id: Mapped[int] = mapped_column(ForeignKey("pianificati.id", ondelete="CASCADE"), index=True)
    filename: Mapped[str] = mapped_column(String(128))            # nome mostrato all'utente
    stored_name: Mapped[str] = mapped_column(String(64))          # nome su disco (uuid): niente path traversal
    content_type: Mapped[str] = mapped_column(String(64))
    size: Mapped[int] = mapped_column(Integer)
    uploaded_by: Mapped[str] = mapped_column(String(64))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))


def migrate():
    """Crea le tabelle mancanti e aggiunge le colonne nuove a quelle esistenti (schema evolutivo minimo)."""
    Base.metadata.create_all(engine)
    insp = inspect(engine)
    with engine.begin() as c:
        if "target_class" not in {x["name"] for x in insp.get_columns("pianificati")}:
            c.execute(text("ALTER TABLE pianificati ADD COLUMN target_class VARCHAR(64)"))
        if "classes" not in {x["name"] for x in insp.get_columns("utenti")}:
            c.execute(text("ALTER TABLE utenti ADD COLUMN classes VARCHAR(256) DEFAULT ''"))


@asynccontextmanager
async def lifespan(_app):
    migrate()
    ATTACHMENTS_DIR.mkdir(parents=True, exist_ok=True)
    yield


app = FastAPI(title="Focus API", description="La tua stanza dello spirito e del tempo", lifespan=lifespan)


@app.middleware("http")
async def metrics_middleware(request: Request, call_next):
    start = time.perf_counter()
    response = await call_next(request)
    route = getattr(request.scope.get("route"), "path", "non_trovata")   # template della rotta, non l'URL reale
    if route != "/metrics":
        HTTP_REQUESTS.labels(request.method, route, str(response.status_code)).inc()
        HTTP_LATENCY.labels(route).observe(time.perf_counter() - start)
    return response


def get_db():
    with SessionLocal() as db:
        yield db


# ================================================================ identità, ruoli e classi
class User(BaseModel):
    username: str
    email: str
    groups: list[str]
    role: str
    classes: list[str]


def role_from_groups(groups: list[str]) -> str:
    """RBAC: il ruolo nell'app deriva dai gruppi LDAP arrivati nel token."""
    if "platform-admin" in groups:
        return "admin"
    if "focus-tutor" in groups:
        return "tutor"
    return "studente"


def current_user(
    x_forwarded_preferred_username: str | None = Header(default=None),
    x_forwarded_user: str | None = Header(default=None),
    x_forwarded_email: str | None = Header(default=None),
    x_forwarded_groups: list[str] | None = Header(default=None),
) -> User:
    username = x_forwarded_preferred_username or x_forwarded_user
    if not username:
        raise HTTPException(401, "utente non autenticato")
    groups = sorted({g.strip() for h in (x_forwarded_groups or []) for g in h.split(",") if g.strip()})
    return User(username=username, email=x_forwarded_email or "", groups=groups,
                role=role_from_groups(groups), classes=[g for g in groups if g.startswith(CLASS_PREFIX)])


def require_tutor(user: User = Depends(current_user)) -> User:
    if user.role not in ("tutor", "admin"):
        raise HTTPException(403, "riservato a tutor e amministratori")
    return user


def unrestricted(user: User) -> bool:
    """L'admin vede tutto; un tutor senza classi assegnate gestisce tutti gli studenti."""
    return user.role == "admin" or not user.classes


def students_in_scope(user: User, db: Session) -> list[AppUser]:
    rows = db.scalars(select(AppUser).where(AppUser.role == "studente").order_by(AppUser.username)).all()
    if unrestricted(user):
        return list(rows)
    mine = set(user.classes)
    return [u for u in rows if mine & set(filter(None, u.classes.split(",")))]


def can_manage(user: User, item: Scheduled, db: Session) -> bool:
    """Un tutor modifica solo voci delle sue classi o dei suoi studenti."""
    if unrestricted(user):
        return True
    if item.target_class:
        return item.target_class in user.classes
    if item.assignee:
        return item.assignee in {u.username for u in students_in_scope(user, db)}
    return False   # le voci "per tutti" le gestisce solo l'admin (o un tutor senza classi)


def can_see(user: User, item: Scheduled, db: Session) -> bool:
    if user.role == "studente":
        return (item.assignee == user.username or
                (item.assignee is None and (item.target_class is None or item.target_class in user.classes)))
    return can_manage(user, item, db)


def week_key(classe: str | None = None) -> str:
    y, w, _ = datetime.now(timezone.utc).isocalendar()
    return f"leaderboard:{y}-W{w:02d}" + (f":{classe}" if classe else "")


def week_start() -> datetime:
    now = datetime.now(timezone.utc)
    return (now - timedelta(days=now.weekday())).replace(hour=0, minute=0, second=0, microsecond=0)


def as_utc(d: datetime) -> datetime:
    return d.replace(tzinfo=timezone.utc) if d.tzinfo is None else d.astimezone(timezone.utc)


# ================================================================ profilo e pomodori
class NewPomodoro(BaseModel):
    subject: str = Field(min_length=1, max_length=64)
    minutes: int = Field(ge=1, le=120)
    schedule_id: int | None = None          # se il pomodoro nasce da una voce del calendario


@app.get("/api/me")
def me(user: User = Depends(current_user), db: Session = Depends(get_db)) -> User:
    db.merge(AppUser(username=user.username, email=user.email, role=user.role,
                     classes=",".join(user.classes), last_seen=datetime.now(timezone.utc)))
    db.commit()                               # così il tutor vede lo studente tra i destinatari
    return user


@app.post("/api/pomodori", status_code=201)
def add_pomodoro(body: NewPomodoro, user: User = Depends(current_user), db: Session = Depends(get_db)):
    db.add(Pomodoro(username=user.username, subject=body.subject.strip(), minutes=body.minutes))
    origin = "libero"
    if body.schedule_id is not None:
        item = db.get(Scheduled, body.schedule_id)
        already = db.scalar(select(ScheduledDone.id).where(
            ScheduledDone.schedule_id == body.schedule_id, ScheduledDone.username == user.username))
        if item and can_see(user, item, db) and not already:
            db.add(ScheduledDone(schedule_id=item.id, username=user.username))
            origin = "pianificato"
    db.commit()
    for key in [week_key()] + [week_key(c) for c in user.classes]:   # classifica generale e di classe
        cache.zincrby(key, body.minutes, user.username)
        cache.expire(key, 60 * 60 * 24 * 14)
    cache.delete(f"stats:{user.username}")                            # invalida la cache delle statistiche
    POMODORI.labels(origin).inc()
    POMODORI_MIN.inc(body.minutes)
    return {"ok": True}


@app.get("/api/me/stats")
def my_stats(user: User = Depends(current_user), db: Session = Depends(get_db)):
    key = f"stats:{user.username}"
    if (hit := cache.get(key)) is not None:
        CACHE.labels("hit").inc()
        return {"cached": True, **json.loads(hit)}
    CACHE.labels("miss").inc()
    rows = db.execute(
        select(Pomodoro.subject, func.count(), func.sum(Pomodoro.minutes))
        .where(Pomodoro.username == user.username, Pomodoro.completed_at >= week_start())
        .group_by(Pomodoro.subject)
    ).all()
    data = {
        "week_minutes": int(sum(r[2] for r in rows)),
        "week_pomodori": int(sum(r[1] for r in rows)),
        "by_subject": [{"subject": r[0], "pomodori": int(r[1]), "minutes": int(r[2])} for r in rows],
    }
    cache.setex(key, STATS_TTL, json.dumps(data))
    return {"cached": False, **data}


@app.get("/api/leaderboard")
def leaderboard(classe: str | None = Query(default=None), user: User = Depends(current_user)):
    if classe and user.role == "studente" and classe not in user.classes:
        raise HTTPException(403, "puoi vedere solo la classifica della tua classe")
    top = cache.zrevrange(week_key(classe), 0, 9, withscores=True)
    return [{"username": u, "minutes": int(m)} for u, m in top]


@app.get("/api/tutor/overview")
def tutor_overview(user: User = Depends(require_tutor), db: Session = Depends(get_db)):
    q = (select(Pomodoro.username, func.count(), func.sum(Pomodoro.minutes))
         .where(Pomodoro.completed_at >= week_start())
         .group_by(Pomodoro.username).order_by(func.sum(Pomodoro.minutes).desc()))
    if not unrestricted(user):
        q = q.where(Pomodoro.username.in_([u.username for u in students_in_scope(user, db)]))
    return [{"username": r[0], "pomodori": int(r[1]), "minutes": int(r[2])} for r in db.execute(q).all()]


# ================================================================ classi e studenti
@app.get("/api/classes")
def classes(user: User = Depends(require_tutor), db: Session = Depends(get_db)):
    if not unrestricted(user):
        return user.classes
    found = set()
    for row in db.scalars(select(AppUser.classes)).all():
        found.update(filter(None, (row or "").split(",")))
    return sorted(found)


@app.get("/api/students")
def students(user: User = Depends(require_tutor), db: Session = Depends(get_db)):
    return [{"username": u.username, "email": u.email, "classes": list(filter(None, u.classes.split(",")))}
            for u in students_in_scope(user, db)]


# ================================================================ calendario
class ScheduleIn(BaseModel):
    subject: str = Field(min_length=1, max_length=64)
    start_at: datetime
    minutes: int = Field(ge=1, le=120)
    note: str = Field(default="", max_length=200)


class ScheduleCreate(ScheduleIn):
    target: str = Field(default="all", pattern="^(all|class|students)$")   # tutti | una classe | studenti scelti
    target_class: str | None = None
    assignees: list[str] = []


class ScheduleUpdate(ScheduleIn):
    target_class: str | None = None
    assignee: str | None = None              # entrambi None = tutti


def attachments_of(db: Session, ids: list[int]) -> dict[int, list[dict]]:
    out: dict[int, list[dict]] = {}
    if ids:
        for a in db.scalars(select(Attachment).where(Attachment.schedule_id.in_(ids)).order_by(Attachment.id)).all():
            out.setdefault(a.schedule_id, []).append({"id": a.id, "filename": a.filename, "size": a.size})
    return out


def serialize(item: Scheduled, done_by: list[str], files: list[dict]) -> dict:
    return {"id": item.id, "subject": item.subject, "start_at": as_utc(item.start_at).isoformat(),
            "minutes": item.minutes, "note": item.note, "assignee": item.assignee,
            "target_class": item.target_class, "created_by": item.created_by,
            "done_by": done_by, "attachments": files}


@app.get("/api/schedule")
def get_schedule(start: datetime | None = Query(default=None), end: datetime | None = Query(default=None),
                 user: User = Depends(current_user), db: Session = Depends(get_db)):
    start = as_utc(start) if start else week_start()
    end = as_utc(end) if end else start + timedelta(days=7)
    q = select(Scheduled).where(Scheduled.start_at >= start, Scheduled.start_at < end).order_by(Scheduled.start_at)
    if user.role == "studente":               # filtro direttamente nella query
        q = q.where(or_(Scheduled.assignee == user.username,
                        Scheduled.assignee.is_(None) & (Scheduled.target_class.is_(None) |
                                                       Scheduled.target_class.in_(user.classes or [""]))))
    items = [i for i in db.scalars(q).all() if can_see(user, i, db)]
    ids = [i.id for i in items]
    done: dict[int, list[str]] = {}
    if ids:
        for sid, uname in db.execute(select(ScheduledDone.schedule_id, ScheduledDone.username)
                                     .where(ScheduledDone.schedule_id.in_(ids))).all():
            done.setdefault(sid, []).append(uname)
    files = attachments_of(db, ids)
    out = []
    for i in items:
        who = done.get(i.id, [])
        if user.role == "studente":
            who = [u for u in who if u == user.username]   # lo studente non vede chi altro ha completato
        out.append(serialize(i, who, files.get(i.id, [])))
    return out


@app.post("/api/schedule", status_code=201)
def create_schedule(body: ScheduleCreate, user: User = Depends(require_tutor), db: Session = Depends(get_db)):
    scope = {u.username for u in students_in_scope(user, db)}
    if body.target == "all":
        if not unrestricted(user):
            raise HTTPException(403, "puoi pianificare solo per le tue classi o i tuoi studenti")
        targets = [(None, None)]
    elif body.target == "class":
        if not body.target_class or (not unrestricted(user) and body.target_class not in user.classes):
            raise HTTPException(403, "classe non valida o non tua")
        targets = [(None, body.target_class)]
    else:
        chosen = sorted({a.strip() for a in body.assignees if a.strip()})
        if not chosen or not set(chosen) <= scope:
            raise HTTPException(403, "scegli studenti delle tue classi")
        targets = [(a, None) for a in chosen]       # una voce per studente: si modificano singolarmente
    created = []
    for assignee, klass in targets:
        item = Scheduled(subject=body.subject.strip(), start_at=as_utc(body.start_at), minutes=body.minutes,
                         note=body.note.strip(), assignee=assignee, target_class=klass, created_by=user.username)
        db.add(item)
        created.append(item)
    db.commit()
    SCHEDULED.labels({"all": "tutti", "class": "classe", "students": "studente"}[body.target]).inc(len(created))
    return [serialize(i, [], []) for i in created]


@app.put("/api/schedule/{item_id}")
def update_schedule(item_id: int, body: ScheduleUpdate, user: User = Depends(require_tutor), db: Session = Depends(get_db)):
    item = db.get(Scheduled, item_id)
    if not item:
        raise HTTPException(404, "pomodoro pianificato non trovato")
    if not can_manage(user, item, db):
        raise HTTPException(403, "voce di un'altra classe")
    item.subject, item.start_at, item.minutes = body.subject.strip(), as_utc(body.start_at), body.minutes
    item.note, item.assignee, item.target_class = body.note.strip(), body.assignee or None, body.target_class or None
    if not can_manage(user, item, db):           # la nuova destinazione deve restare nel suo perimetro
        db.rollback()
        raise HTTPException(403, "destinatario fuori dalle tue classi")
    db.commit()
    return serialize(item, [], attachments_of(db, [item.id]).get(item.id, []))


@app.delete("/api/schedule/{item_id}", status_code=204)
def delete_schedule(item_id: int, user: User = Depends(require_tutor), db: Session = Depends(get_db)):
    item = db.get(Scheduled, item_id)
    if not item:
        raise HTTPException(404, "pomodoro pianificato non trovato")
    if not can_manage(user, item, db):
        raise HTTPException(403, "voce di un'altra classe")
    for a in db.scalars(select(Attachment).where(Attachment.schedule_id == item_id)).all():
        (ATTACHMENTS_DIR / a.stored_name).unlink(missing_ok=True)
    db.execute(delete(Attachment).where(Attachment.schedule_id == item_id))
    db.execute(delete(ScheduledDone).where(ScheduledDone.schedule_id == item_id))
    db.delete(item)
    db.commit()


# ================================================================ appunti allegati
@app.post("/api/schedule/{item_id}/attachments", status_code=201)
async def upload_attachment(item_id: int, file: UploadFile = File(...),
                            user: User = Depends(require_tutor), db: Session = Depends(get_db)):
    item = db.get(Scheduled, item_id)
    if not item or not can_manage(user, item, db):
        raise HTTPException(404, "voce non trovata")
    original = Path(file.filename or "appunti").name                      # niente percorsi dal client
    ext = Path(original).suffix.lower()
    if ext not in ALLOWED_EXT:
        raise HTTPException(415, "formato non ammesso (pdf, png, jpg, txt, md)")
    data = await file.read(MAX_UPLOAD_MB * 1024 * 1024 + 1)
    if len(data) > MAX_UPLOAD_MB * 1024 * 1024:
        raise HTTPException(413, f"file troppo grande (max {MAX_UPLOAD_MB} MB)")
    stored = uuid.uuid4().hex + ext                                       # nome su disco scelto dal server
    (ATTACHMENTS_DIR / stored).write_bytes(data)
    safe_name = re.sub(r"[^\w.\- ]", "_", original)[:128]
    att = Attachment(schedule_id=item.id, filename=safe_name, stored_name=stored,
                     content_type=file.content_type or "application/octet-stream", size=len(data),
                     uploaded_by=user.username)
    db.add(att)
    db.commit()
    ATTACHMENTS.inc()
    return {"id": att.id, "filename": att.filename, "size": att.size}


@app.get("/api/attachments/{att_id}")
def download_attachment(att_id: int, user: User = Depends(current_user), db: Session = Depends(get_db)):
    att = db.get(Attachment, att_id)
    item = db.get(Scheduled, att.schedule_id) if att else None
    if not att or not item or not can_see(user, item, db):                # stesso controllo del calendario
        raise HTTPException(404, "allegato non trovato")
    path = ATTACHMENTS_DIR / att.stored_name
    if not path.exists():
        raise HTTPException(410, "file non più disponibile sul volume")
    return FileResponse(path, filename=att.filename, media_type=att.content_type,
                        headers={"X-Content-Type-Options": "nosniff"})


@app.delete("/api/attachments/{att_id}", status_code=204)
def delete_attachment(att_id: int, user: User = Depends(require_tutor), db: Session = Depends(get_db)):
    att = db.get(Attachment, att_id)
    item = db.get(Scheduled, att.schedule_id) if att else None
    if not att or not item or not can_manage(user, item, db):
        raise HTTPException(404, "allegato non trovato")
    (ATTACHMENTS_DIR / att.stored_name).unlink(missing_ok=True)
    db.delete(att)
    db.commit()


# ================================================================ salute e metriche
@app.get("/healthz")
def healthz():
    return {"status": "ok"}


@app.get("/readyz")
def readyz(db: Session = Depends(get_db)):
    db.execute(text("SELECT 1"))
    cache.ping()
    return {"status": "ready"}


@app.get("/metrics")
def metrics():
    """Fuori da /api: oauth2-proxy non lo espone, lo legge solo il monitoring dentro il cluster."""
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
