"""
EdTech Pipeline — Stateful Data Generator v2
=============================================
Simulates realistic EdTech platform events with full state-machine tracking.

Key improvements over v1:
  - Users follow strict state transitions: offline→online→in_class→in_session
  - Classes have lifecycles; students are properly evicted when a class ends
  - Session_end only fires if session_start already happened for that user
  - Logout only when user is online but NOT inside a class
  - Rich CC_Attributes schema (user_role, subscription_type, os, course_name, etc.)
  - Realistic proportions: ~30% offline, ~20% browsing, ~50% in live classes

Usage examples
--------------
# Normal load (10 events/sec, all event types)
python generator.py

# Spike simulation
python generator.py --mode spike --spike-rate 50 --spike-duration 60

# Only login/logout events
python generator.py --events login logout

# Batch mode: generate a file instead of sending to Kinesis
python generator.py --mode file --output events.json --count 10000

# Dry run: print events to console without sending
python generator.py --dry-run --rate 5
"""

import argparse
import json
import random
import time
import threading
import uuid
import boto3
import logging
from datetime import datetime, timezone
from botocore.exceptions import ClientError

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("generator")

# ── Static reference data ─────────────────────────────────────────────────────
REGIONS = ["Hyderabad", "Mumbai", "Delhi", "Bengaluru", "Chennai", "Pune", "Kolkata"]

DEVICES = ["mobile", "desktop", "tablet"]
OS_BY_DEVICE = {
    "mobile":  ["android", "ios"],
    "desktop": ["windows", "mac", "linux"],
    "tablet":  ["android", "ios", "windows"],
}
APP_VERSIONS = ["v2.1.0", "v2.0.5", "v1.9.3", "v1.8.7"]

COURSES = [
    {"class_id": "csg527", "course_name": "Advanced Algorithms",        "course_category": "CS",           "difficulty_level": "hard"},
    {"class_id": "csg511", "course_name": "Machine Learning",           "course_category": "Data Science", "difficulty_level": "medium"},
    {"class_id": "csg415", "course_name": "Linear Algebra",             "course_category": "Math",         "difficulty_level": "medium"},
    {"class_id": "csg302", "course_name": "Python for Beginners",       "course_category": "CS",           "difficulty_level": "easy"},
    {"class_id": "csg221", "course_name": "Data Structures",            "course_category": "CS",           "difficulty_level": "medium"},
    {"class_id": "csg610", "course_name": "Deep Learning",              "course_category": "Data Science", "difficulty_level": "hard"},
    {"class_id": "csg499", "course_name": "Statistics & Probability",   "course_category": "Math",         "difficulty_level": "medium"},
    {"class_id": "csg388", "course_name": "Database Systems",           "course_category": "CS",           "difficulty_level": "medium"},
    {"class_id": "csg175", "course_name": "Cloud Computing",            "course_category": "CS",           "difficulty_level": "hard"},
    {"class_id": "csg244", "course_name": "Web Development",            "course_category": "CS",           "difficulty_level": "easy"},
]

INTERACTION_TYPES  = ["video", "poll", "quiz", "chat"]
AGE_GROUPS         = ["18-22", "23-30", "31-40", "41+"]
USER_LEVELS        = ["beginner", "intermediate", "advanced"]
SUBSCRIPTION_TYPES = ["free", "premium"]

# ── Simulation constants ──────────────────────────────────────────────────────
USER_POOL_SIZE       = 20000
INSTRUCTOR_COUNT     = 200

# A class lives for 60–180 *real* seconds.
# With TIMESCALE=60 that represents 60–180 simulated minutes — realistic lecture durations.
CLASS_TIMESCALE      = 60          # sim-seconds per real-second (for durations)
CLASS_MIN_DURATION   = 60          # real seconds a class stays open
CLASS_MAX_DURATION   = 180
MAX_CONCURRENT_CLS   = 6           # how many classes run simultaneously

# ── User states ───────────────────────────────────────────────────────────────
ST_OFFLINE     = "offline"
ST_ONLINE      = "online"       # logged in, not in any class
ST_IN_CLASS    = "in_class"     # joined a class, no active session interaction
ST_IN_SESSION  = "in_session"   # active session/content interaction within a class

ALL_EVENT_TYPES = ["login", "logout", "class_join", "class_leave",
                   "session_start", "session_end"]


# ══════════════════════════════════════════════════════════════════════════════
#  Data model
# ══════════════════════════════════════════════════════════════════════════════

class UserProfile:
    """
    Fixed attributes assigned once when the user pool is built.
    In a real system these come from a user database; here we assign them
    consistently per user_id so the same user always has the same role/device/etc.
    """
    __slots__ = (
        "user_id", "user_role", "subscription_type", "user_age_group",
        "user_level", "region", "country", "timezone", "device", "os",
        "app_version",
    )

    def __init__(self, user_id: str, seed: int):
        rng = random.Random(seed)   # deterministic per user
        self.user_id          = user_id
        idx                   = int(user_id[1:])
        self.user_role        = "instructor" if idx <= INSTRUCTOR_COUNT else "student"
        self.subscription_type = rng.choices(
            SUBSCRIPTION_TYPES, weights=[0.60, 0.40]
        )[0]
        self.user_age_group   = rng.choice(AGE_GROUPS)
        self.user_level       = rng.choice(USER_LEVELS)
        self.region           = rng.choice(REGIONS)
        self.country          = "India"
        self.timezone         = "Asia/Kolkata"
        self.device           = rng.choice(DEVICES)
        self.os               = rng.choice(OS_BY_DEVICE[self.device])
        self.app_version      = rng.choice(APP_VERSIONS)


class UserState:
    """Mutable runtime state for one user — updated on every event."""
    __slots__ = (
        "status", "session_id", "current_class_id",
        "login_at", "class_join_at", "session_start_at",
    )

    def __init__(self):
        self.status:          str         = ST_OFFLINE
        self.session_id:      str | None  = None   # login-scoped session
        self.current_class_id: str | None = None
        self.login_at:        float | None = None
        self.class_join_at:   float | None = None
        self.session_start_at: float | None = None


class LiveClass:
    """A running class with a roster of enrolled students."""
    __slots__ = (
        "class_id", "course_name", "course_category", "difficulty_level",
        "instructor_id", "session_type", "expires_at", "students",
    )

    def __init__(self, course: dict, instructor_id: str, expires_at: float):
        self.class_id         = course["class_id"]
        self.course_name      = course["course_name"]
        self.course_category  = course["course_category"]
        self.difficulty_level = course["difficulty_level"]
        self.instructor_id    = instructor_id
        self.session_type     = random.choice(["live", "recorded"])
        self.expires_at       = expires_at
        self.students: set[str] = set()


# ══════════════════════════════════════════════════════════════════════════════
#  Stateful generator
# ══════════════════════════════════════════════════════════════════════════════

class StatefulGenerator:
    """
    Maintains the full simulation state and emits events that are always
    consistent with each user's current lifecycle state.

    State machine (per user):
        offline ──login──► online ──class_join──► in_class ──session_start──► in_session
           ▲                  │                      │   ◄──session_end──────────────┘
           └──logout──────────┘       class_leave ───┘
                                       (→ online; user may then join another or logout)

    Class lifecycle:
        Created → students join → class expires → all students evicted (class_leave)
        → each evicted student independently decides: join another class OR logout
    """

    def __init__(self, args):
        self.args = args

        # Build stable user pool
        self.profiles: dict[str, UserProfile] = {}
        self.states:   dict[str, UserState]   = {}
        for i in range(1, USER_POOL_SIZE + 1):
            uid = f"u{i:03d}"
            self.profiles[uid] = UserProfile(uid, seed=i * 7919)
            self.states[uid]   = UserState()

        # Active classes
        self.classes:      dict[str, LiveClass] = {}
        self.classes_lock: threading.Lock       = threading.Lock()

        # Pending forced events from class expiry (FIFO)
        self._pending: list[dict] = []

        # Counters
        self.sent   = 0
        self.failed = 0

        # Warm up: open some classes, bring some users online/in-class
        self._replenish_classes()
        self._warmup()

    # ── Class lifecycle ────────────────────────────────────────────────────────

    def _replenish_classes(self):
        with self.classes_lock:
            while len(self.classes) < MAX_CONCURRENT_CLS:
                course = random.choice(COURSES)
                instructor_id = f"i{random.randint(1, INSTRUCTOR_COUNT):02d}"
                expires_at = time.time() + random.uniform(CLASS_MIN_DURATION, CLASS_MAX_DURATION)
                cls = LiveClass(course, instructor_id, expires_at)
                self.classes[cls.class_id] = cls

    def _check_expired_classes(self):
        """
        Detect expired classes and queue class_leave (and session_end if needed)
        events for every student still inside.  Called once per tick.
        """
        now = time.time()
        expired: list[LiveClass] = []

        with self.classes_lock:
            expired = [c for c in self.classes.values() if now >= c.expires_at]
            for cls in expired:
                del self.classes[cls.class_id]

        for cls in expired:
            log.info("Class %s (%s) ended — evicting %d students",
                     cls.class_id, cls.course_name, len(cls.students))
            for uid in list(cls.students):
                st = self.states[uid]
                if st.status == ST_IN_SESSION:
                    # End the session first (interrupted — class closed under them)
                    self._pending.append(
                        self._build_session_end(uid, st, cls, "interrupted")
                    )
                    st.status         = ST_IN_CLASS
                    st.session_start_at = None

                if st.status == ST_IN_CLASS:
                    self._pending.append(
                        self._build_class_leave(uid, st, cls, "class_ended")
                    )
                    st.status          = ST_ONLINE
                    st.current_class_id = None
                    st.class_join_at   = None

        if expired:
            self._replenish_classes()

    # ── Warmup ─────────────────────────────────────────────────────────────────

    def _warmup(self):
        """
        Pre-populate some realistic state so the first events don't all look
        like a mass-login.  Silently sets state without emitting events.
        """
        user_ids = list(self.profiles.keys())
        random.shuffle(user_ids)

        cls_list = list(self.classes.values())

        for uid in user_ids[:120]:          # 60 % start online / in-class
            st = self.states[uid]
            st.status     = ST_ONLINE
            st.session_id = f"s{random.randint(10000, 99999)}"
            st.login_at   = time.time() - random.uniform(30, 300)

        for uid in user_ids[20:90]:         # ~35 % already in a class
            if not cls_list:
                break
            st  = self.states[uid]
            cls = random.choice(cls_list)
            st.status           = ST_IN_CLASS
            st.current_class_id = cls.class_id
            st.class_join_at    = time.time() - random.uniform(10, 120)
            cls.students.add(uid)

        for uid in user_ids[20:60]:         # ~20 % already in a session
            st = self.states[uid]
            if st.status == ST_IN_CLASS:
                st.status          = ST_IN_SESSION
                st.session_start_at = time.time() - random.uniform(5, 60)

    # ── Event builders ─────────────────────────────────────────────────────────

    def _now_iso(self) -> str:
        return datetime.now(timezone.utc).isoformat()

    def _base(self, uid: str, event_type: str) -> dict:
        p = self.profiles[uid]
        return {
            "event_id":         str(uuid.uuid4()),
            "user_id":          uid,
            "event_type":       event_type,
            "timestamp":        self._now_iso(),
            "user_role":        p.user_role,
            "subscription_type": p.subscription_type,
            "user_age_group":   p.user_age_group,
            "user_level":       p.user_level,
            "region":           p.region,
            "country":          p.country,
            "timezone":         p.timezone,
            "device":           p.device,
            "os":               p.os,
            "app_version":      p.app_version,
            "schema_version":   "2.0",
        }

    def _build_login(self, uid: str, st: UserState) -> dict:
        st.session_id = f"s{random.randint(10000, 99999)}"
        st.login_at   = time.time()
        ev = self._base(uid, "login")
        ev["session_id"] = st.session_id
        return ev

    def _build_logout(self, uid: str, st: UserState) -> dict:
        ev = self._base(uid, "logout")
        ev["session_id"] = st.session_id
        if st.login_at:
            ev["active_duration_sec"] = round(
                (time.time() - st.login_at) * CLASS_TIMESCALE
            )
        return ev

    def _build_class_join(self, uid: str, st: UserState, cls: LiveClass) -> dict:
        st.current_class_id = cls.class_id
        st.class_join_at    = time.time()
        cls.students.add(uid)
        ev = self._base(uid, "class_join")
        ev.update({
            "session_id":       st.session_id,
            "class_id":         cls.class_id,
            "course_name":      cls.course_name,
            "course_category":  cls.course_category,
            "difficulty_level": cls.difficulty_level,
            "instructor_id":    cls.instructor_id,
            "session_type":     cls.session_type,
        })
        return ev

    def _build_class_leave(self, uid: str, st: UserState,
                           cls: LiveClass, reason: str = "voluntary") -> dict:
        cls.students.discard(uid)
        ev = self._base(uid, "class_leave")
        ev.update({
            "session_id":       st.session_id,
            "class_id":         cls.class_id,
            "course_name":      cls.course_name,
            "course_category":  cls.course_category,
            "difficulty_level": cls.difficulty_level,
            "instructor_id":    cls.instructor_id,
            "leave_reason":     reason,
        })
        if st.class_join_at:
            ev["class_duration_sec"] = round(
                (time.time() - st.class_join_at) * CLASS_TIMESCALE
            )
        return ev

    def _build_session_start(self, uid: str, st: UserState, cls: LiveClass) -> dict:
        st.session_start_at = time.time()
        interaction = random.choice(INTERACTION_TYPES)
        ev = self._base(uid, "session_start")
        ev.update({
            "session_id":       st.session_id,
            "class_id":         cls.class_id,
            "course_name":      cls.course_name,
            "course_category":  cls.course_category,
            "difficulty_level": cls.difficulty_level,
            "instructor_id":    cls.instructor_id,
            "session_type":     cls.session_type,
            "interaction_type": interaction,
            "content_id":       f"cnt{random.randint(100, 999)}",
            "content_duration": random.randint(300, 3600),
            "session_status":   "started",
        })
        return ev

    def _build_session_end(self, uid: str, st: UserState,
                           cls: LiveClass, status: str = "completed") -> dict:
        ev = self._base(uid, "session_end")
        ev.update({
            "session_id":       st.session_id,
            "class_id":         cls.class_id,
            "course_name":      cls.course_name,
            "course_category":  cls.course_category,
            "difficulty_level": cls.difficulty_level,
            "instructor_id":    cls.instructor_id,
            "session_type":     cls.session_type,
            "session_status":   status,
        })
        if st.session_start_at:
            ev["session_duration_sec"] = round(
                (time.time() - st.session_start_at) * CLASS_TIMESCALE
            )
        return ev

    # ── Core tick ──────────────────────────────────────────────────────────────

    def next_event(self, allowed_types: list[str] | None = None) -> dict | None:
        """
        Return one valid event, respecting the current state of every user
        and every class.  Returns None if no valid event can be generated
        (caller should retry on the next tick).
        """
        # 1. Drain forced eviction events first — they maintain consistency
        self._check_expired_classes()
        if self._pending:
            ev = self._pending.pop(0)
            if allowed_types is None or ev["event_type"] in allowed_types:
                return ev

        # 2. Bucket users by state
        offline_u     = [u for u, s in self.states.items() if s.status == ST_OFFLINE]
        online_u      = [u for u, s in self.states.items() if s.status == ST_ONLINE]
        in_class_u    = [u for u, s in self.states.items() if s.status == ST_IN_CLASS]
        in_session_u  = [u for u, s in self.states.items() if s.status == ST_IN_SESSION]

        total = USER_POOL_SIZE
        offline_pct    = len(offline_u)    / total
        in_class_pct   = (len(in_class_u) + len(in_session_u)) / total

        cls_list = list(self.classes.values())

        # 3. Build weighted candidate list: (uid, event_type, weight)
        candidates: list[tuple[str, str, float]] = []

        # --- offline → login (boost if too many sitting idle) ---
        if offline_u and (allowed_types is None or "login" in (allowed_types or [])):
            boost = 2.5 if offline_pct > 0.45 else 1.2
            for uid in random.sample(offline_u, min(8, len(offline_u))):
                candidates.append((uid, "login", boost))

        # --- online → class_join (strongly preferred over logout) ---
        if online_u and cls_list:
            if allowed_types is None or "class_join" in (allowed_types or []):
                boost = 3.0 if in_class_pct < 0.30 else 1.5
                for uid in random.sample(online_u, min(8, len(online_u))):
                    candidates.append((uid, "class_join", boost))

        # --- online → logout (rare; students don't log out randomly mid-day) ---
        if online_u:
            if allowed_types is None or "logout" in (allowed_types or []):
                for uid in random.sample(online_u, min(4, len(online_u))):
                    candidates.append((uid, "logout", 0.3))

        # --- in_class → session_start (they're there to learn) ---
        if in_class_u:
            if allowed_types is None or "session_start" in (allowed_types or []):
                for uid in random.sample(in_class_u, min(8, len(in_class_u))):
                    candidates.append((uid, "session_start", 2.0))
            if allowed_types is None or "class_leave" in (allowed_types or []):
                for uid in random.sample(in_class_u, min(3, len(in_class_u))):
                    candidates.append((uid, "class_leave", 0.25))

        # --- in_session → session_end ---
        if in_session_u:
            if allowed_types is None or "session_end" in (allowed_types or []):
                for uid in random.sample(in_session_u, min(8, len(in_session_u))):
                    candidates.append((uid, "session_end", 1.0))

        if not candidates:
            return None

        # 4. Weighted pick
        total_w = sum(w for _, _, w in candidates)
        r       = random.uniform(0, total_w)
        cumul   = 0.0
        chosen_uid, chosen_evt = candidates[-1][0], candidates[-1][1]
        for uid, evt, w in candidates:
            cumul += w
            if r <= cumul:
                chosen_uid, chosen_evt = uid, evt
                break

        # 5. Apply state transition and build event
        st = self.states[chosen_uid]

        # Sanity guard (state may have changed due to eviction processing above)
        valid = (
            (chosen_evt == "login"         and st.status == ST_OFFLINE)   or
            (chosen_evt == "logout"        and st.status == ST_ONLINE)    or
            (chosen_evt == "class_join"    and st.status == ST_ONLINE)    or
            (chosen_evt == "class_leave"   and st.status == ST_IN_CLASS)  or
            (chosen_evt == "session_start" and st.status == ST_IN_CLASS)  or
            (chosen_evt == "session_end"   and st.status == ST_IN_SESSION)
        )
        if not valid:
            return None

        if chosen_evt == "login":
            ev = self._build_login(chosen_uid, st)
            st.status = ST_ONLINE

        elif chosen_evt == "logout":
            ev = self._build_logout(chosen_uid, st)
            st.status     = ST_OFFLINE
            st.session_id = None
            st.login_at   = None

        elif chosen_evt == "class_join":
            cls = random.choice(cls_list)
            ev  = self._build_class_join(chosen_uid, st, cls)
            st.status = ST_IN_CLASS

        elif chosen_evt == "class_leave":
            cid = st.current_class_id
            with self.classes_lock:
                cls = self.classes.get(cid)
            if cls is None:
                return None  # class already expired — skip
            ev = self._build_class_leave(chosen_uid, st, cls, "voluntary")
            st.status           = ST_ONLINE
            st.current_class_id = None
            st.class_join_at    = None
            # 60 % chance they immediately look for another class (queued next tick)
            if random.random() < 0.60 and cls_list:
                next_cls = random.choice([c for c in cls_list if c.class_id != cid] or cls_list)
                pending_join = self._build_class_join(chosen_uid, st, next_cls)
                st.status = ST_IN_CLASS
                self._pending.append(pending_join)

        elif chosen_evt == "session_start":
            cid = st.current_class_id
            with self.classes_lock:
                cls = self.classes.get(cid)
            if cls is None:
                return None
            ev = self._build_session_start(chosen_uid, st, cls)
            st.status = ST_IN_SESSION

        elif chosen_evt == "session_end":
            cid = st.current_class_id
            with self.classes_lock:
                cls = self.classes.get(cid)
            if cls is None:
                return None
            ev = self._build_session_end(chosen_uid, st, cls, "completed")
            st.status           = ST_IN_CLASS   # still in the class; may start another session
            st.session_start_at = None

        else:
            return None

        return ev


# ══════════════════════════════════════════════════════════════════════════════
#  Kinesis producer (unchanged from v1)
# ══════════════════════════════════════════════════════════════════════════════

class KinesisProducer:
    def __init__(self, stream_name: str, region: str):
        self.stream_name = stream_name
        self.client = boto3.client("kinesis", region_name=region)
        self.sent   = 0
        self.failed = 0

    def send(self, event: dict) -> bool:
        try:
            self.client.put_record(
                StreamName=self.stream_name,
                Data=json.dumps(event).encode("utf-8"),
                PartitionKey=event["user_id"],
            )
            self.sent += 1
            return True
        except ClientError as e:
            log.warning("Kinesis put_record failed: %s", e.response["Error"]["Code"])
            self.failed += 1
            return False

    def send_batch(self, events: list[dict]) -> int:
        CHUNK   = 500
        success = 0
        for i in range(0, len(events), CHUNK):
            chunk   = events[i : i + CHUNK]
            records = [
                {"Data": json.dumps(e).encode("utf-8"), "PartitionKey": e["user_id"]}
                for e in chunk
            ]
            try:
                resp         = self.client.put_records(StreamName=self.stream_name, Records=records)
                failed_count = resp.get("FailedRecordCount", 0)
                success += len(chunk) - failed_count
                self.sent   += len(chunk) - failed_count
                self.failed += failed_count
            except ClientError as e:
                log.error("Batch send failed: %s", e)
                self.failed += len(chunk)
        return success


# ══════════════════════════════════════════════════════════════════════════════
#  Run modes
# ══════════════════════════════════════════════════════════════════════════════

def _emit(event: dict, args, producer: KinesisProducer | None):
    if args.dry_run:
        print(json.dumps(event, indent=2))
    elif producer:
        producer.send(event)


def _log_class_state(gen: StatefulGenerator):
    """
    Emit a structured snapshot of active classes and user states.
    Parsed by console_server.py to power the class roster panel.
    Format is intentionally machine-readable (key=value pairs).
    """
    with gen.classes_lock:
        active_cls = list(gen.classes.values())

    counts = {ST_OFFLINE: 0, ST_ONLINE: 0, ST_IN_CLASS: 0, ST_IN_SESSION: 0}
    for st in gen.states.values():
        counts[st.status] += 1

    log.info(
        "SIM_STATE classes=%d active_students=%d offline=%d online=%d",
        len(active_cls),
        counts[ST_IN_CLASS] + counts[ST_IN_SESSION],
        counts[ST_OFFLINE],
        counts[ST_ONLINE],
    )
    for cls in active_cls:
        log.info(
            "CLASS_STAT id=%s students=%d name=%s category=%s difficulty=%s",
            cls.class_id,
            len(cls.students),
            cls.course_name.replace(" ", "_"),
            cls.course_category.replace(" ", "_"),
            cls.difficulty_level,
        )


def run_continuous(args, producer=None):
    gen      = StatefulGenerator(args)
    interval = 1.0 / args.rate
    log.info("Continuous mode | rate=%.1f ev/s | events=%s",
             args.rate, args.events or "all")
    total = 0
    try:
        while True:
            ev = gen.next_event(args.events)
            if ev:
                _emit(ev, args, producer)
                total += 1
                if total % 100 == 0:
                    sent   = producer.sent   if producer else total
                    failed = producer.failed if producer else 0
                    log.info("Events sent=%d  failed=%d", sent, failed)
                    _log_class_state(gen)
            time.sleep(interval)
    except KeyboardInterrupt:
        log.info("Stopped. Total=%d", total)


def run_spike(args, producer=None):
    gen = StatefulGenerator(args)
    log.info("Spike mode | normal=%.1f ev/s | spike=%.1f ev/s | spike-dur=%ds",
             args.rate, args.spike_rate, int(args.spike_duration))

    def send_phase(rate: float, duration: float, label: str):
        interval = 1.0 / rate
        deadline = time.time() + duration
        count    = 0
        while time.time() < deadline:
            ev = gen.next_event(args.events)
            if ev:
                _emit(ev, args, producer)
                count += 1
            time.sleep(interval)
        log.info("[%s] phase ended — events: %d", label, count)
        _log_class_state(gen)

    try:
        log.info("Phase 1: normal load for %ds", int(args.pre_spike_duration))
        send_phase(args.rate, args.pre_spike_duration, "NORMAL")
        log.info("Phase 2: SPIKE — %.0f ev/s for %ds", args.spike_rate, int(args.spike_duration))
        send_phase(args.spike_rate, args.spike_duration, "SPIKE")
        log.info("Phase 3: recovery for %ds", int(args.post_spike_duration))
        send_phase(args.rate, args.post_spike_duration, "RECOVERY")
    except KeyboardInterrupt:
        log.info("Stopped early.")


def run_failure_sim(args, producer=None):
    gen      = StatefulGenerator(args)
    interval = 1.0 / args.rate
    log.info("Failure-sim | rate=%.1f ev/s | pause=%ds after %ds",
             args.rate, int(args.failure_pause), int(args.failure_after))
    try:
        deadline = time.time() + args.failure_after
        while time.time() < deadline:
            ev = gen.next_event(args.events)
            if ev:
                _emit(ev, args, producer)
            time.sleep(interval)

        log.info("--- FAILURE SIMULATED: pausing for %ds ---", int(args.failure_pause))
        _log_class_state(gen)
        time.sleep(args.failure_pause)

        log.info("--- RECOVERY: resuming ---")
        run_continuous(args, producer)
    except KeyboardInterrupt:
        log.info("Stopped.")


def run_file(args):
    gen = StatefulGenerator(args)
    log.info("File mode | count=%d | output=%s", args.count, args.output)
    with open(args.output, "w") as f:
        written = 0
        attempts = 0
        while written < args.count:
            attempts += 1
            if attempts > args.count * 3:
                log.warning("Too many retries generating events — stopping at %d", written)
                break
            ev = gen.next_event(args.events)
            if ev is None:
                continue
            f.write(json.dumps(ev) + "\n")
            written += 1
            if written % 1000 == 0:
                log.info("Written %d / %d events", written, args.count)
    log.info("Done. File saved: %s", args.output)


# ══════════════════════════════════════════════════════════════════════════════
#  CLI
# ══════════════════════════════════════════════════════════════════════════════

def parse_args():
    p = argparse.ArgumentParser(
        description="EdTech pipeline — stateful event generator v2",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    p.add_argument("--mode",   choices=["continuous", "spike", "failure", "file"],
                   default="continuous")
    p.add_argument("--rate",   type=float, default=10.0,
                   help="Normal events per second (default: 10)")
    p.add_argument("--events", nargs="+", choices=ALL_EVENT_TYPES, default=None,
                   metavar="EVENT_TYPE",
                   help=f"Restrict to specific event types: {ALL_EVENT_TYPES}")
    p.add_argument("--dry-run", action="store_true",
                   help="Print events to console, do not send to Kinesis")

    # Kinesis
    p.add_argument("--stream", default="edtech-events-stream")
    p.add_argument("--region", default="ap-south-1")

    # Spike
    p.add_argument("--spike-rate",          type=float, default=50.0)
    p.add_argument("--spike-duration",      type=float, default=60.0)
    p.add_argument("--pre-spike-duration",  type=float, default=60.0)
    p.add_argument("--post-spike-duration", type=float, default=60.0)

    # Failure sim
    p.add_argument("--failure-after",  type=float, default=60.0)
    p.add_argument("--failure-pause",  type=float, default=30.0)

    # File mode
    p.add_argument("--count",  type=int, default=10000)
    p.add_argument("--output", type=str, default="events.json")

    return p.parse_args()


def main():
    args     = parse_args()
    producer = None
    if not args.dry_run and args.mode != "file":
        producer = KinesisProducer(args.stream, args.region)
        log.info("Connected to Kinesis stream: %s (%s)", args.stream, args.region)

    if args.mode == "continuous":
        run_continuous(args, producer)
    elif args.mode == "spike":
        run_spike(args, producer)
    elif args.mode == "failure":
        run_failure_sim(args, producer)
    elif args.mode == "file":
        run_file(args)


if __name__ == "__main__":
    main()