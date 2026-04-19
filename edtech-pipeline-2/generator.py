"""
EdTech Pipeline — Stateful Data Generator v4
=============================================
Complete rewrite.  Key fixes over v3:

  1. Round-robin user cycling — events spread across ALL online users so
     Grafana active_users = 60 % of --users regardless of --rate.
  2. 60 % online target enforced by a churn manager that runs every
     2 seconds; users naturally log in/out over time.
  3. Rate control via monotonic clock — actual ev/s matches configured rate.
  4. Spike mode doubles/triples distinct user activity AND rate.
  5. State machine never blocks the send loop — offline users are skipped
     silently and the cycle rotates to the next online user.

CLI is 100 % backward-compatible with v3 so console_server.py needs
no changes.

Usage examples
--------------
# Normal load — generator runs ON EC2-1, so uses localhost:9092 (internal port)
python generator.py --rate 50 --users 500 \\
    --bootstrap-servers localhost:9092 --topic edtech-events

# Spike (see dramatic surge in Grafana)
python generator.py --mode spike --rate 30 --spike-rate 300 \\
    --users 1000 --bootstrap-servers localhost:9092 --topic edtech-events

# Failure simulation
python generator.py --mode failure --rate 40 --users 500 \\
    --failure-after 90 --failure-pause 60 \\
    --bootstrap-servers localhost:9092 --topic edtech-events

# Dry run (no Kafka)
python generator.py --dry-run --rate 10 --users 200
"""

import argparse
import collections
import json
import logging
import random
import threading
import time
import uuid
from datetime import datetime, timezone

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("generator")

# ── Reference data ─────────────────────────────────────────────────────────────
REGIONS = ["Hyderabad", "Mumbai", "Delhi", "Bengaluru", "Chennai", "Pune", "Kolkata"]
DEVICES = ["mobile", "desktop", "tablet"]
OS_MAP  = {
    "mobile":  ["android", "ios"],
    "desktop": ["windows", "mac", "linux"],
    "tablet":  ["android", "ios", "windows"],
}
APP_VERSIONS = ["v2.1.0", "v2.0.5", "v1.9.3", "v1.8.7"]

COURSES = [
    {"class_id": "csg527", "course_name": "Cloud_Computing",          "course_category": "CS",           "difficulty_level": "hard"},
    {"class_id": "csg511", "course_name": "Machine_Learning",         "course_category": "Data_Science", "difficulty_level": "medium"},
    {"class_id": "csg415", "course_name": "Linear_Algebra",           "course_category": "Math",         "difficulty_level": "medium"},
    {"class_id": "csg302", "course_name": "Python_for_Beginners",     "course_category": "CS",           "difficulty_level": "easy"},
    {"class_id": "csg221", "course_name": "Data_Structures",          "course_category": "CS",           "difficulty_level": "medium"},
    {"class_id": "csg610", "course_name": "Deep_Learning",            "course_category": "Data_Science", "difficulty_level": "hard"},
    {"class_id": "csg499", "course_name": "Statistics_Probability",   "course_category": "Math",         "difficulty_level": "medium"},
    {"class_id": "csg388", "course_name": "Database_Systems",         "course_category": "CS",           "difficulty_level": "medium"},
    {"class_id": "csg175", "course_name": "Advanced_Algorithms",      "course_category": "CS",           "difficulty_level": "hard"},
    {"class_id": "csg244", "course_name": "Web_Development",          "course_category": "CS",           "difficulty_level": "easy"},
]

INTERACTION_TYPES  = ["video", "poll", "quiz", "chat"]
AGE_GROUPS         = ["18-22", "23-30", "31-40", "41+"]
USER_LEVELS        = ["beginner", "intermediate", "advanced"]
SUBSCRIPTION_TYPES = ["free", "premium"]
ALL_EVENT_TYPES    = ["login", "logout", "class_join", "class_leave",
                      "session_start", "session_end"]

# User lifecycle states
ST_OFFLINE    = "offline"
ST_ONLINE     = "online"       # logged in, browsing
ST_IN_CLASS   = "in_class"     # attending a live class
ST_IN_SESSION = "in_session"   # actively consuming content


# ══════════════════════════════════════════════════════════════════════════════
#  Data model
# ══════════════════════════════════════════════════════════════════════════════

class UserProfile:
    """Immutable per-user attributes (assigned at pool creation)."""
    __slots__ = (
        "user_id", "user_role", "subscription_type", "user_age_group",
        "user_level", "region", "device", "os", "app_version",
    )

    def __init__(self, uid: str, seed: int, is_instructor: bool,
                 sub_split: float, regions: list, dev_weights: list):
        rng = random.Random(seed)
        self.user_id           = uid
        self.user_role         = "instructor" if is_instructor else "student"
        self.subscription_type = rng.choices(
            SUBSCRIPTION_TYPES, weights=[1.0 - sub_split, sub_split]
        )[0]
        self.user_age_group = rng.choice(AGE_GROUPS)
        self.user_level     = rng.choice(USER_LEVELS)
        self.region         = rng.choice(regions)
        self.device         = rng.choices(DEVICES, weights=dev_weights)[0]
        self.os             = rng.choice(OS_MAP[self.device])
        self.app_version    = rng.choice(APP_VERSIONS)


class UserState:
    """Mutable per-user lifecycle state."""
    __slots__ = (
        "status", "session_id", "class_id",
        "login_ts", "join_ts", "session_ts",
        "interaction", "content_dur", "logout_at",
    )

    def __init__(self):
        self.status      = ST_OFFLINE
        self.session_id  = None
        self.class_id    = None
        self.login_ts    = None
        self.join_ts     = None
        self.session_ts  = None
        self.interaction = None
        self.content_dur = None
        self.logout_at   = None   # real-time ts to auto-logout


class LiveClass:
    """A running class session that students can join."""
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
        self.students: set    = set()


# ══════════════════════════════════════════════════════════════════════════════
#  Core generator
# ══════════════════════════════════════════════════════════════════════════════

class EdTechGenerator:
    """
    Stateful EdTech event generator with guaranteed user diversity.

    Design:
    ● 60 % of pool is kept online at all times by a churn manager.
    ● A deque cycles through ALL online users in random order so every
      active user appears in every Spark batch window.
    ● next_event() never returns None (forces a login if needed).
    ● Rate control lives outside this class, in the run_* functions.
    """

    TARGET_ONLINE  = 0.60   # fraction of pool that is online at steady state
    MIN_LOGIN_SECS = 60     # minimum time a user stays logged in (real seconds)
    MAX_LOGIN_SECS = 600    # maximum
    CLS_MIN_SECS   = 90     # live class duration (real seconds)
    CLS_MAX_SECS   = 240

    # Transition probabilities per state
    #   ST_ONLINE     → class_join : logout        = 4 : 1
    #   ST_IN_CLASS   → session_start : class_leave = 5 : 1
    #   ST_IN_SESSION → session_end only

    def __init__(self, args):
        self.pool_size = args.users
        instructor_n   = max(5, int(self.pool_size * 0.10))

        # ── Active regions ─────────────────────────────────────────────────────
        if getattr(args, "regions", None):
            req = {r.strip() for r in args.regions.split(",")}
            active_regions = [r for r in REGIONS if r in req] or REGIONS[:]
        else:
            active_regions = REGIONS[:]

        # ── Active courses ─────────────────────────────────────────────────────
        self.courses = COURSES[:]
        if getattr(args, "courses", None):
            ids = {c.strip() for c in args.courses.split(",")}
            filt = [c for c in COURSES if c["class_id"] in ids]
            if filt:
                self.courses = filt
        if getattr(args, "difficulty", None) and args.difficulty != "all":
            filt = [c for c in self.courses if c["difficulty_level"] == args.difficulty]
            if filt:
                self.courses = filt

        # ── Device weights ─────────────────────────────────────────────────────
        if getattr(args, "device_weights", None):
            dev_w = [float(x) for x in args.device_weights.split(":")]
        else:
            dev_w = [0.50, 0.30, 0.20]  # mobile : desktop : tablet

        sub_split        = float(getattr(args, "subscription_split", 0.40))
        self.max_classes = int(getattr(args, "max_classes", 6))

        # ── Build pool ─────────────────────────────────────────────────────────
        log.info("Initialising user pool: %d users …", self.pool_size)
        self.uids: list[str] = [f"u{i:06d}" for i in range(1, self.pool_size + 1)]

        self.profiles: dict[str, UserProfile] = {}
        self.states:   dict[str, UserState]   = {}
        for i, uid in enumerate(self.uids):
            self.profiles[uid] = UserProfile(
                uid, seed=i * 7919,
                is_instructor=(i < instructor_n),
                sub_split=sub_split,
                regions=active_regions,
                dev_weights=dev_w,
            )
            self.states[uid] = UserState()

        # ── Live classes ───────────────────────────────────────────────────────
        self.classes: dict[str, LiveClass] = {}
        self._cls_lock = threading.Lock()

        # ── Round-robin cycle of online users ──────────────────────────────────
        # The deque rotates by 1 on every next_event() call.
        # Rebuilt every 10 s (or whenever online population changes a lot).
        self._cycle: collections.deque = collections.deque()
        self._cycle_lock = threading.Lock()

        # ── Pending event buffer (class expiry cascades, same-user follow-ups) ─
        self._pending: list[dict] = []

        # ── Counters ───────────────────────────────────────────────────────────
        self.sent   = 0
        self.failed = 0

        # ── Warmup ─────────────────────────────────────────────────────────────
        self._replenish_classes()
        self._warmup()
        self._rebuild_cycle()

        on = self._online_count()
        log.info(
            "Generator ready: pool=%d  online=%d (%.0f%%)  classes=%d",
            self.pool_size, on, 100 * on / self.pool_size, len(self.classes),
        )

    # ── Internal helpers ───────────────────────────────────────────────────────

    def _online_count(self) -> int:
        return sum(1 for s in self.states.values() if s.status != ST_OFFLINE)

    def _session_id(self) -> str:
        return f"s{random.randint(100000, 999999)}"

    def _replenish_classes(self):
        """Ensure max_classes live classes exist."""
        target = min(self.max_classes, len(self.courses))
        instructor_ids = [
            uid for uid, p in self.profiles.items()
            if p.user_role == "instructor"
        ]
        now = time.time()
        with self._cls_lock:
            existing = set(self.classes)
            avail = [c for c in self.courses if c["class_id"] not in existing]
            random.shuffle(avail)
            for c in avail:
                if len(self.classes) >= target:
                    break
                inst = random.choice(instructor_ids) if instructor_ids else "u000001"
                expires = now + random.uniform(self.CLS_MIN_SECS, self.CLS_MAX_SECS)
                cls = LiveClass(c, inst, expires)
                self.classes[cls.class_id] = cls

    def _expire_classes(self) -> list[dict]:
        """Remove expired classes, evict students, return pending events."""
        now = time.time()
        evicted_events: list[dict] = []
        with self._cls_lock:
            expired = [c for c in self.classes.values() if now >= c.expires_at]
            for cls in expired:
                del self.classes[cls.class_id]
                for uid in list(cls.students):
                    st = self.states[uid]
                    if st.status == ST_IN_SESSION:
                        evicted_events.append(self._mk_session_end(uid, st, cls, "interrupted"))
                        st.status = ST_IN_CLASS
                        st.session_ts = None
                    if st.status == ST_IN_CLASS:
                        evicted_events.append(self._mk_class_leave(uid, st, cls, "class_ended"))
                        st.status   = ST_ONLINE
                        st.class_id = None
                        st.join_ts  = None
        if expired:
            self._replenish_classes()
        return evicted_events

    def _warmup(self):
        """
        Pre-populate realistic state:
          60 % → online/in_class/in_session (varied so first events aren't all logins)
        """
        rng = random.Random(12345)
        n   = self.pool_size
        shuffled = list(self.uids)
        rng.shuffle(shuffled)

        target_online = int(n * self.TARGET_ONLINE)
        now = time.time()

        # Mark users online
        for uid in shuffled[:target_online]:
            st            = self.states[uid]
            st.status     = ST_ONLINE
            st.session_id = self._session_id()
            st.login_ts   = now - rng.uniform(10, 400)
            st.logout_at  = now + rng.uniform(self.MIN_LOGIN_SECS, self.MAX_LOGIN_SECS)

        # Put ~50 % of online users in a class
        with self._cls_lock:
            cls_list = list(self.classes.values())
        if cls_list:
            in_class_n = int(target_online * 0.50)
            for uid in shuffled[:in_class_n]:
                st = self.states[uid]
                if st.status != ST_ONLINE:
                    continue
                cls = rng.choice(cls_list)
                st.status   = ST_IN_CLASS
                st.class_id = cls.class_id
                st.join_ts  = now - rng.uniform(5, 120)
                cls.students.add(uid)

            # Put ~35 % of in-class users in session
            in_class_ids = [u for u in shuffled[:in_class_n]
                            if self.states[u].status == ST_IN_CLASS]
            for uid in in_class_ids[:int(len(in_class_ids) * 0.70)]:
                st = self.states[uid]
                st.status      = ST_IN_SESSION
                st.session_ts  = now - rng.uniform(5, 90)
                st.interaction = rng.choice(INTERACTION_TYPES)
                st.content_dur = rng.randint(300, 3600)

        log.info(
            "Warmup: offline=%d  online=%d  in_class=%d  in_session=%d",
            sum(1 for s in self.states.values() if s.status == ST_OFFLINE),
            sum(1 for s in self.states.values() if s.status == ST_ONLINE),
            sum(1 for s in self.states.values() if s.status == ST_IN_CLASS),
            sum(1 for s in self.states.values() if s.status == ST_IN_SESSION),
        )

    def _rebuild_cycle(self):
        """Shuffle all non-offline users into the round-robin deque."""
        active = [u for u, s in self.states.items() if s.status != ST_OFFLINE]
        random.shuffle(active)
        with self._cycle_lock:
            self._cycle = collections.deque(active)

    # ── Churn management (called externally every 2 s) ─────────────────────────

    def churn(self) -> list[dict]:
        """
        1. Auto-logout users whose logout_at has passed.
        2. Bring enough offline users online to maintain TARGET_ONLINE.
        Returns login/logout events produced.
        """
        now    = time.time()
        events = []

        # Auto-logout
        for uid, st in self.states.items():
            if (st.status == ST_ONLINE and
                    st.logout_at is not None and now >= st.logout_at):
                events.append(self._mk_logout(uid, st))
                st.status = ST_OFFLINE
                st.session_id = st.login_ts = st.logout_at = None

        # Bring back offline users
        target  = int(self.pool_size * self.TARGET_ONLINE)
        deficit = target - self._online_count()
        if deficit > 0:
            offline = [u for u, s in self.states.items() if s.status == ST_OFFLINE]
            batch   = random.sample(offline, min(deficit, len(offline), max(5, deficit // 2 + 1)))
            for uid in batch:
                st            = self.states[uid]
                st.status     = ST_ONLINE
                st.session_id = self._session_id()
                st.login_ts   = now
                st.logout_at  = now + random.uniform(self.MIN_LOGIN_SECS, self.MAX_LOGIN_SECS)
                events.append(self._mk_login(uid, st))

        return events

    # ── Event builders ─────────────────────────────────────────────────────────

    def _base(self, uid: str, event_type: str) -> dict:
        p   = self.profiles[uid]
        now = datetime.now(timezone.utc)
        return {
            "event_id":          str(uuid.uuid4()),
            "user_id":           uid,
            "event_type":        event_type,
            "timestamp":         now.isoformat(),
            "schema_version":    "4.0",
            "user_role":         p.user_role,
            "subscription_type": p.subscription_type,
            "user_age_group":    p.user_age_group,
            "user_level":        p.user_level,
            "region":            p.region,
            "country":           "India",
            "timezone":          "Asia/Kolkata",
            "device":            p.device,
            "os":                p.os,
            "app_version":       p.app_version,
            "hour_of_day":       now.hour,
            "day_of_week":       now.strftime("%A"),
            "is_weekend":        now.weekday() >= 5,
            "is_premium":        p.subscription_type == "premium",
        }

    def _mk_login(self, uid: str, st: UserState) -> dict:
        ev = self._base(uid, "login")
        ev["session_id"] = st.session_id
        return ev

    def _mk_logout(self, uid: str, st: UserState) -> dict:
        ev = self._base(uid, "logout")
        ev["session_id"] = st.session_id
        if st.login_ts:
            ev["active_duration_sec"] = round(time.time() - st.login_ts)
        return ev

    def _mk_class_join(self, uid: str, st: UserState, cls: LiveClass) -> dict:
        st.class_id = cls.class_id
        st.join_ts  = time.time()
        cls.students.add(uid)
        ev = self._base(uid, "class_join")
        ev.update({
            "session_id":       st.session_id,
            "class_id":         cls.class_id,
            "course_name":      cls.course_name.replace("_", " "),
            "course_category":  cls.course_category.replace("_", " "),
            "difficulty_level": cls.difficulty_level,
            "instructor_id":    cls.instructor_id,
            "session_type":     cls.session_type,
        })
        return ev

    def _mk_class_leave(self, uid: str, st: UserState, cls: LiveClass,
                        reason: str = "voluntary") -> dict:
        cls.students.discard(uid)
        ev = self._base(uid, "class_leave")
        ev.update({
            "session_id":       st.session_id,
            "class_id":         cls.class_id,
            "course_name":      cls.course_name.replace("_", " "),
            "course_category":  cls.course_category.replace("_", " "),
            "difficulty_level": cls.difficulty_level,
            "instructor_id":    cls.instructor_id,
            "leave_reason":     reason,
        })
        if st.join_ts:
            ev["class_duration_sec"] = round(time.time() - st.join_ts)
        return ev

    def _mk_session_start(self, uid: str, st: UserState, cls: LiveClass) -> dict:
        interaction    = random.choice(INTERACTION_TYPES)
        content_dur    = random.randint(300, 3600)
        st.session_ts  = time.time()
        st.interaction = interaction
        st.content_dur = content_dur
        ev = self._base(uid, "session_start")
        ev.update({
            "session_id":       st.session_id,
            "class_id":         cls.class_id,
            "course_name":      cls.course_name.replace("_", " "),
            "course_category":  cls.course_category.replace("_", " "),
            "difficulty_level": cls.difficulty_level,
            "instructor_id":    cls.instructor_id,
            "session_type":     cls.session_type,
            "interaction_type": interaction,
            "content_id":       f"cnt{random.randint(100, 999)}",
            "content_duration": content_dur,
            "session_status":   "started",
        })
        return ev

    def _mk_session_end(self, uid: str, st: UserState, cls: LiveClass,
                        status: str = "completed") -> dict:
        p  = self.profiles[uid]
        ev = self._base(uid, "session_end")
        ev.update({
            "session_id":       st.session_id,
            "class_id":         cls.class_id,
            "course_name":      cls.course_name.replace("_", " "),
            "course_category":  cls.course_category.replace("_", " "),
            "difficulty_level": cls.difficulty_level,
            "instructor_id":    cls.instructor_id,
            "session_status":   status,
        })
        if st.session_ts:
            ev["session_duration_sec"] = round(time.time() - st.session_ts)

        if status == "interrupted":
            ev.update({"score": None, "passed": None, "engagement_score": None})
        else:
            base = {"beginner": (40, 75), "intermediate": (55, 85), "advanced": (65, 100)}
            lo, hi = base.get(p.user_level, (50, 80))
            raw = random.uniform(lo, hi)
            if cls.difficulty_level == "hard":   raw -= random.uniform(0, 10)
            elif cls.difficulty_level == "easy": raw += random.uniform(0, 10)
            score = round(max(0.0, min(100.0, raw)), 2)
            ev["score"]  = score
            ev["passed"] = score >= 60
            if st.session_ts and st.content_dur:
                eng = min(1.0, (time.time() - st.session_ts) / st.content_dur)
                bonus = 0.1 if st.interaction in ("quiz", "poll") else 0.0
                ev["engagement_score"] = round(min(1.0, eng + bonus), 3)
            else:
                ev["engagement_score"] = round(random.uniform(0.40, 0.95), 3)
        return ev

    # ── Core tick ──────────────────────────────────────────────────────────────

    def next_event(self, allowed: list | None = None) -> dict:
        """
        Return the next valid event.  NEVER returns None — if the cycle is
        exhausted or stuck, forces a login from the offline pool.

        Round-robin: each call rotates the deque by 1 so events are spread
        across ALL online users rather than concentrating on a few.
        """
        # ── 1. Flush pending buffer first (class expiry cascades) ──────────────
        while self._pending:
            ev = self._pending.pop(0)
            if allowed is None or ev["event_type"] in allowed:
                return ev

        # ── 2. Expire classes, buffer eviction events ──────────────────────────
        evicted = self._expire_classes()
        self._pending.extend(evicted)
        while self._pending:
            ev = self._pending.pop(0)
            if allowed is None or ev["event_type"] in allowed:
                return ev

        # ── 3. Round-robin through online users ────────────────────────────────
        with self._cycle_lock:
            # Try up to full-cycle before giving up
            n_try = max(1, len(self._cycle))
            for _ in range(n_try):
                if not self._cycle:
                    break
                uid = self._cycle[0]
                self._cycle.rotate(-1)

                st = self.states[uid]
                if st.status == ST_OFFLINE:
                    continue

                ev = self._transition(uid, st, allowed)
                if ev is not None:
                    return ev

        # ── 4. Fallback: force a login so we always return an event ────────────
        offline = [u for u, s in self.states.items() if s.status == ST_OFFLINE]
        if offline:
            uid = random.choice(offline)
            st  = self.states[uid]
            st.status     = ST_ONLINE
            st.session_id = self._session_id()
            st.login_ts   = time.time()
            st.logout_at  = time.time() + random.uniform(self.MIN_LOGIN_SECS,
                                                          self.MAX_LOGIN_SECS)
            ev = self._mk_login(uid, st)
            if allowed is None or "login" in allowed:
                return ev

        # ── 5. Last resort: synthetic heartbeat-style login from pool ──────────
        uid = random.choice(self.uids)
        st  = self.states[uid]
        st.status     = ST_ONLINE
        st.session_id = self._session_id()
        st.login_ts   = time.time()
        st.logout_at  = time.time() + random.uniform(self.MIN_LOGIN_SECS,
                                                      self.MAX_LOGIN_SECS)
        return self._mk_login(uid, st)

    def _transition(self, uid: str, st: UserState,
                    allowed: list | None) -> dict | None:
        """
        Pick and execute the most natural next event for this user's state.
        Returns None if no valid/allowed transition exists.
        """
        if st.status == ST_ONLINE:
            # 80 % → class_join, 20 % → logout
            with self._cls_lock:
                cls_list = list(self.classes.values())
            if cls_list and (allowed is None or "class_join" in allowed):
                if random.random() < 0.80:
                    cls = random.choice(cls_list)
                    ev  = self._mk_class_join(uid, st, cls)
                    st.status = ST_IN_CLASS
                    return ev
            if allowed is None or "logout" in allowed:
                ev = self._mk_logout(uid, st)
                st.status = ST_OFFLINE
                st.session_id = st.login_ts = st.logout_at = None
                return ev

        elif st.status == ST_IN_CLASS:
            # 85 % → session_start, 15 % → class_leave
            with self._cls_lock:
                cls = self.classes.get(st.class_id)
            if cls is None:
                # Class expired between cycle rebuild — clean up silently
                st.status = ST_ONLINE
                st.class_id = st.join_ts = None
                return None
            if random.random() < 0.85:
                if allowed is None or "session_start" in allowed:
                    ev = self._mk_session_start(uid, st, cls)
                    st.status = ST_IN_SESSION
                    return ev
            if allowed is None or "class_leave" in allowed:
                ev = self._mk_class_leave(uid, st, cls, "voluntary")
                st.status = ST_ONLINE
                st.class_id = st.join_ts = None
                # 70 % chance to immediately switch to another class
                if random.random() < 0.70:
                    with self._cls_lock:
                        others = [c for c in self.classes.values()
                                  if c.class_id != cls.class_id]
                    if others:
                        next_cls = random.choice(others)
                        join = self._mk_class_join(uid, st, next_cls)
                        st.status = ST_IN_CLASS
                        self._pending.append(join)
                return ev

        elif st.status == ST_IN_SESSION:
            with self._cls_lock:
                cls = self.classes.get(st.class_id)
            if cls is None:
                st.status = ST_ONLINE
                st.class_id = st.join_ts = st.session_ts = None
                return None
            if allowed is None or "session_end" in allowed:
                ev = self._mk_session_end(uid, st, cls, "completed")
                st.status     = ST_IN_CLASS
                st.session_ts = None
                return ev

        return None

    # ── Simulation state log (parsed by console_server.py) ────────────────────

    def log_sim_state(self):
        counts = {ST_OFFLINE: 0, ST_ONLINE: 0, ST_IN_CLASS: 0, ST_IN_SESSION: 0}
        for s in self.states.values():
            counts[s.status] += 1

        active_students = counts[ST_IN_CLASS] + counts[ST_IN_SESSION]
        online_total    = counts[ST_ONLINE] + counts[ST_IN_CLASS] + counts[ST_IN_SESSION]

        with self._cls_lock:
            cls_list = list(self.classes.values())

        log.info(
            "SIM_STATE classes=%d active_students=%d offline=%d online=%d",
            len(cls_list), active_students, counts[ST_OFFLINE], online_total,
        )
        log.info(
            "Events sent=%d  failed=%d",
            self.sent, self.failed,
        )
        for cls in cls_list:
            log.info(
                "CLASS_STAT id=%s students=%d name=%s category=%s difficulty=%s",
                cls.class_id, len(cls.students),
                cls.course_name,
                cls.course_category,
                cls.difficulty_level,
            )


# ══════════════════════════════════════════════════════════════════════════════
#  Kafka producer
# ══════════════════════════════════════════════════════════════════════════════

class KafkaEventProducer:
    """
    Thin kafka-python wrapper.

    ● Events keyed by user_id → same partition per user → ordered per user.
    ● lz4 compression + linger_ms=5 for throughput.
    ● send() returns True/False; failures are logged but not fatal.
    """

    def __init__(self, bootstrap_servers: str, topic: str,
                 security_protocol: str = "PLAINTEXT",
                 sasl_mechanism: str | None = None,
                 sasl_username: str | None = None,
                 sasl_password: str | None = None):
        from kafka import KafkaProducer
        from kafka.errors import NoBrokersAvailable

        self.topic  = topic
        self.sent   = 0
        self.failed = 0

        kwargs: dict = {
            "bootstrap_servers": bootstrap_servers,
            "value_serializer":  lambda v: json.dumps(v).encode("utf-8"),
            "key_serializer":    lambda k: k.encode("utf-8") if k else None,
            "acks":              1,
            "retries":           3,
            "linger_ms":         10,
            "batch_size":        65536,
            "compression_type":  "lz4",
        }
        if security_protocol != "PLAINTEXT":
            kwargs["security_protocol"] = security_protocol
        if sasl_mechanism:
            kwargs["sasl_mechanism"]      = sasl_mechanism
            kwargs["sasl_plain_username"] = sasl_username or ""
            kwargs["sasl_plain_password"] = sasl_password or ""

        log.info("Connecting to Kafka: %s  topic='%s'", bootstrap_servers, topic)
        try:
            self._prod = KafkaProducer(**kwargs)
            self._prod.partitions_for(topic)
            log.info("Kafka connection OK — topic '%s' reachable", topic)
        except NoBrokersAvailable as exc:
            log.error("No Kafka broker at %s: %s", bootstrap_servers, exc)
            raise
        except Exception as exc:
            log.error("Kafka init failed: %s", exc)
            raise

    def send(self, event: dict) -> bool:
        try:
            self._prod.send(self.topic, value=event, key=event.get("user_id"))
            self.sent += 1
            return True
        except Exception as exc:
            log.warning("Kafka send failed: %s  user=%s", exc, event.get("user_id", "?"))
            self.failed += 1
            return False

    def flush(self):
        try:
            self._prod.flush(timeout=10)
        except Exception:
            pass

    def close(self):
        self.flush()
        try:
            self._prod.close()
        except Exception:
            pass


# ══════════════════════════════════════════════════════════════════════════════
#  Rate-controlled send loop
# ══════════════════════════════════════════════════════════════════════════════

def _send(ev: dict, args, producer: KafkaEventProducer | None,
          gen: EdTechGenerator):
    """Emit one event; update generator counters."""
    if args.dry_run:
        print(json.dumps(ev))
        gen.sent += 1
    elif producer:
        if producer.send(ev):
            gen.sent += 1
        else:
            gen.failed += 1


def _run_phase(gen: EdTechGenerator, rate: float, duration_secs: float | None,
               label: str, args, producer: KafkaEventProducer | None,
               allowed: list | None = None) -> int:
    """
    Core send loop.

    Sends events at exactly `rate` events / second.
    Uses monotonic clock + drift correction — no accumulated lag.

    Churn runs every 2 s to maintain the 60 % online target.
    Cycle is rebuilt every 10 s for fresh randomisation.

    Returns number of events sent in this phase.
    """
    if rate <= 0:
        return 0

    # Batch events per sleep: for low rates 1-event batches, for high rates
    # up to 25 events per sleep to reduce OS overhead.
    batch = max(1, min(25, int(rate / 4)))
    interval = batch / rate          # seconds between batches

    deadline      = (time.monotonic() + duration_secs) if duration_secs else float("inf")
    next_tick     = time.monotonic()
    last_churn    = time.monotonic()
    last_rebuild  = time.monotonic()
    last_state_log = time.monotonic()
    count = 0

    log.info("Phase 1: normal load for %.0fs", duration_secs or 0) if "NORMAL" in label else None
    log.info("Phase 2: SPIKE — %.0f ev/s for %.0fs", rate, duration_secs or 0) if "SPIKE" in label else None
    log.info("Phase 3: recovery for %.0fs", duration_secs or 0) if "RECOVERY" in label else None

    while time.monotonic() < deadline:
        now = time.monotonic()

        # ── Churn every 2 s ───────────────────────────────────────────────────
        if now - last_churn >= 2.0:
            churn_evs = gen.churn()
            for ev in churn_evs:
                if allowed is None or ev["event_type"] in allowed:
                    _send(ev, args, producer, gen)
                    count += 1
            last_churn = now

        # ── Rebuild cycle every 10 s ──────────────────────────────────────────
        if now - last_rebuild >= 10.0:
            gen._rebuild_cycle()
            last_rebuild = now

        # ── State log every 5 s ───────────────────────────────────────────────
        if now - last_state_log >= 5.0:
            gen.log_sim_state()
            last_state_log = now

        # ── Generate a batch of events ────────────────────────────────────────
        for _ in range(batch):
            ev = gen.next_event(allowed)
            _send(ev, args, producer, gen)
            count += 1

        # ── Precise sleep until next tick ─────────────────────────────────────
        next_tick += interval
        sleep = next_tick - time.monotonic()
        if sleep > 0:
            time.sleep(sleep)
        elif sleep < -2.0:
            # Fell more than 2 s behind (slow machine / heavy GC) — reset clock
            next_tick = time.monotonic() + interval

    log.info("[%s] phase complete — events this phase: %d", label, count)
    gen.log_sim_state()
    return count


# ══════════════════════════════════════════════════════════════════════════════
#  Run modes
# ══════════════════════════════════════════════════════════════════════════════

def run_continuous(args, producer=None):
    gen = EdTechGenerator(args)
    log.info(
        "Continuous mode | rate=%.1f ev/s | users=%d | events=%s",
        args.rate, args.users, args.events or "all",
    )
    try:
        _run_phase(gen, args.rate, None, "CONTINUOUS", args, producer, args.events)
    except KeyboardInterrupt:
        log.info("Stopped. Total sent=%d  failed=%d", gen.sent, gen.failed)
    finally:
        if producer:
            producer.close()


def run_spike(args, producer=None):
    gen = EdTechGenerator(args)
    log.info(
        "Spike mode | normal=%.1f ev/s | spike=%.1f ev/s | "
        "pre=%ds spike=%ds post=%ds | users=%d",
        args.rate, args.spike_rate,
        int(args.pre_spike_duration), int(args.spike_duration),
        int(args.post_spike_duration), args.users,
    )
    try:
        # Phase 1 — normal
        log.info("Phase 1: normal load for %.0fs", args.pre_spike_duration)
        _run_phase(gen, args.rate,       args.pre_spike_duration,  "NORMAL",   args, producer, args.events)

        # Phase 2 — spike
        log.info("Phase 2: SPIKE — %.0f ev/s for %.0fs", args.spike_rate, args.spike_duration)
        # Bring extra users online during spike for realism
        gen.TARGET_ONLINE = min(0.95, gen.TARGET_ONLINE + 0.20)
        churn_evs = gen.churn()
        for ev in churn_evs:
            _send(ev, args, producer, gen)
        gen._rebuild_cycle()

        _run_phase(gen, args.spike_rate, args.spike_duration,      "SPIKE",    args, producer, args.events)

        # Phase 3 — recovery
        log.info("Phase 3: recovery for %.0fs", args.post_spike_duration)
        gen.TARGET_ONLINE = max(0.50, gen.TARGET_ONLINE - 0.20)
        _run_phase(gen, args.rate,       args.post_spike_duration, "RECOVERY", args, producer, args.events)

    except KeyboardInterrupt:
        log.info("Stopped early. sent=%d  failed=%d", gen.sent, gen.failed)
    finally:
        if producer:
            producer.close()


def run_failure_sim(args, producer=None):
    gen = EdTechGenerator(args)
    log.info(
        "Failure-sim | rate=%.1f ev/s | normal=%ds | pause=%ds | users=%d",
        args.rate, int(args.failure_after), int(args.failure_pause), args.users,
    )
    try:
        log.info("Phase 1: normal load for %.0fs", args.failure_after)
        _run_phase(gen, args.rate, args.failure_after, "NORMAL", args, producer, args.events)

        log.info("--- FAILURE SIMULATED: pausing producer for %.0fs ---", args.failure_pause)
        gen.log_sim_state()
        time.sleep(args.failure_pause)

        log.info("--- RECOVERY: resuming ---")
        _run_phase(gen, args.rate, None, "RECOVERY", args, producer, args.events)

    except KeyboardInterrupt:
        log.info("Stopped. sent=%d  failed=%d", gen.sent, gen.failed)
    finally:
        if producer:
            producer.close()


def run_file(args):
    gen   = EdTechGenerator(args)
    count = args.count
    log.info("File mode | events=%d | output=%s | users=%d", count, args.output, args.users)
    written = 0
    with open(args.output, "w") as fh:
        while written < count:
            ev = gen.next_event(args.events)
            fh.write(json.dumps(ev) + "\n")
            written += 1
            if written % 1000 == 0:
                log.info("Written %d / %d events …", written, count)
    log.info("Done. %s saved (%d events).", args.output, written)


# ══════════════════════════════════════════════════════════════════════════════
#  CLI
# ══════════════════════════════════════════════════════════════════════════════

def parse_args():
    p = argparse.ArgumentParser(
        description="EdTech pipeline — stateful event generator v4 (Kafka)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )

    # Core
    p.add_argument("--mode",   choices=["continuous", "spike", "failure", "file"],
                   default="continuous")
    p.add_argument("--rate",   type=float, default=10.0,
                   help="Normal events per second (default: 10)")
    p.add_argument("--users",  type=int,   default=200,
                   help="Total simulated user pool (default: 200)")
    p.add_argument("--events", nargs="+",  choices=ALL_EVENT_TYPES, default=None,
                   metavar="EVENT_TYPE",
                   help="Restrict to these event types (default: all)")
    p.add_argument("--dry-run", action="store_true",
                   help="Print events to stdout, skip Kafka")

    # Kafka
    # Generator runs on EC2-1 → uses localhost:9092 (internal Kafka port)
    p.add_argument("--bootstrap-servers", default="localhost:9092")
    p.add_argument("--topic",             default="edtech-events")
    p.add_argument("--security-protocol", default="PLAINTEXT",
                   choices=["PLAINTEXT", "SASL_PLAINTEXT", "SSL", "SASL_SSL"])
    p.add_argument("--sasl-mechanism",    default=None,
                   choices=["PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"])
    p.add_argument("--sasl-username",     default=None)
    p.add_argument("--sasl-password",     default=None)

    # Simulation filters
    p.add_argument("--regions",             type=str,   default=None)
    p.add_argument("--courses",             type=str,   default=None)
    p.add_argument("--difficulty",          default="all",
                   choices=["easy", "medium", "hard", "all"])
    p.add_argument("--subscription-split",  type=float, default=0.40, metavar="FRAC")
    p.add_argument("--device-weights",      type=str,   default="0.5:0.3:0.2", metavar="M:D:T")
    p.add_argument("--event-weights",       type=str,   default=None,
                   metavar="W1:W2:W3:W4:W5:W6",
                   help="(legacy flag — accepted but ignored in v4)")
    p.add_argument("--max-classes",         type=int,   default=6)
    p.add_argument("--instructors",         type=int,   default=None,
                   help="(legacy — accepted but auto-derived as 10%% of --users)")

    # Spike
    p.add_argument("--spike-rate",          type=float, default=100.0)
    p.add_argument("--spike-duration",      type=float, default=60.0)
    p.add_argument("--pre-spike-duration",  type=float, default=60.0)
    p.add_argument("--post-spike-duration", type=float, default=60.0)

    # Failure
    p.add_argument("--failure-after", type=float, default=60.0)
    p.add_argument("--failure-pause", type=float, default=30.0)

    # File
    p.add_argument("--count",  type=int, default=10000)
    p.add_argument("--output", type=str, default="events.json")

    args = p.parse_args()

    # Back-compat: instructors flag is accepted but ignored
    if args.instructors is None:
        args.instructors = max(5, int(args.users * 0.10))

    return args


def main():
    args     = parse_args()
    producer = None

    if not args.dry_run and args.mode != "file":
        producer = KafkaEventProducer(
            bootstrap_servers=args.bootstrap_servers,
            topic=args.topic,
            security_protocol=args.security_protocol,
            sasl_mechanism=args.sasl_mechanism,
            sasl_username=args.sasl_username,
            sasl_password=args.sasl_password,
        )

    dispatch = {
        "continuous": run_continuous,
        "spike":      run_spike,
        "failure":    run_failure_sim,
        "file":       run_file,
    }
    fn = dispatch[args.mode]
    if args.mode == "file":
        fn(args)
    else:
        fn(args, producer)


if __name__ == "__main__":
    main()
