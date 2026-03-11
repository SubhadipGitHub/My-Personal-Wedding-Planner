import sqlite3
import base64
import hashlib
import hmac
import re
import secrets
import html
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import pandas as pd
import plotly.express as px
import streamlit as st

# Store the database alongside the app file so it is created in a consistent location
APP_DIR = Path(__file__).resolve().parent
DB_PATH = APP_DIR / 'wedding_planner.db'
SESSION_STATE_FILE = APP_DIR / '.streamlit_login.json'
SETTINGS_DEFAULTS = {
    'currency_symbol': 'Rs ', 'currency_code': 'INR', 'timezone': 'Asia/Kolkata',
    'date_format': '%d-%b-%Y', 'user_name': 'Me', 'partner_name': 'Partner',
    'expense_categories': 'Venue,Dress,Cake,Decor,Photography,Travel,Jewelry,Invitation,Catering,Other',
    'planning_categories': 'Dress,Cake,Event,Vendor,Decoration,Guest Task,Other'
}
st.set_page_config(page_title='Wedding Planner', page_icon=':ring:', layout='wide')


def db():
    # Ensure the directory exists (important for some deployment environments).
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    c = sqlite3.connect(DB_PATH)
    c.execute('PRAGMA foreign_keys = ON')
    return c


def init(c):
    c.executescript('''
    CREATE TABLE IF NOT EXISTS settings(setting_key TEXT PRIMARY KEY, setting_value TEXT NOT NULL);
    CREATE TABLE IF NOT EXISTS members(id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL UNIQUE, role TEXT, created_at TEXT NOT NULL);
    CREATE TABLE IF NOT EXISTS user_accounts(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        member_id INTEGER NOT NULL,
        username TEXT NOT NULL UNIQUE,
        password_hash TEXT NOT NULL,
        is_admin INTEGER NOT NULL DEFAULT 0,
        budget_role TEXT,
        expenses_role TEXT,
        plans_role TEXT,
        created_at TEXT NOT NULL,
        last_login TEXT,
        FOREIGN KEY(member_id) REFERENCES members(id) ON DELETE CASCADE
    );
    CREATE TABLE IF NOT EXISTS access_grants(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        viewer_member_id INTEGER NOT NULL,
        target_member_id INTEGER NOT NULL,
        scope TEXT NOT NULL,
        created_at TEXT NOT NULL,
        UNIQUE(viewer_member_id, target_member_id, scope),
        FOREIGN KEY(viewer_member_id) REFERENCES members(id) ON DELETE CASCADE,
        FOREIGN KEY(target_member_id) REFERENCES members(id) ON DELETE CASCADE
    );
    CREATE TABLE IF NOT EXISTS budget_items(id INTEGER PRIMARY KEY AUTOINCREMENT, category TEXT NOT NULL, allocated_amount REAL NOT NULL, notes TEXT, created_at TEXT NOT NULL);
    CREATE TABLE IF NOT EXISTS expenses(id INTEGER PRIMARY KEY AUTOINCREMENT, expense_date TEXT NOT NULL, title TEXT NOT NULL, category TEXT NOT NULL, amount REAL NOT NULL, paid_by INTEGER, for_whom INTEGER, status TEXT NOT NULL, bill_link TEXT, email_ref TEXT, notes TEXT, created_at TEXT NOT NULL, FOREIGN KEY(paid_by) REFERENCES members(id) ON DELETE SET NULL, FOREIGN KEY(for_whom) REFERENCES members(id) ON DELETE SET NULL);
    CREATE TABLE IF NOT EXISTS expense_allocations(id INTEGER PRIMARY KEY AUTOINCREMENT, expense_id INTEGER NOT NULL, member_id INTEGER, allocated_amount REAL NOT NULL, created_at TEXT NOT NULL, FOREIGN KEY(expense_id) REFERENCES expenses(id) ON DELETE CASCADE, FOREIGN KEY(member_id) REFERENCES members(id) ON DELETE SET NULL);
    CREATE TABLE IF NOT EXISTS expense_people(id INTEGER PRIMARY KEY AUTOINCREMENT, expense_id INTEGER NOT NULL, member_id INTEGER NOT NULL, relation_type TEXT NOT NULL, created_at TEXT NOT NULL, FOREIGN KEY(expense_id) REFERENCES expenses(id) ON DELETE CASCADE, FOREIGN KEY(member_id) REFERENCES members(id) ON DELETE CASCADE);
    CREATE TABLE IF NOT EXISTS plans(id INTEGER PRIMARY KEY AUTOINCREMENT, item_type TEXT NOT NULL, title TEXT NOT NULL, due_date TEXT, assigned_to INTEGER, status TEXT NOT NULL, estimated_cost REAL, notes TEXT, created_at TEXT NOT NULL, FOREIGN KEY(assigned_to) REFERENCES members(id) ON DELETE SET NULL);
    ''')
    ua_cols = {r[1] for r in c.execute("PRAGMA table_info(user_accounts)").fetchall()}
    if "is_admin" not in ua_cols:
        c.execute("ALTER TABLE user_accounts ADD COLUMN is_admin INTEGER NOT NULL DEFAULT 0")
    if "is_global_admin" not in ua_cols:
        c.execute("ALTER TABLE user_accounts ADD COLUMN is_global_admin INTEGER NOT NULL DEFAULT 0")
    for col in ["budget_role", "expenses_role", "plans_role", "last_login", "reset_token", "reset_expires_at"]:
        if col not in ua_cols:
            c.execute(f"ALTER TABLE user_accounts ADD COLUMN {col} TEXT")
    # Default non-admin member roles to edit so members can manage their own data.
    c.execute("UPDATE user_accounts SET budget_role=COALESCE(NULLIF(TRIM(budget_role),''),'edit')")
    c.execute("UPDATE user_accounts SET expenses_role=COALESCE(NULLIF(TRIM(expenses_role),''),'edit')")
    c.execute("UPDATE user_accounts SET plans_role=COALESCE(NULLIF(TRIM(plans_role),''),'edit')")
    # Ensure admins have full edit rights.
    c.execute("UPDATE user_accounts SET budget_role='edit', expenses_role='edit', plans_role='edit' WHERE is_admin=1")

    # Allow multiple accounts to belong to the same family (member).
    # Previously user_accounts.member_id was UNIQUE; this migration removes that constraint.
    idxs = c.execute("PRAGMA index_list(user_accounts)").fetchall()
    for idx in idxs:
        # idx: (seq, name, unique, origin, partial)
        if idx[2] == 1:
            name = idx[1]
            cols = [r[2] for r in c.execute(f"PRAGMA index_info({name})").fetchall()]
            if cols == ["member_id"]:
                c.execute(
                    """
                    CREATE TABLE IF NOT EXISTS user_accounts_new(
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        member_id INTEGER NOT NULL,
                        username TEXT NOT NULL UNIQUE,
                        password_hash TEXT NOT NULL,
                        is_admin INTEGER NOT NULL DEFAULT 0,
                        is_global_admin INTEGER NOT NULL DEFAULT 0,
                        budget_role TEXT,
                        expenses_role TEXT,
                        plans_role TEXT,
                        created_at TEXT NOT NULL,
                        last_login TEXT,
                        reset_token TEXT,
                        reset_expires_at TEXT,
                        FOREIGN KEY(member_id) REFERENCES members(id) ON DELETE CASCADE
                    );
                    """
                )
                c.execute(
                    "INSERT INTO user_accounts_new(id,member_id,username,password_hash,is_admin,is_global_admin,budget_role,expenses_role,plans_role,created_at,last_login,reset_token,reset_expires_at) "
                    "SELECT id,member_id,username,password_hash,is_admin,0,budget_role,expenses_role,plans_role,created_at,last_login,reset_token,reset_expires_at FROM user_accounts"
                )
                c.execute("DROP TABLE user_accounts")
                c.execute("ALTER TABLE user_accounts_new RENAME TO user_accounts")
                break

    budget_cols = {r[1] for r in c.execute("PRAGMA table_info(budget_items)").fetchall()}
    if "owner_member_id" not in budget_cols:
        c.execute("ALTER TABLE budget_items ADD COLUMN owner_member_id INTEGER")
    c.execute(
        """
        UPDATE budget_items
        SET owner_member_id = COALESCE(owner_member_id, member_id)
        WHERE owner_member_id IS NULL
        """
    )

    exp_cols = {r[1] for r in c.execute("PRAGMA table_info(expenses)").fetchall()}
    if "owner_member_id" not in exp_cols:
        c.execute("ALTER TABLE expenses ADD COLUMN owner_member_id INTEGER")
    c.execute(
        """
        UPDATE expenses
        SET owner_member_id = COALESCE(owner_member_id, paid_by, (SELECT id FROM members ORDER BY id LIMIT 1))
        WHERE owner_member_id IS NULL
        """
    )

    plan_cols = {r[1] for r in c.execute("PRAGMA table_info(plans)").fetchall()}
    if "owner_member_id" not in plan_cols:
        c.execute("ALTER TABLE plans ADD COLUMN owner_member_id INTEGER")
    c.execute(
        """
        UPDATE plans
        SET owner_member_id = COALESCE(owner_member_id, assigned_to, (SELECT id FROM members ORDER BY id LIMIT 1))
        WHERE owner_member_id IS NULL
        """
    )
    # lightweight schema migrations
    cols = {r[1] for r in c.execute("PRAGMA table_info(expense_allocations)").fetchall()}
    if "allocation_type" not in cols:
        c.execute("ALTER TABLE expense_allocations ADD COLUMN allocation_type TEXT")
    if "allocation_value" not in cols:
        c.execute("ALTER TABLE expense_allocations ADD COLUMN allocation_value REAL")
    c.execute("UPDATE expense_allocations SET allocation_type='By Amount' WHERE allocation_type IS NULL OR TRIM(allocation_type)=''")
    c.execute("UPDATE expense_allocations SET allocation_value=allocated_amount WHERE allocation_value IS NULL")
    exp_cols = {r[1] for r in c.execute("PRAGMA table_info(expenses)").fetchall()}
    if "due_date" not in exp_cols:
        c.execute("ALTER TABLE expenses ADD COLUMN due_date TEXT")
    c.execute("UPDATE expenses SET due_date=expense_date WHERE due_date IS NULL OR TRIM(due_date)=''")
    budget_cols = {r[1] for r in c.execute("PRAGMA table_info(budget_items)").fetchall()}
    if "member_id" not in budget_cols:
        c.execute("ALTER TABLE budget_items ADD COLUMN member_id INTEGER")
    c.execute(
        """
        UPDATE budget_items
        SET member_id = (SELECT id FROM members ORDER BY id LIMIT 1)
        WHERE member_id IS NULL
        """
    )
    # backfill legacy single-value paid_by/for_whom into expense_people
    c.execute(
        """
        INSERT INTO expense_people(expense_id, member_id, relation_type, created_at)
        SELECT e.id, e.paid_by, 'paid_by', COALESCE(e.created_at, CURRENT_TIMESTAMP)
        FROM expenses e
        WHERE e.paid_by IS NOT NULL
        AND NOT EXISTS (
            SELECT 1 FROM expense_people ep
            WHERE ep.expense_id = e.id AND ep.relation_type = 'paid_by'
        )
        """
    )
    c.execute(
        """
        INSERT INTO expense_people(expense_id, member_id, relation_type, created_at)
        SELECT e.id, e.for_whom, 'for_whom', COALESCE(e.created_at, CURRENT_TIMESTAMP)
        FROM expenses e
        WHERE e.for_whom IS NOT NULL
        AND NOT EXISTS (
            SELECT 1 FROM expense_people ep
            WHERE ep.expense_id = e.id AND ep.relation_type = 'for_whom'
        )
        """
    )

    # Ensure there is always at least one global admin account.
    # If no admin exists, create (or promote) the default admin user "admin".
    admin_exists = c.execute("SELECT 1 FROM user_accounts WHERE is_admin=1 LIMIT 1").fetchone()
    if not admin_exists:
        now = datetime.now().isoformat(timespec='seconds')
        member_name = "Admin"
        admin_username = "admin"
        admin_password = "Welcome@12345"

        mrow = c.execute("SELECT id FROM members WHERE name=?", (member_name,)).fetchone()
        if mrow:
            member_id = int(mrow[0])
        else:
            member_id = c.execute(
                "INSERT INTO members(name,role,created_at) VALUES(?,?,?)",
                (member_name, "Admin", now),
            ).lastrowid

        urow = c.execute("SELECT id FROM user_accounts WHERE username=?", (admin_username,)).fetchone()
        if urow:
            c.execute(
                "UPDATE user_accounts SET is_admin=1, is_global_admin=1, budget_role='edit', expenses_role='edit', plans_role='edit' WHERE id= ?",
                (int(urow[0]),),
            )
        else:
            pwh = hash_password(admin_password)
            c.execute(
                "INSERT INTO user_accounts(member_id,username,password_hash,is_admin,is_global_admin,budget_role,expenses_role,plans_role,created_at,last_login) VALUES(?,?,?,?,?,?,?,?,?,?)",
                (member_id, admin_username, pwh, 1, 1, "edit", "edit", "edit", now, None),
            )

    # Also ensure the default "admin" account is always a global admin.
    c.execute("UPDATE user_accounts SET is_global_admin=1 WHERE username='admin'")

    c.commit()


def seed(c):
    now = datetime.now().isoformat(timespec='seconds')
    for k, v in SETTINGS_DEFAULTS.items():
        c.execute('INSERT INTO settings(setting_key,setting_value) VALUES(?,?) ON CONFLICT(setting_key) DO NOTHING', (k, v))
    c.commit()


def q(c, sql, p=()):
    return pd.read_sql_query(sql, c, params=p)


def settings(c):
    s = SETTINGS_DEFAULTS.copy()
    rows = c.execute('SELECT setting_key,setting_value FROM settings').fetchall()
    s.update({k: v for k, v in rows})
    return s


def save_settings(c, s):
    for k, v in s.items():
        c.execute('INSERT INTO settings(setting_key,setting_value) VALUES(?,?) ON CONFLICT(setting_key) DO UPDATE SET setting_value=excluded.setting_value', (k, str(v)))
    c.commit()


def members_map(c):
    rows = c.execute('SELECT id,name FROM members ORDER BY name').fetchall()
    return {n: i for i, n in rows}


def _pbkdf2_hash(password, salt, iterations=210_000):
    return hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, iterations)


def hash_password(password):
    password = (password or "").strip()
    if len(password) < 6:
        raise ValueError("Password must be at least 6 characters.")
    iterations = 210_000
    salt = secrets.token_bytes(16)
    digest = _pbkdf2_hash(password, salt, iterations)
    return "pbkdf2_sha256${}${}${}".format(
        iterations,
        base64.b64encode(salt).decode("ascii"),
        base64.b64encode(digest).decode("ascii"),
    )


def verify_password(stored, password):
    try:
        algo, iters, salt_b64, digest_b64 = (stored or "").split("$", 3)
        if algo != "pbkdf2_sha256":
            return False
        iterations = int(iters)
        salt = base64.b64decode(salt_b64.encode("ascii"))
        expected = base64.b64decode(digest_b64.encode("ascii"))
        actual = _pbkdf2_hash(password or "", salt, iterations)
        return hmac.compare_digest(expected, actual)
    except Exception:
        return False


def create_password_reset_token(c, username, expires_minutes=60):
    username = (username or "").strip()
    if not username:
        return None
    token = secrets.token_urlsafe(24)
    expires_at = (datetime.now() + timedelta(minutes=expires_minutes)).isoformat(timespec="seconds")
    cur = c.execute(
        "UPDATE user_accounts SET reset_token=?, reset_expires_at=? WHERE username=?",
        (token, expires_at, username),
    )
    if cur.rowcount == 0:
        return None
    c.commit()
    return token


def reset_password_with_token(c, token, new_password):
    token = (token or "").strip()
    if not token:
        return False, "Invalid token."
    row = c.execute(
        "SELECT id, reset_expires_at FROM user_accounts WHERE reset_token=?",
        (token,),
    ).fetchone()
    if not row:
        return False, "Invalid token."
    uid, expires_at = row
    if not expires_at:
        return False, "Invalid token."
    try:
        expires_dt = datetime.fromisoformat(expires_at)
    except Exception:
        return False, "Invalid token."
    if expires_dt < datetime.now():
        return False, "Token has expired."
    try:
        pwh = hash_password(new_password)
    except ValueError as e:
        return False, str(e)
    c.execute(
        "UPDATE user_accounts SET password_hash=?, reset_token=NULL, reset_expires_at=NULL WHERE id=?",
        (pwh, uid),
    )
    c.commit()
    return True, None


def _persist_auth_uid(uid: int | None):
    """Persist the current logged-in user for reloads/refreshes."""
    try:
        if uid is None:
            if SESSION_STATE_FILE.exists():
                SESSION_STATE_FILE.unlink()
            return
        SESSION_STATE_FILE.write_text(str(int(uid)))
    except Exception:
        pass


def _load_persisted_auth_uid():
    if not SESSION_STATE_FILE.exists():
        return None
    try:
        return int(SESSION_STATE_FILE.read_text().strip())
    except Exception:
        return None


def auth_clear():
    for k in ["auth_user_id"]:
        if k in st.session_state:
            del st.session_state[k]
    _persist_auth_uid(None)


def auth_get(c):
    uid = st.session_state.get("auth_user_id")
    if not uid:
        uid = _load_persisted_auth_uid()
        if uid:
            st.session_state["auth_user_id"] = uid
    if not uid:
        return None
    row = c.execute(
        """
        SELECT ua.id, ua.username, ua.member_id, ua.is_admin, ua.is_global_admin, ua.budget_role, ua.expenses_role, ua.plans_role, m.name
        FROM user_accounts ua
        JOIN members m ON m.id = ua.member_id
        WHERE ua.id=?
        """,
        (int(uid),),
    ).fetchone()
    if not row:
        auth_clear()
        return None
    return {
        "user_id": int(row[0]),
        "username": row[1],
        "member_id": int(row[2]),
        "is_admin": int(row[3]) == 1,
        "is_global_admin": int(row[4]) == 1,
        "budget_role": (row[5] or "edit"),
        "expenses_role": (row[6] or "edit"),
        "plans_role": (row[7] or "edit"),
        "member_name": row[8],
    }


def allowed_member_ids(c, auth, scope):
    if not auth:
        return set()
    # Global admins can access all families.
    if auth.get("is_global_admin"):
        rows = c.execute("SELECT id FROM members").fetchall()
        return {int(r[0]) for r in rows}
    # Family admins can only access their own family (and any additional families explicitly granted).
    if auth.get("is_admin"):
        return {int(auth["member_id"])}
    viewer = int(auth["member_id"])
    scope = (scope or "").strip().lower()
    allowed_scopes = [scope, "all"]
    rows = c.execute(
        """
        SELECT target_member_id
        FROM access_grants
        WHERE viewer_member_id=?
        AND lower(scope) IN ({})
        """.format(",".join(["?"] * len(allowed_scopes))),
        (viewer, *allowed_scopes),
    ).fetchall()
    out = {viewer}
    out.update({int(r[0]) for r in rows})
    return out


def _norm_role(role):
    r = (role or "").strip().lower()
    return r if r in ["none", "view", "edit"] else "view"


def module_role(auth, module):
    if not auth:
        return "none"
    # Global and family admins can edit everything.
    if auth.get("is_global_admin") or auth.get("is_admin"):
        return "edit"

    # For budget/expenses/plans, members should be able to edit by default.
    # Only a role explicitly set to "none" blocks access.
    if module in ["budget", "expenses", "plans"]:
        role = (auth.get(f"{module}_role") or "").strip().lower()
        return "none" if role == "none" else "edit"

    # For other modules, use the stored role (defaults to view for safety).
    return _norm_role(auth.get(f"{module}_role"))


def can_view(auth, module):
    return module_role(auth, module) in ["view", "edit"]


def can_edit(auth, module):
    return module_role(auth, module) == "edit"


def _in_params(vals):
    vals = list(dict.fromkeys([int(v) for v in vals]))
    if not vals:
        return "(NULL)", ()
    return "({})".format(",".join(["?"] * len(vals))), tuple(vals)


def visible_expense_ids(c, allowed_ids):
    clause, params = _in_params(allowed_ids)
    rows = c.execute(
        f"""
        SELECT DISTINCT e.id
        FROM expenses e
        LEFT JOIN expense_people ep
            ON ep.expense_id=e.id AND ep.relation_type='paid_by'
        LEFT JOIN expense_allocations ea
            ON ea.expense_id=e.id
        WHERE (ep.member_id IN {clause} OR ea.member_id IN {clause})
        """,
        (*params, *params),
    ).fetchall()
    return {int(r[0]) for r in rows}


def _username_base_from_name(name):
    # Lowercase, keep alnum, convert whitespace to dots, collapse repeats.
    s = (name or "").strip().lower()
    s = re.sub(r"\s+", ".", s)
    s = re.sub(r"[^a-z0-9.]+", "", s)
    s = re.sub(r"\.+", ".", s).strip(".")
    return s or "user"


def _unique_username(c, base):
    base = _username_base_from_name(base)
    cand = base
    i = 2
    while c.execute("SELECT 1 FROM user_accounts WHERE username=? LIMIT 1", (cand,)).fetchone():
        cand = f"{base}{i}"
        i += 1
    return cand


def _random_temp_password(length=12):
    # Avoid punctuation to reduce login friction when copying.
    alphabet = "ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz23456789"
    length = max(8, int(length or 12))
    return "".join(secrets.choice(alphabet) for _ in range(length))


def login_sidebar(c):
    st.sidebar.markdown("### 🔐 Login")
    st.sidebar.caption("Sign in with your family account to access your budget, expenses, and plans.")
    existing = c.execute("SELECT COUNT(*) FROM user_accounts").fetchone()[0]
    auth = auth_get(c)
    if auth:
        st.sidebar.write(f"Signed in as: **{auth['username']}**")
        st.sidebar.caption(f"Member: {auth['member_name']} | Admin: {'Yes' if auth['is_admin'] else 'No'}")
        if st.sidebar.button("Logout"):
            auth_clear()
            st.rerun()
        return auth

    if existing == 0:
        st.sidebar.info("First-time setup: create the admin account.")
        with st.sidebar.form("setup_admin"):
            member_name = st.text_input("Admin profile name", value="Admin", help="This is the family name for the admin account.")
            username = st.text_input("Admin username", value="admin", help="This is the login username for the admin account.")
            password = st.text_input("Admin password", type="password", value="Welcome@12345", disabled=True)
            st.markdown("*The admin password is fixed to **Welcome@12345**.*")
            ok = st.form_submit_button("Create Admin")
        if ok:
            member_name = (member_name or "").strip()
            username = (username or "").strip()
            if not member_name:
                st.sidebar.error("Admin profile name is required.")
                return None
            if not username:
                st.sidebar.error("Admin username is required.")
                return None
            if not (password or "").strip():
                st.sidebar.error("Admin password is required.")
                return None
            try:
                pwh = hash_password(password)
            except ValueError as e:
                st.sidebar.error(str(e))
                return None
            now = datetime.now().isoformat(timespec="seconds")
            try:
                mrow = c.execute("SELECT id FROM members WHERE name=?", (member_name,)).fetchone()
                if mrow:
                    member_id = int(mrow[0])
                else:
                    inserted_m = c.execute(
                        "INSERT INTO members(name,role,created_at) VALUES(?,?,?)",
                        (member_name, "Admin", now),
                    )
                    member_id = int(inserted_m.lastrowid)
                curx = c.execute(
                    "INSERT INTO user_accounts(member_id,username,password_hash,is_admin,is_global_admin,budget_role,expenses_role,plans_role,created_at,last_login) VALUES(?,?,?,?,?,?,?,?,?,?)",
                    (member_id, username, pwh, 1, 0, "edit", "edit", "edit", now, None),
                )
                c.commit()
            except sqlite3.IntegrityError:
                st.sidebar.error("Username already exists or that member already has an account.")
                return None
            st.session_state["auth_user_id"] = int(curx.lastrowid)
            _persist_auth_uid(int(curx.lastrowid))
            st.rerun()
        return None

    with st.sidebar.form("login_form"):
        st.markdown(tooltip("Username", "Your login username."), unsafe_allow_html=True)
        username = st.text_input("", key="login_username")
        st.markdown(tooltip("Password", "Your account password."), unsafe_allow_html=True)
        password = st.text_input("", type="password", key="login_password")
        ok = st.form_submit_button("Login")
    if ok:
        row = c.execute(
            "SELECT id, password_hash FROM user_accounts WHERE username=?",
            (username.strip(),),
        ).fetchone()
        if not row or not verify_password(row[1], password):
            st.sidebar.error("Invalid username/password.")
            return None
        now = datetime.now().isoformat(timespec='seconds')
        c.execute("UPDATE user_accounts SET last_login=? WHERE id=?", (now, int(row[0])))
        st.session_state["auth_user_id"] = int(row[0])
        _persist_auth_uid(int(row[0]))
        st.rerun()

    with st.sidebar.expander("Forgot password?", expanded=False):
        st.markdown(
            "Enter your username to generate a reset token (valid for 1 hour). "
            "Then use that token below to set a new password."
        )
        with st.form("forgot_password_request"):
            fp_username = st.text_input("Username", key="fp_username", help="Your login username")
            fp_ok = st.form_submit_button("Generate reset token")
        if fp_ok:
            token = create_password_reset_token(c, fp_username)
            if not token:
                st.error("No account found with that username.")
            else:
                st.success("Reset token generated. Keep it safe and use it to reset your password below.")
                st.code(token)

        st.markdown("---")
        with st.form("forgot_password_reset"):
            fp_token = st.text_input("Reset token", key="fp_token", help="Paste the token you received above.")
            new_password = st.text_input("New password", type="password", key="fp_new_password", help="Minimum 6 characters.")
            confirm_password = st.text_input("Confirm new password", type="password", key="fp_confirm_password")
            fp_reset_ok = st.form_submit_button("Reset password")
        if fp_reset_ok:
            if new_password != confirm_password:
                st.error("Passwords do not match.")
            else:
                ok, msg = reset_password_with_token(c, fp_token, new_password)
                if not ok:
                    st.error(msg)
                else:
                    st.success("Password has been reset. You can now log in with your new password.")

    return None


def blank(v):
    return v is None or (isinstance(v, float) and pd.isna(v)) or str(v).strip() == ''


def cats(raw, fallback):
    src = raw if raw.strip() else fallback
    out = []
    for p in [x.strip() for x in src.split(',') if x.strip()]:
        if p.lower() not in [x.lower() for x in out]:
            out.append(p)
    return out


def cur(v, s):
    return f"{s['currency_symbol']}{float(v):,.2f}"


def badge(text, bg='#999', fg='#fff'):
    text = html.escape(str(text))
    return f"<span style='display:inline-block;padding:2px 10px;border-radius:12px;background:{bg};color:{fg};font-size:0.85em;font-weight:600;'>{text}</span>"


def status_badge(status):
    """Return a colored status badge for an expense status."""
    status = (status or "").strip().lower()
    if status == "paid":
        return badge("Paid", bg="#2d8a3f")
    if status == "pending":
        return badge("Pending", bg="#e09f3e")
    if status:
        return badge(status.title(), bg="#6c757d")
    return badge("—", bg="#adb5bd")


def tooltip(label, text):
    t = html.escape(str(text))
    return f"{label} <span title=\"{t}\" style='cursor:help;color:#0d6efd;'>ℹ️</span>"


def apply_apex_filters(df, key_prefix, hidden_cols=None):
    hidden_cols = hidden_cols or []
    cols = [c for c in df.columns if c not in hidden_cols]
    with st.expander("Filters", expanded=False):
        if df.empty:
            st.info("No rows found; filters are available once data is present.")
            return df, False
        qtext = st.text_input("Row Search (all columns)", key=f"{key_prefix}_row_search")
        selected_cols = st.multiselect(
            "Column Filters",
            options=cols,
            key=f"{key_prefix}_selected_cols",
            help="Pick columns and filter values/ranges by type.",
        )
        filtered = df.copy()
        filter_active = False
        if qtext.strip():
            filter_active = True
            q = qtext.strip().lower()
            mask = filtered.astype(str).apply(lambda row: row.str.lower().str.contains(q, na=False)).any(axis=1)
            filtered = filtered[mask]
        for col in selected_cols:
            filter_active = True
            s = filtered[col]
            if pd.api.types.is_datetime64_any_dtype(s):
                valid = s.dropna()
                if valid.empty:
                    continue
                dmin = valid.min().date()
                dmax = valid.max().date()
                dfrom, dto = st.date_input(
                    f"{col} range",
                    value=(dmin, dmax),
                    key=f"{key_prefix}_{col}_date",
                )
                filtered = filtered[(pd.to_datetime(filtered[col], errors="coerce").dt.date >= dfrom) & (pd.to_datetime(filtered[col], errors="coerce").dt.date <= dto)]
            elif pd.api.types.is_numeric_dtype(s):
                valid = pd.to_numeric(s, errors="coerce").dropna()
                if valid.empty:
                    continue
                nmin = float(valid.min())
                nmax = float(valid.max())
                low, high = st.slider(
                    f"{col} range",
                    min_value=nmin,
                    max_value=nmax,
                    value=(nmin, nmax),
                    key=f"{key_prefix}_{col}_num",
                )
                numeric = pd.to_numeric(filtered[col], errors="coerce")
                filtered = filtered[(numeric >= low) & (numeric <= high)]
            else:
                s_text = s.apply(lambda x: ", ".join(x) if isinstance(x, list) else str(x))
                vals = sorted([str(v) for v in s_text.dropna().unique().tolist()])
                picked = st.multiselect(f"{col} values", options=vals, default=vals, key=f"{key_prefix}_{col}_vals")
                if picked:
                    filtered = filtered[s_text.isin(picked)]
                else:
                    filtered = filtered.iloc[0:0]
        st.caption(f"Filtered rows: {len(filtered)} / {len(df)}")
        return filtered, filter_active


def upsert_grid(c, table, df, cols, validators, map_in=None, map_out=None, delete_missing=True):
    ex = {r[0] for r in c.execute(f'SELECT id FROM {table}').fetchall()}
    keep = set()
    now = datetime.now().isoformat(timespec='seconds')
    map_in = map_in or {}
    map_out = map_out or {}
    for _, r in df.iterrows():
        rid = None if blank(r.get('id')) else int(r.get('id'))
        row = {}
        for col in cols:
            v = r.get(col)
            row[col] = None if blank(v) else v
            if isinstance(row[col], str):
                row[col] = row[col].strip()
        if all(row.get(cn) in [None, ''] for cn in cols):
            if rid is None:
                continue
        for fn in validators:
            err = fn(row)
            if err:
                st.error(err)
                return False
        for col in cols:
            if col in map_in and row[col] is not None:
                row[col] = map_in[col].get(str(row[col]).strip())
        if rid in ex:
            setc = ','.join([f'{k}=?' for k in cols])
            vals = [row[k] for k in cols] + [rid]
            c.execute(f'UPDATE {table} SET {setc} WHERE id=?', vals)
            keep.add(rid)
        else:
            ins_cols = cols + ['created_at']
            vals = [row[k] for k in cols] + [now]
            ph = ','.join(['?'] * len(ins_cols))
            curx = c.execute(f"INSERT INTO {table}({','.join(ins_cols)}) VALUES({ph})", vals)
            keep.add(curx.lastrowid)
    if delete_missing:
        for did in ex - keep:
            c.execute(f'DELETE FROM {table} WHERE id=?', (did,))
    c.commit()
    return True


def header(s):
    st.markdown(
        f"### 💍 Wedding Planning Command Center\n"
        f"**{s['user_name']}** & **{s['partner_name']}** | {s['timezone']} | {s['currency_code']}"
    )
    st.markdown("---")
    st.caption("Manage budgets, expenses and plans for your family with ease. Use the tabs above to navigate.")


def dashboard_card(title, value, subtitle, color, href=None):
    content = (
        f"<div style='background:{color};padding:16px;border-radius:12px;box-shadow:0 2px 6px rgba(0,0,0,0.08);'>"
        f"<div style='font-size:0.85em;color:#ffffff70;margin-bottom:4px;'>{html.escape(title)}</div>"
        f"<div style='font-size:1.8em;font-weight:700;color:#fff;margin-bottom:6px;'>{html.escape(value)}</div>"
        f"<div style='font-size:0.85em;color:#ffffffcc;'>{html.escape(subtitle)}</div>"
        "</div>"
    )
    if href:
        return f"<a href=\"{html.escape(href)}\" style='text-decoration:none'>{content}</a>"
    return content


def overview(c, s, auth):
    mm = members_map(c)
    id_to_name = {v: k for k, v in mm.items()}
    allowed_names = [auth["member_name"]] if auth else []
    selected_members = st.multiselect(
        "Overview Members Filter (multi-select)",
        options=allowed_names,
        default=allowed_names,
        key="overview_member_filter_multi",
        help="Choose members to include in the dashboard metrics.",
    )

    if auth:
        e = q(c, 'SELECT id,amount,status,category,expense_date FROM expenses WHERE owner_member_id=?', (auth["member_id"],))
    else:
        e = q(c, 'SELECT id,amount,status,category,expense_date FROM expenses WHERE 0')
    alloc = q(
        c,
        """
        SELECT
            ea.expense_id,
            ea.allocated_amount,
            e.status,
            e.category,
            e.expense_date,
            m.name AS member_name
        FROM expense_allocations ea
        JOIN expenses e ON e.id = ea.expense_id
        LEFT JOIN members m ON m.id = ea.member_id
        WHERE e.owner_member_id=?
        """,
        (auth["member_id"],),
    )
    if not alloc.empty and not e.empty:
        alloc = alloc[alloc["expense_id"].isin(e["id"].tolist())]
    b = q(
        c,
        'SELECT bi.category,bi.allocated_amount,m.name AS member_name FROM budget_items bi LEFT JOIN members m ON m.id=bi.member_id WHERE owner_member_id=?',
        (auth["member_id"],),
    )
    paid_links = q(
        c,
        """
        SELECT ep.expense_id, m.name AS member_name
        FROM expense_people ep
        JOIN expenses e ON e.id = ep.expense_id
        LEFT JOIN members m ON m.id = ep.member_id
        WHERE ep.relation_type='paid_by' AND e.owner_member_id=?
        """,
        (auth["member_id"],),
    )

    total_budget = float(b['allocated_amount'].sum()) if not b.empty else 0.0
    use_allocation_metrics = not alloc.empty
    if selected_members:
        member_budget = float(b.loc[b["member_name"].isin(selected_members), "allocated_amount"].sum()) if not b.empty else 0.0
        if use_allocation_metrics:
            selected_alloc = alloc[alloc["member_name"].isin(selected_members)]
        else:
            paid_ids = set(paid_links.loc[paid_links["member_name"].isin(selected_members), "expense_id"].dropna().tolist()) if not paid_links.empty else set()
            e = e[e["id"].isin(paid_ids)] if paid_ids else e.iloc[0:0]
    else:
        member_budget = 0.0
        selected_alloc = alloc.iloc[0:0] if use_allocation_metrics else None
        e = e.iloc[0:0]

    tb = member_budget
    if use_allocation_metrics:
        te = float(selected_alloc['allocated_amount'].sum()) if not selected_alloc.empty else 0.0
        pdv = float(selected_alloc[selected_alloc['status'] == 'Paid']['allocated_amount'].sum()) if not selected_alloc.empty else 0.0
        pnd = float(selected_alloc[selected_alloc['status'] == 'Pending']['allocated_amount'].sum()) if not selected_alloc.empty else 0.0
        total_expenses_all = float(alloc['allocated_amount'].sum()) if not alloc.empty else 0.0
        total_pending_all = float(alloc[alloc['status'] == 'Pending']['allocated_amount'].sum()) if not alloc.empty else 0.0
    else:
        te = float(e['amount'].sum()) if not e.empty else 0.0
        pdv = float(e[e['status'] == 'Paid']['amount'].sum()) if not e.empty else 0.0
        pnd = float(e[e['status'] == 'Pending']['amount'].sum()) if not e.empty else 0.0
        total_expenses_all = te
        total_pending_all = pnd

    # Dashboard summary cards
    plans_df = q(c, "SELECT due_date FROM plans WHERE owner_member_id=?", (auth["member_id"],))
    plans_df['due_date'] = pd.to_datetime(plans_df['due_date'], errors='coerce')
    upcoming_plans = int(plans_df[plans_df['due_date'] >= pd.Timestamp.now().normalize()].shape[0]) if not plans_df.empty else 0

    c1, c2, c3 = st.columns(3)
    c1.markdown(dashboard_card("Total Budget", cur(total_budget, s), "Allocated budget amount", "#0d6efd", href="?tab=Budget"), unsafe_allow_html=True)
    c2.markdown(dashboard_card("Outstanding", cur(total_pending_all, s), "Pending expenses", "#dc3545", href="?tab=Expenses"), unsafe_allow_html=True)
    c3.markdown(dashboard_card("Upcoming Plans", str(upcoming_plans), "Due in the future", "#20c997", href="?tab=Planning"), unsafe_allow_html=True)

    budget_share = (tb / total_budget * 100.0) if total_budget > 0 else 0.0
    expense_share = (te / total_expenses_all * 100.0) if total_expenses_all > 0 else 0.0
    pending_share = (pnd / total_pending_all * 100.0) if total_pending_all > 0 else 0.0

    c1, c2, c3, c4 = st.columns(4)
    c1.metric('Selected Budget', cur(tb, s), f"{budget_share:.1f}% share")
    c2.metric('Selected Expenses', cur(te, s), f"{expense_share:.1f}% share")
    c3.metric('Selected Pending', cur(pnd, s), f"{pending_share:.1f}% share")
    c4.metric('Selected Paid', cur(pdv, s))

    # Charts: expense trends + category breakdown
    if not e.empty:
        st.markdown('### Expense Trends & Breakdown')
        e2 = e.copy()
        e2['expense_date'] = pd.to_datetime(e2['expense_date'], errors='coerce')
        e2 = e2.dropna(subset=['expense_date'])
        if not e2.empty:
            by_month = (
                e2.assign(month=e2['expense_date'].dt.to_period('M').dt.to_timestamp())
                .groupby('month')['amount']
                .sum()
                .reset_index()
            )
            fig_line = px.line(by_month, x='month', y='amount', title='Expenses over time')
            fig_line.update_layout(xaxis_title='Month', yaxis_title=f"Amount ({s['currency_code']})")

            cat = e2.groupby('category', as_index=False)['amount'].sum()
            fig_pie = px.pie(cat, names='category', values='amount', title='Expense breakdown by category')

            c1, c2 = st.columns(2)
            c1.plotly_chart(fig_line, use_container_width=True)
            c2.plotly_chart(fig_pie, use_container_width=True)

    # Recent activity feed
    st.markdown('### Recent Activity')
    act_expenses = q(c, "SELECT created_at, 'Expense' AS type, title AS desc FROM expenses WHERE owner_member_id=?", (auth['member_id'],))
    act_plans = q(c, "SELECT created_at, 'Plan' AS type, title AS desc FROM plans WHERE owner_member_id=?", (auth['member_id'],))
    act_budget = q(c, "SELECT created_at, 'Budget' AS type, category AS desc FROM budget_items WHERE owner_member_id=?", (auth['member_id'],))
    act = pd.concat([act_expenses, act_plans, act_budget], ignore_index=True)
    if not act.empty:
        act['created_at'] = pd.to_datetime(act['created_at'], errors='coerce')
        act = act.dropna(subset=['created_at']).sort_values('created_at', ascending=False).head(10)
        act['created_at'] = act['created_at'].dt.strftime('%Y-%m-%d %H:%M')
        st.table(act.rename(columns={'created_at': 'When', 'type': 'Type', 'desc': 'Description'}))
    else:
        st.write('No recent activity.')

    if use_allocation_metrics and selected_members and not selected_alloc.empty:
        by_cat = selected_alloc.groupby('category', as_index=False)['allocated_amount'].sum()
        st.plotly_chart(px.bar(by_cat, x='category', y='allocated_amount'), use_container_width=True)
        # Use expense_date as the reference for overdue/pending calculations (no separate due date needed).
        pending_base = selected_alloc[selected_alloc['status'] == 'Pending'][['expense_date', 'allocated_amount']].rename(columns={'allocated_amount': 'pending_amount'})
    else:
        if not e.empty:
            st.plotly_chart(px.bar(e.groupby('category', as_index=False)['amount'].sum(), x='category', y='amount'), use_container_width=True)
        pending_base = e[e['status'] == 'Pending'][['expense_date', 'amount']].rename(columns={'amount': 'pending_amount'}) if not e.empty else pd.DataFrame(columns=['expense_date', 'pending_amount'])

    if not pending_base.empty:
        pending_base['expense_date'] = pd.to_datetime(pending_base['expense_date'], errors='coerce')
        pending_base = pending_base.dropna(subset=['expense_date'])
        if not pending_base.empty:
            pending_base['expense_month'] = pending_base['expense_date'].dt.to_period('M').dt.to_timestamp()
            month_options = sorted(pending_base['expense_month'].dropna().unique().tolist())
            default_months = month_options
            selected_due_months = st.multiselect(
                "Pending Month Filter",
                options=month_options,
                default=default_months,
                format_func=lambda d: pd.Timestamp(d).strftime('%b %Y'),
                key='overview_pending_due_month_filter',
            )
            if selected_due_months:
                filtered_pending = pending_base[pending_base['expense_month'].isin(selected_due_months)]
            else:
                filtered_pending = pending_base.iloc[0:0]
            expense_month_series = filtered_pending.groupby('expense_month', as_index=False)['pending_amount'].sum().sort_values('expense_month')
            if not expense_month_series.empty:
                current_month = pd.Timestamp.now().normalize().replace(day=1)
                max_month = expense_month_series['expense_month'].max()
                month_axis = pd.date_range(start=current_month, end=max_month, freq='MS')
                rows = []
                for month_start in month_axis:
                    # Amount remains pending from current month until its month (inclusive).
                    still_pending = float(
                        expense_month_series.loc[expense_month_series['expense_month'] >= month_start, 'pending_amount'].sum()
                    )
                    rows.append({'month': month_start, 'pending_amount': still_pending})
                timeline_df = pd.DataFrame(rows)
                timeline_df['month_label'] = timeline_df['month'].dt.strftime('%b %Y')
                st.markdown('#### Pending Expense Timeline (Current Month to Expense Months)')
                fig = px.bar(timeline_df, x='month_label', y='pending_amount')
                fig.update_layout(xaxis_title='Month', yaxis_title=f"Pending ({s['currency_code']})")
                st.plotly_chart(fig, use_container_width=True)


def grid_family(c, auth):
    st.subheader('👪 Family')
    st.caption('Create and manage families. Admins can also manage user accounts for each family.')
    if not auth:
        st.info("Login required to manage families.")
        return

    is_family_admin = bool(auth.get("is_admin"))
    is_global_admin = bool(auth.get("is_global_admin"))

    df = q(c, 'SELECT id,name FROM members ORDER BY name')
    if df.empty:
        df = pd.DataFrame(columns=['id', 'name'])
    id_to_name = {int(r['id']): r['name'] for _, r in df.iterrows()}

    if is_global_admin:
        st.markdown("#### Family Grid")
        st.caption("Add or remove families here. Each family can have multiple user accounts and its own budget/expense data.")
        grid_df, filtered_mode = apply_apex_filters(df, "family", hidden_cols=["id"])
        ed = st.data_editor(
            grid_df,
            num_rows='dynamic',
            use_container_width=True,
            key='mgrid',
            column_config={'id': None},
        )
        if st.button('Save Family Grid'):
            ok = upsert_grid(
                c,
                'members',
                ed,
                ['name'],
                [lambda r: 'Name required' if not r.get('name') else None],
                delete_missing=not filtered_mode,
            )
            if ok:
                st.success('Saved')
                st.rerun()
    else:
        st.markdown("#### Your Family (read-only)")
        st.caption("Only global admins can add or remove families.")
        df = q(c, "SELECT id,name FROM members WHERE id=? ORDER BY name", (auth["member_id"],))
        if df.empty:
            df = pd.DataFrame(columns=["id", "name"])
        st.dataframe(df.drop(columns=["id"], errors="ignore"), use_container_width=True, hide_index=True)

    # If not family admin, allow viewing only your own family.
    if not is_family_admin:
        st.markdown("#### Your Family")
        df = q(c, "SELECT id,name FROM members WHERE id=? ORDER BY name", (auth["member_id"],))
        if df.empty:
            df = pd.DataFrame(columns=["id", "name"])
        st.dataframe(df.drop(columns=["id"], errors="ignore"), use_container_width=True, hide_index=True)
        return

    # Family admins can manage their own family; global admins can manage all families.
    # If global admin, show all accounts and allow selecting another family.
    managed_family_id = auth["member_id"]
    if is_global_admin:
        all_accounts = q(
            c,
            """
            SELECT ua.id, m.name AS family, ua.username, ua.is_admin, ua.created_at, ua.last_login
            FROM user_accounts ua
            JOIN members m ON m.id=ua.member_id
            ORDER BY m.name, ua.username
            """,
        )
        if all_accounts.empty:
            all_accounts = pd.DataFrame(columns=["id", "family", "username", "is_admin", "created_at", "last_login"])

        st.markdown("#### All Family Accounts")
        st.caption("View every account across all families. Use the selector below to manage a specific family.")
        st.dataframe(all_accounts.drop(columns=["id"], errors="ignore"), use_container_width=True, hide_index=True)

        st.markdown("---")
        st.markdown("#### Manage a Family")
        st.caption("Pick a family to see its members and to create/update accounts for that family.")
        id_to_name = {int(r['id']): r['name'] for _, r in df.iterrows()}
        family_ids = sorted(id_to_name.keys())
        if not family_ids:
            st.info("No families found. Create one using the Family Grid above.")
            return

        selected_family_id = st.selectbox(
            "Select family to manage",
            options=family_ids,
            format_func=lambda x: id_to_name.get(x, ""),
            key="selected_family_id",
        )
        managed_family_id = selected_family_id

    ua = q(
        c,
        """
        SELECT ua.id, m.name AS family, ua.username, ua.is_admin, ua.created_at, ua.last_login
        FROM user_accounts ua
        JOIN members m ON m.id=ua.member_id
        WHERE ua.member_id=?
        ORDER BY ua.username
        """,
        (managed_family_id,),
    )
    if ua.empty:
        ua = pd.DataFrame(columns=["id", "family", "username", "is_admin", "created_at", "last_login"])

    family_has_admin = bool((ua["is_admin"] == 1).any()) if not ua.empty else False

    membership = ua[["username", "is_admin", "created_at", "last_login"]].copy()
    membership["role"] = membership["is_admin"].apply(lambda v: "Admin" if int(v) == 1 else "Member")
    membership["created_at"] = pd.to_datetime(membership["created_at"], errors="coerce")
    membership["last_login"] = pd.to_datetime(membership["last_login"], errors="coerce")
    membership = membership[["username", "role", "created_at", "last_login"]]

    st.markdown("#### Family Membership")
    st.table(membership)

    st.markdown("#### Family Accounts")
    st.dataframe(ua.drop(columns=["id"], errors="ignore"), use_container_width=True, hide_index=True)

    missing = q(
        c,
        """
        SELECT m.id AS member_id, m.name AS member
        FROM members m
        LEFT JOIN user_accounts ua ON ua.member_id = m.id
        WHERE ua.id IS NULL
          AND m.id = ?
        ORDER BY m.name
        """,
        (managed_family_id,),
    )
    if missing.empty:
        st.caption("All family accounts are created.")
    else:
        st.dataframe(missing.drop(columns=["member_id"], errors="ignore"), use_container_width=True, hide_index=True)
        with st.form("bulk_create_accounts"):
            pw_len = st.number_input("Temporary password length", min_value=8, max_value=32, value=12, step=1)
            ok_bulk = st.form_submit_button("Generate Accounts For Missing Family Members")
        if ok_bulk:
            now = datetime.now().isoformat(timespec="seconds")
            created_rows = []
            for _, r in missing.iterrows():
                member_id = int(r["member_id"])
                member_name = str(r["member"])
                username = _unique_username(c, member_name)
                temp_pw = _random_temp_password(int(pw_len))
                pwh = hash_password(temp_pw)
                try:
                    c.execute(
                        "INSERT INTO user_accounts(member_id,username,password_hash,is_admin,created_at) VALUES(?,?,?,?,?)",
                        (member_id, username, pwh, 0, now),
                    )
                    created_rows.append({"member": member_name, "username": username, "temp_password": temp_pw})
                except sqlite3.IntegrityError:
                    continue
            c.commit()
            if created_rows:
                st.session_state["generated_creds"] = created_rows
                st.warning("Temporary passwords are shown once per generation. Store them now and have members change them.")
                out_df = pd.DataFrame(created_rows)
                st.dataframe(out_df, use_container_width=True, hide_index=True)
                st.download_button(
                    "Download CSV",
                    data=out_df.to_csv(index=False).encode("utf-8"),
                    file_name="member_credentials.csv",
                    mime="text/csv",
                )
                st.rerun()
            else:
                st.info("No accounts created.")

    family_name = id_to_name.get(managed_family_id, "")
    default_admin = not family_has_admin
    with st.expander("Create / Update Account", expanded=True):
        with st.form("ua_form"):
            st.markdown(f"**Family:** {family_name}")
            st.markdown(tooltip("Login username", "This will be the login credential for the account."), unsafe_allow_html=True)
            username = st.text_input("", key="ua_username")

            st.markdown(tooltip("Password", "Required when creating a new account. Leave blank to keep the current password when editing."), unsafe_allow_html=True)
            password = st.text_input("", type="password", key="ua_password")

            st.markdown(tooltip("Family admin", "Admins can manage family members and promote other admins."), unsafe_allow_html=True)
            is_admin = st.checkbox(
                "",
                value=default_admin,
                key="ua_is_admin",
            )
            ok = st.form_submit_button("Create/Update Account")
    if ok:
        username = (username or "").strip()
        if not username:
            st.error("Login username is required.")
            return
        now = datetime.now().isoformat(timespec="seconds")
        existing = c.execute("SELECT id, member_id, is_admin FROM user_accounts WHERE username=?", (username,)).fetchone()

        if not family_has_admin:
            is_admin = True

        try:
            if existing:
                existing_id, existing_member_id, existing_is_admin = existing
                if existing_member_id != managed_family_id:
                    st.error("Username already exists in another family.")
                    return

                # Prevent removing admin from last family admin.
                if existing_is_admin == 1 and not is_admin and (ua["is_admin"].sum() <= 1):
                    st.error("Cannot remove admin privileges from the last family admin.")
                    return

                set_pw = (password or "").strip() != ""
                if set_pw:
                    pwh = hash_password(password)
                    c.execute(
                        "UPDATE user_accounts SET password_hash=?, is_admin=? WHERE id=?",
                        (pwh, 1 if is_admin else 0, int(existing_id)),
                    )
                else:
                    c.execute(
                        "UPDATE user_accounts SET is_admin=? WHERE id=?",
                        (1 if is_admin else 0, int(existing_id)),
                    )
            else:
                if not (password or "").strip():
                    st.error("Password is required when creating a new account.")
                    return
                pwh = hash_password(password)
                c.execute(
                    "INSERT INTO user_accounts(member_id,username,password_hash,is_admin,created_at,last_login) VALUES(?,?,?,?,?,?)",
                    (managed_family_id, username.strip(), pwh, 1 if is_admin else 0, now, None),
                )
            c.commit()
        except sqlite3.IntegrityError:
            st.error("Username already exists.")
            return
        except ValueError as e:
            st.error(str(e))
            return
        st.success("Saved user account.")
        st.rerun()

    st.markdown("#### Access Model")
    st.caption("All family members can create/edit/delete records for their family. Family admins can promote another account to Family admin.")


def grid_budget(c, s, auth):
    st.subheader('💰 Budget')
    st.caption('Track your family budget categories and allocations.')
    if not can_view(auth, "budget"):
        st.info("You don't have access to Budget.")
        return
    mm = members_map(c)
    names = sorted(mm.keys())
    if not names:
        st.info("No families found. Create families in the Family tab first.")
        return
    cts = cats(s.get('expense_categories', ''), SETTINGS_DEFAULTS['expense_categories'])
    df = q(
        c,
        """
        SELECT
            bi.id,
            bi.category,
            bi.allocated_amount,
            m.name AS family,
            bi.notes,
            bi.member_id,
            bi.owner_member_id
        FROM budget_items bi
        LEFT JOIN members m ON m.id=bi.member_id
        WHERE bi.owner_member_id=?
        ORDER BY bi.id DESC
        """,
        (auth["member_id"],),
    )
    if df.empty:
        df = pd.DataFrame(columns=['id', 'category', 'allocated_amount', 'family', 'notes', 'member_id', 'owner_member_id'])

    if not can_edit(auth, "budget"):
        grid_df, _ = apply_apex_filters(df.drop(columns=["member_id", "owner_member_id"], errors="ignore"), "budget_ro", hidden_cols=["id"])
        st.dataframe(grid_df.drop(columns=["id"], errors="ignore"), use_container_width=True, hide_index=True)
        st.caption("Budget is read-only.")
        return

    with st.expander("Budget Editor", expanded=True):
        st.markdown("#### Budget Grid")
        st.markdown("**Required:** <span style='color:red;'>*</span> Category, Allocated amount, Family", unsafe_allow_html=True)
        st.caption("Edit budget lines and allocations. Use the filters to drill into specific categories or members.")
        grid_df, filtered_mode = apply_apex_filters(df, "budget", hidden_cols=["id", "member_id", "owner_member_id"])
        # Show family instead of member; default to current family for non-global admins.
        family_options = names if auth.get('is_global_admin') else [auth['member_name']]
        col_cfg = {
            'id': None,
            'member_id': None,
            'owner_member_id': None,
            'category': st.column_config.SelectboxColumn(options=cts, help="Category for this budget item."),
            'family': st.column_config.SelectboxColumn(options=family_options, help="Which family this budget item is for."),
        }
        grid_df['family'] = grid_df['family'].fillna(auth['member_name'])
        ed = st.data_editor(grid_df, num_rows='dynamic', use_container_width=True, key='bgrid', column_config=col_cfg)
        if st.button('Save Budget Grid'):
            ed2 = ed.copy()
            # Always enforce the correct family selection for non-global admins.
            if not auth.get('is_global_admin'):
                ed2['family'] = auth['member_name']
            ed2['member_id'] = ed2['family']
            ed2['owner_member_id'] = auth['member_id']
            ok = upsert_grid(c, 'budget_items', ed2, ['category', 'member_id', 'allocated_amount', 'notes', 'owner_member_id'], [
                lambda r: 'Budget category required' if not r.get('category') else None,
                lambda r: 'Family required' if not r.get('member_id') else None,
                lambda r: 'Allocated amount required' if r.get('allocated_amount') in [None, '', 0] else None,
                lambda r: 'Allocated amount must be > 0' if float(r.get('allocated_amount') or 0) <= 0 else None,
                lambda r: 'Owner required' if not r.get('owner_member_id') else None,
            ], map_in={'member_id': mm}, delete_missing=not filtered_mode)
            if ok:
                st.success('Saved')
                st.rerun()


def grid_expenses(c, s, auth):
    st.subheader('🧾 Expenses')
    st.caption('Log spending, assign who paid, and track expense status.')
    if not can_view(auth, "expenses"):
        st.info("You don't have access to Expenses.")
        return
    mm = members_map(c)
    names = sorted(mm.keys())
    id_to_name = {v: k for k, v in mm.items()}
    if not names:
        st.info("No families found. Create families in the Family tab first.")
        return
    cts = cats(s.get('expense_categories', ''), SETTINGS_DEFAULTS['expense_categories'])
    df = q(
        c,
        """
        SELECT
            e.id,
            e.expense_date,
            e.title,
            e.category,
            e.amount,
            e.paid_by,
            e.status,
            e.bill_link,
            e.email_ref,
            e.notes,
            e.owner_member_id,
            m.name AS family
        FROM expenses e
        LEFT JOIN members m ON m.id = e.owner_member_id
        WHERE e.owner_member_id=?
        ORDER BY e.expense_date DESC, e.id DESC
        """,
        (auth["member_id"],),
    )
    if df.empty:
        df = pd.DataFrame(columns=['id', 'expense_date', 'title', 'category', 'amount', 'paid_by', 'status', 'bill_link', 'email_ref', 'notes', 'owner_member_id', 'family'])
    else:
        status_counts = df['status'].astype(str).str.lower().value_counts()
        st.markdown(
            f"**Status summary:** {status_badge('paid')} {int(status_counts.get('paid', 0))}  |  {status_badge('pending')} {int(status_counts.get('pending', 0))}",
            unsafe_allow_html=True,
        )
        participants = q(
            c,
            '''
            SELECT ep.expense_id, ep.relation_type, m.name
            FROM expense_people ep
            LEFT JOIN members m ON m.id = ep.member_id
            JOIN expenses e ON e.id = ep.expense_id
            WHERE e.owner_member_id=?
            ORDER BY ep.id
            ''' ,
            (auth["member_id"],),
        )
        paid_map = {}
        whom_map = {}
        if not participants.empty:
            for _, r in participants.iterrows():
                if pd.isna(r["name"]):
                    continue
                if r["relation_type"] == "paid_by":
                    paid_map.setdefault(int(r["expense_id"]), []).append(str(r["name"]))
                elif r["relation_type"] == "for_whom":
                    whom_map.setdefault(int(r["expense_id"]), []).append(str(r["name"]))
        df["paid_by"] = df.apply(
            lambda r: paid_map.get(int(r["id"]), [id_to_name[r["paid_by"]]] if pd.notna(r["paid_by"]) and r["paid_by"] in id_to_name else []),
            axis=1,
        )
        df['expense_date'] = pd.to_datetime(df['expense_date'], errors='coerce')

        # Highlight overdue pending expenses (expense_date passed and status still pending)
        today = pd.Timestamp.now().normalize()
        df['overdue'] = (df['status'].astype(str).str.lower() == 'pending') & (df['expense_date'] < today)
        if df['overdue'].any():
            st.markdown('#### ⚠️ Overdue Pending Expenses')
            overdue_df = df[df['overdue']].copy()
            overdue_df['expense_date'] = overdue_df['expense_date'].dt.strftime('%Y-%m-%d')
            overdue_df = overdue_df.drop(columns=['overdue', 'owner_member_id'], errors='ignore')
            st.dataframe(
                overdue_df.style.apply(lambda row: ['background-color: #f8d7da' for _ in row], axis=1),
                use_container_width=True,
            )

    if not can_edit(auth, "expenses"):
        grid_df, _ = apply_apex_filters(df.drop(columns=["owner_member_id"], errors="ignore"), "expenses_ro", hidden_cols=["id", "overdue"])
        st.dataframe(grid_df.drop(columns=["id"], errors="ignore"), use_container_width=True, hide_index=True)
        st.caption("Expenses are read-only.")
        return
    with st.expander("Expense Editor", expanded=True):
        st.markdown("#### Expenses Grid")
        st.markdown("**Required:** <span style='color:red;'>*</span> Date, Title, Category, Amount, Paid by, Status", unsafe_allow_html=True)
        st.caption("Edit and track expenses. Use filters to narrow down to specific dates, categories, or status.")
        grid_df, filtered_mode = apply_apex_filters(df, "expenses", hidden_cols=["id", "owner_member_id", "overdue"])
        col_cfg = {
            'id': None,
            'owner_member_id': None,
            'expense_date': st.column_config.DateColumn(format='YYYY-MM-DD'),
            'category': st.column_config.SelectboxColumn(options=cts),
            'paid_by': st.column_config.MultiselectColumn(options=names),
            'status': st.column_config.SelectboxColumn(options=['Paid', 'Pending'], help="Mark an expense as Paid when settled. Pending means still due."),
        }
        ed = st.data_editor(grid_df, num_rows='dynamic', use_container_width=True, key='egrid', column_config=col_cfg)
        def v1(r): return 'Expense title required' if not r.get('title') else None
        def v2(r): return 'Expense category required' if not r.get('category') else None
        def v3(r):
            if r.get('amount') in [None, '', 0]:
                return 'Expense amount required'
            return 'Expense amount must be > 0' if float(r.get('amount') or 0) <= 0 else None
        def v4(r):
            vals = r.get('paid_by') if isinstance(r.get('paid_by'), list) else ([r.get('paid_by')] if r.get('paid_by') else [])
            if not vals:
                return 'Paid By required'
            for v in vals:
                if v not in mm:
                    return 'Paid By invalid'
            return None
        def v5(r):
            if not r.get('status'):
                return 'Status required'
            return 'Status invalid' if r.get('status') not in ['Paid', 'Pending'] else None
        def v6(r):
            if blank(r.get('expense_date')): return 'Expense date required'
            if pd.isna(pd.to_datetime(r.get('expense_date'), errors='coerce')): return 'Expense date invalid'
            return None
        if st.button('Save Expenses Grid'):
            ed2 = ed.copy()
            ed2['expense_date'] = pd.to_datetime(ed2['expense_date'], errors='coerce').dt.date.astype(str)
            ex = {r[0] for r in c.execute('SELECT id FROM expenses WHERE owner_member_id=?', (auth["member_id"],)).fetchall()}
            keep = set()
            now = datetime.now().isoformat(timespec='seconds')
            for _, r in ed2.iterrows():
                rid = None if blank(r.get('id')) else int(r.get('id'))
                owner_id = auth["member_id"]
                row = {
                    'expense_date': r.get('expense_date'),
                    'title': '' if blank(r.get('title')) else str(r.get('title')).strip(),
                    'category': '' if blank(r.get('category')) else str(r.get('category')).strip(),
                    'amount': 0.0 if blank(r.get('amount')) else float(r.get('amount')),
                    'paid_by': r.get('paid_by') if isinstance(r.get('paid_by'), list) else ([] if blank(r.get('paid_by')) else [r.get('paid_by')]),
                    'status': '' if blank(r.get('status')) else str(r.get('status')).strip(),
                    'bill_link': '' if blank(r.get('bill_link')) else str(r.get('bill_link')).strip(),
                    'email_ref': '' if blank(r.get('email_ref')) else str(r.get('email_ref')).strip(),
                    'notes': '' if blank(r.get('notes')) else str(r.get('notes')).strip(),
                }
                if all([blank(row['expense_date']), not row['title'], not row['category'], row['amount'] == 0.0, not row['paid_by'], not row['status']]):
                    if rid is None:
                        continue
                for fn in [v1, v2, v3, v4, v5, v6]:
                    err = fn(row)
                    if err:
                        st.error(err)
                        return
                # Use expense_date as the due date; highlight overdue pending items if past today.
                paid_ids = [mm[v] for v in row['paid_by'] if v in mm]
                paid_primary = paid_ids[0] if paid_ids else None
                if rid in ex:
                    c.execute(
                        """
                        UPDATE expenses
                        SET expense_date=?, due_date=?, title=?, category=?, amount=?, paid_by=?, for_whom=?, status=?, bill_link=?, email_ref=?, notes=?, owner_member_id=?
                        WHERE id=?
                        """,
                        (row['expense_date'], row['expense_date'], row['title'], row['category'], row['amount'], paid_primary, None, row['status'], row['bill_link'], row['email_ref'], row['notes'], owner_id, rid),
                    )
                    expense_id = rid
                    keep.add(rid)
                else:
                    inserted = c.execute(
                        """
                        INSERT INTO expenses(expense_date,due_date,title,category,amount,paid_by,for_whom,status,bill_link,email_ref,notes,owner_member_id,created_at)
                        VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
                        """,
                        (row['expense_date'], row['expense_date'], row['title'], row['category'], row['amount'], paid_primary, None, row['status'], row['bill_link'], row['email_ref'], row['notes'], owner_id, now),
                    )
                    expense_id = inserted.lastrowid
                    keep.add(expense_id)
                c.execute('DELETE FROM expense_people WHERE expense_id=?', (expense_id,))
                for pid in paid_ids:
                    c.execute('INSERT INTO expense_people(expense_id,member_id,relation_type,created_at) VALUES(?,?,?,?)', (expense_id, pid, 'paid_by', now))
            if not filtered_mode:
                for did in ex - keep:
                    c.execute('DELETE FROM expenses WHERE id=?', (did,))
            c.commit()
            st.success('Saved')
            st.rerun()

    st.markdown('#### Allocation Grid (By Expense Row)')
    expense_rows = q(c, "SELECT id,title,amount,paid_by FROM expenses WHERE owner_member_id=? ORDER BY expense_date DESC, id DESC", (auth["member_id"],))
    if expense_rows.empty:
        st.info('Add at least one expense row to manage allocations.')
    else:
        expense_label_to_id = {
            f"#{int(r['id'])} | {r['title']} | {cur(float(r['amount']), s)}": int(r["id"])
            for _, r in expense_rows.iterrows()
        }
        selected_label = st.selectbox("Select Expense Row", options=list(expense_label_to_id.keys()), key="alloc_expense_selector")
        selected_expense_id = expense_label_to_id[selected_label]
        selected_expense_amount = float(expense_rows.loc[expense_rows["id"] == selected_expense_id, "amount"].iloc[0])
        selected_expense_paid_by_id = expense_rows.loc[expense_rows["id"] == selected_expense_id, "paid_by"].iloc[0]

        existing_type_row = c.execute(
            "SELECT allocation_type FROM expense_allocations WHERE expense_id=? AND allocation_type IS NOT NULL LIMIT 1",
            (selected_expense_id,),
        ).fetchone()
        default_type = existing_type_row[0] if existing_type_row and existing_type_row[0] in ["Single", "By Amount", "By Percentage"] else "By Amount"
        existing_paid_by_df = q(
            c,
            "SELECT m.name AS member FROM expense_people ep LEFT JOIN members m ON m.id=ep.member_id WHERE ep.expense_id=? AND ep.relation_type='paid_by'",
            (selected_expense_id,),
        )
        paid_by_default_name = next((name for name, mid in mm.items() if mid == selected_expense_paid_by_id), names[0] if names else "")
        existing_paid_by = [m for m in existing_paid_by_df["member"].dropna().tolist()] if not existing_paid_by_df.empty else ([paid_by_default_name] if paid_by_default_name else [])

        top1, top2 = st.columns(2)
        with top1:
            alloc_paid_by_selected = st.multiselect(
                "Paid By (multi-select)",
                options=names,
                default=[m for m in existing_paid_by if m in names],
                key="alloc_paid_by_selector",
            )
        with top2:
            alloc_type = st.selectbox(
                "Allocation Type",
                options=["Single", "By Amount", "By Percentage"],
                index=["Single", "By Amount", "By Percentage"].index(default_type),
                key="alloc_type_selector",
            )

        adf = q(
            c,
            """
            SELECT
                ea.id,
                m.name AS member,
                COALESCE(ea.allocation_type, 'By Amount') AS allocation_type,
                COALESCE(ea.allocation_value, ea.allocated_amount) AS allocation_value,
                ea.allocated_amount
            FROM expense_allocations ea
            LEFT JOIN members m ON m.id=ea.member_id
            WHERE ea.expense_id=?
            ORDER BY ea.id
            """,
            (selected_expense_id,),
        )
        if adf.empty:
            adf = pd.DataFrame(columns=['id', 'member', 'allocation_type', 'allocation_value', 'allocated_amount'])
        if adf.empty and alloc_paid_by_selected:
            # Seed defaults when expense has no allocation rows.
            if alloc_type == "Single":
                seed_members = alloc_paid_by_selected[:1]
                seed_values = [selected_expense_amount]
            elif alloc_type == "By Percentage":
                seed_members = alloc_paid_by_selected
                base_pct = round(100.0 / len(seed_members), 2)
                seed_values = [base_pct] * len(seed_members)
                seed_values[-1] = round(100.0 - sum(seed_values[:-1]), 2)
            else:
                seed_members = alloc_paid_by_selected
                base_amt = round(selected_expense_amount / len(seed_members), 2)
                seed_values = [base_amt] * len(seed_members)
                seed_values[-1] = round(selected_expense_amount - sum(seed_values[:-1]), 2)
            adf = pd.DataFrame({
                'id': [None] * len(seed_members),
                'member': seed_members,
                'allocation_type': [alloc_type] * len(seed_members),
                'allocation_value': seed_values,
                'allocated_amount': [0.0] * len(seed_members),
            })
        adf["allocation_type"] = alloc_type
        aed = st.data_editor(
            adf,
            num_rows='dynamic',
            use_container_width=True,
            key='agrid',
            column_config={
                'id': None,
                'member': st.column_config.SelectboxColumn(options=names),
                'allocation_value': st.column_config.NumberColumn(
                    "Allocation Value",
                    min_value=0.0,
                    step=1.0 if alloc_type == "By Percentage" else 100.0,
                    help="Use percentage values for By Percentage type.",
                ),
                'allocated_amount': st.column_config.NumberColumn("Allocated Amount (computed)"),
                'allocation_type': None,
            },
        )
    if st.button('Save Allocation Grid'):
        if expense_rows.empty:
            st.error("No expense row selected.")
            return
        if not alloc_paid_by_selected:
            st.error("Select at least one person in 'Paid By'.")
            return
        ex = {r[0] for r in c.execute('SELECT id FROM expense_allocations WHERE expense_id=?', (selected_expense_id,)).fetchall()}
        keep = set()
        now = datetime.now().isoformat(timespec='seconds')
        for _, r in aed.iterrows():
            rid = None if blank(r.get('id')) else int(r.get('id'))
            member = '' if blank(r.get('member')) else str(r.get('member')).strip()
            val = 0.0 if blank(r.get('allocation_value')) else float(r.get('allocation_value'))
            if rid is None and not member and val == 0.0:
                continue
            if member not in mm:
                st.error('Allocation member invalid')
                return
            if val <= 0:
                st.error('Allocation value must be > 0')
                return
            if alloc_type == "By Percentage" and val > 100:
                st.error('Percentage allocation cannot exceed 100 per row.')
                return
            if alloc_type == "Single":
                amt = selected_expense_amount
            elif alloc_type == "By Percentage":
                amt = round((val / 100.0) * selected_expense_amount, 2)
            else:
                amt = val
            member_id = mm[member]
            if rid in ex:
                c.execute(
                    'UPDATE expense_allocations SET expense_id=?, member_id=?, allocated_amount=?, allocation_type=?, allocation_value=? WHERE id=?',
                    (selected_expense_id, member_id, amt, alloc_type, val, rid),
                )
                keep.add(rid)
            else:
                inserted = c.execute(
                    'INSERT INTO expense_allocations(expense_id,member_id,allocated_amount,allocation_type,allocation_value,created_at) VALUES(?,?,?,?,?,?)',
                    (selected_expense_id, member_id, amt, alloc_type, val, now),
                )
                keep.add(inserted.lastrowid)
        if alloc_type == "Single" and len(keep) > 1:
            st.error("Single allocation type supports only one allocation row.")
            c.rollback()
            return
        if alloc_type == "By Percentage":
            total_pct = float(aed["allocation_value"].fillna(0).sum()) if "allocation_value" in aed.columns else 0.0
            if abs(total_pct - 100.0) > 0.01:
                st.error(f"Total percentage must be 100. Current total: {total_pct:.2f}.")
                c.rollback()
                return
        if alloc_type == "By Amount":
            total_amt = float(aed["allocation_value"].fillna(0).sum()) if "allocation_value" in aed.columns else 0.0
            if abs(total_amt - selected_expense_amount) > 0.01:
                st.error(f"Total amount must match expense amount {cur(selected_expense_amount, s)}. Current total: {cur(total_amt, s)}")
                c.rollback()
                return
        for did in ex - keep:
            c.execute('DELETE FROM expense_allocations WHERE id=?', (did,))
        c.execute('DELETE FROM expense_people WHERE expense_id=? AND relation_type=?', (selected_expense_id, 'paid_by'))
        for payer_name in alloc_paid_by_selected:
            c.execute(
                'INSERT INTO expense_people(expense_id,member_id,relation_type,created_at) VALUES(?,?,?,?)',
                (selected_expense_id, mm[payer_name], 'paid_by', now),
            )
        c.execute(
            "UPDATE expenses SET paid_by=? WHERE id=?",
            (
                mm[alloc_paid_by_selected[0]] if alloc_paid_by_selected else None,
                selected_expense_id,
            ),
        )
        c.commit()
        st.success('Saved')
        st.rerun()

    a = q(c, 'SELECT amount,status FROM expenses')
    if not a.empty:
        t = float(a['amount'].sum()); p = float(a[a['status'] == 'Paid']['amount'].sum()); n = float(a[a['status'] == 'Pending']['amount'].sum())
        c1, c2, c3 = st.columns(3)
        c1.metric('Total Expenditure', cur(t, s)); c2.metric('Paid', cur(p, s)); c3.metric('Pending', cur(n, s))
        st.plotly_chart(px.pie(pd.DataFrame({'Status': ['Paid', 'Pending'], 'Amount': [p, n]}), names='Status', values='Amount', hole=0.5), use_container_width=True)


def grid_plans(c, s, auth):
    st.subheader('📅 Planning')
    st.caption('Create tasks, assign to family members, and track progress.')
    if not can_view(auth, "plans"):
        st.info("You don't have access to Planning.")
        return
    mm = members_map(c)
    names = sorted(mm.keys())
    if not names:
        st.info("No families found. Create families in the Family tab first.")
        return
    cts = cats(s.get('planning_categories', ''), SETTINGS_DEFAULTS['planning_categories'])
    df = q(
        c,
        """
        SELECT
            p.id,
            p.item_type,
            p.title,
            p.due_date,
            m.name AS assigned_to,
            p.status,
            p.estimated_cost,
            p.notes,
            p.assigned_to AS assigned_to_id,
            p.owner_member_id,
            o.name AS family
        FROM plans p
        LEFT JOIN members m ON m.id=p.assigned_to
        LEFT JOIN members o ON o.id=p.owner_member_id
        WHERE p.owner_member_id=?
        ORDER BY p.due_date,p.id DESC
        """,
        (auth["member_id"],),
    )
    if df.empty:
        df = pd.DataFrame(columns=['id', 'item_type', 'title', 'due_date', 'assigned_to', 'status', 'estimated_cost', 'notes', 'family'])
    else:
        df['due_date'] = pd.to_datetime(df['due_date'], errors='coerce')

    if not can_edit(auth, "plans"):
        grid_df, _ = apply_apex_filters(df.drop(columns=["assigned_to_id", "owner_member_id"], errors="ignore"), "plans_ro", hidden_cols=["id"])
        st.dataframe(grid_df.drop(columns=["id"], errors="ignore"), use_container_width=True, hide_index=True)
        st.caption("Planning is read-only for your role.")
        return

    with st.expander("Planning Editor", expanded=True):
        st.markdown("#### Planning Grid")
        st.markdown("**Required:** <span style='color:red;'>*</span> Item Type, Title, Status", unsafe_allow_html=True)
        st.caption("Create tasks, assign family members, and track progress as you plan.")
        grid_df, filtered_mode = apply_apex_filters(df, "plans", hidden_cols=["id", "assigned_to_id", "owner_member_id"])
        col_cfg = {
            'id': None,
            'assigned_to_id': None,
            'owner_member_id': None,
            'due_date': st.column_config.DateColumn(format='YYYY-MM-DD'),
            'item_type': st.column_config.SelectboxColumn(options=cts),
            'assigned_to': st.column_config.SelectboxColumn(options=names),
            'status': st.column_config.SelectboxColumn(options=['Not Started', 'In Progress', 'Done'], help="Track stage of the task."),
        }
        col_cfg["owner"] = None
        ed = st.data_editor(grid_df, num_rows='dynamic', use_container_width=True, key='pgrid', column_config=col_cfg)
        if st.button('Save Planning Grid'):
            ed2 = ed.copy()
            ed2['due_date'] = pd.to_datetime(ed2['due_date'], errors='coerce').dt.date.astype(str).replace('NaT', None)
            ed2["owner_member_id"] = auth["member_id"]
            ok = upsert_grid(c, 'plans', ed2, ['item_type', 'title', 'due_date', 'assigned_to', 'status', 'estimated_cost', 'notes', 'owner_member_id'], [
                lambda r: 'Planning type required' if not r.get('item_type') else None,
                lambda r: 'Planning type invalid' if r.get('item_type') and r.get('item_type') not in cts else None,
                lambda r: 'Planning title required' if not r.get('title') else None,
                lambda r: 'Planning status required' if not r.get('status') else None,
                lambda r: 'Planning status invalid' if r.get('status') and r.get('status') not in ['Not Started', 'In Progress', 'Done'] else None,
                lambda r: 'Estimated cost cannot be negative' if float(r.get('estimated_cost') or 0) < 0 else None,
                lambda r: 'Assigned to invalid' if r.get('assigned_to') and r.get('assigned_to') not in mm else None,
                lambda r: 'Owner required' if not r.get('owner_member_id') else None,
            ], map_in={'assigned_to': mm}, delete_missing=not filtered_mode)
            if ok:
                st.success('Saved')
                st.rerun()


def admin_panel(c, auth):
    if not auth or not auth.get("is_admin"):
        st.info("Admin panel is admin-only.")
        return

    st.subheader("Family Admin")

    st.markdown("#### Family User Accounts")
    ua = q(
        c,
        """
        SELECT ua.id, m.name AS family, ua.username, ua.is_admin, ua.created_at, ua.last_login
        FROM user_accounts ua
        JOIN members m ON m.id=ua.member_id
        WHERE ua.member_id=?
        ORDER BY ua.username
        """,
        (auth["member_id"],),
    )
    if ua.empty:
        ua = pd.DataFrame(columns=["id", "family", "username", "is_admin", "created_at", "last_login"])

    family_has_admin = bool((ua["is_admin"] == 1).any()) if not ua.empty else False

    membership = ua[["username", "is_admin", "created_at", "last_login"]].copy()
    membership["role"] = membership["is_admin"].apply(lambda v: "Admin" if int(v) == 1 else "Member")
    membership["created_at"] = pd.to_datetime(membership["created_at"], errors="coerce")
    membership["last_login"] = pd.to_datetime(membership["last_login"], errors="coerce")
    membership = membership[["username", "role", "created_at", "last_login"]]

    st.markdown("#### Family Membership")
    st.table(membership)

    st.markdown("#### Family Accounts")
    st.dataframe(ua.drop(columns=["id"], errors="ignore"), use_container_width=True, hide_index=True)

    missing = q(
        c,
        """
        SELECT m.id AS member_id, m.name AS member
        FROM members m
        LEFT JOIN user_accounts ua ON ua.member_id = m.id
        WHERE ua.id IS NULL
          AND m.id = ?
        ORDER BY m.name
        """,
        (auth["member_id"],),
    )
    if missing.empty:
        st.caption("All family accounts are created.")
    else:
        st.dataframe(missing.drop(columns=["member_id"], errors="ignore"), use_container_width=True, hide_index=True)
        with st.form("bulk_create_accounts"):
            pw_len = st.number_input("Temporary password length", min_value=8, max_value=32, value=12, step=1)
            ok_bulk = st.form_submit_button("Generate Accounts For Missing Family Members")
        if ok_bulk:
            now = datetime.now().isoformat(timespec="seconds")
            created_rows = []
            for _, r in missing.iterrows():
                member_id = int(r["member_id"])
                member_name = str(r["member"])
                username = _unique_username(c, member_name)
                temp_pw = _random_temp_password(int(pw_len))
                pwh = hash_password(temp_pw)
                try:
                    c.execute(
                        "INSERT INTO user_accounts(member_id,username,password_hash,is_admin,created_at) VALUES(?,?,?,?,?)",
                        (member_id, username, pwh, 0, now),
                    )
                    created_rows.append({"member": member_name, "username": username, "temp_password": temp_pw})
                except sqlite3.IntegrityError:
                    continue
            c.commit()
            if created_rows:
                st.session_state["generated_creds"] = created_rows
                st.warning("Temporary passwords are shown once per generation. Store them now and have members change them.")
                out_df = pd.DataFrame(created_rows)
                st.dataframe(out_df, use_container_width=True, hide_index=True)
                st.download_button(
                    "Download CSV",
                    data=out_df.to_csv(index=False).encode("utf-8"),
                    file_name="member_credentials.csv",
                    mime="text/csv",
                )
                st.rerun()
            else:
                st.info("No accounts created.")

    family_name = auth.get("member_name")
    default_admin = not family_has_admin
    with st.form("ua_form"):
        st.markdown(f"**Family:** {family_name}")
        username = st.text_input("Login username", key="ua_username")
        password = st.text_input("Set password (required for new accounts)", type="password", key="ua_password")
        is_admin = st.checkbox(
            "Family admin",
            value=default_admin,
            help="The first account created for a family is automatically a Family admin.",
            key="ua_is_admin",
        )
        ok = st.form_submit_button("Create/Update Account")
    if ok:
        username = (username or "").strip()
        if not username:
            st.error("Login username is required.")
            return
        now = datetime.now().isoformat(timespec="seconds")
        existing = c.execute("SELECT id, member_id, is_admin FROM user_accounts WHERE username=?", (username,)).fetchone()

        if not family_has_admin:
            is_admin = True

        try:
            if existing:
                existing_id, existing_member_id, existing_is_admin = existing
                if existing_member_id != auth["member_id"]:
                    st.error("Username already exists in another family.")
                    return

                # Prevent removing admin from last family admin.
                if existing_is_admin == 1 and not is_admin and (ua["is_admin"].sum() <= 1):
                    st.error("Cannot remove admin privileges from the last family admin.")
                    return

                set_pw = (password or "").strip() != ""
                if set_pw:
                    pwh = hash_password(password)
                    c.execute(
                        "UPDATE user_accounts SET password_hash=?, is_admin=? WHERE id=?",
                        (pwh, 1 if is_admin else 0, int(existing_id)),
                    )
                else:
                    c.execute(
                        "UPDATE user_accounts SET is_admin=? WHERE id=?",
                        (1 if is_admin else 0, int(existing_id)),
                    )
            else:
                if not (password or "").strip():
                    st.error("Password is required when creating a new account.")
                    return
                pwh = hash_password(password)
                c.execute(
                    "INSERT INTO user_accounts(member_id,username,password_hash,is_admin,created_at,last_login) VALUES(?,?,?,?,?,?)",
                    (auth["member_id"], username.strip(), pwh, 1 if is_admin else 0, now, None),
                )
            c.commit()
        except sqlite3.IntegrityError:
            st.error("Username already exists.")
            return
        except ValueError as e:
            st.error(str(e))
            return
        st.success("Saved user account.")
        st.rerun()

    st.markdown("#### Access Model")
    st.caption("All family members can create/edit/delete records for their family. Family admins can promote another account to Family admin.")


def render_settings(c, s, auth):
    st.subheader('⚙️ Settings')
    st.caption('Global application settings. Only admin users can change these.')
    if not auth or not auth.get("is_admin"):
        st.info("Settings are admin-only.")
        st.caption(f"Current: {s['currency_symbol']} | {s['currency_code']} | {s['timezone']} | {s['date_format']}")
        return s
    with st.form('sform'):
        c1, c2 = st.columns(2)
        with c1:
            user = st.text_input('Your Name', value=s['user_name'])
            partner = st.text_input('Partner Name', value=s['partner_name'])
            sym = st.text_input('Currency Symbol', value=s['currency_symbol'])
            exp = st.text_area('Expense Categories (comma-separated)', value=s['expense_categories'])
        with c2:
            code = st.text_input('Currency Code', value=s['currency_code'])
            tz = st.text_input('Timezone', value=s['timezone'])
            dfmt = st.selectbox('Date Format', options=['%d-%b-%Y', '%Y-%m-%d', '%d/%m/%Y', '%m/%d/%Y'], index=['%d-%b-%Y', '%Y-%m-%d', '%d/%m/%Y', '%m/%d/%Y'].index(s['date_format']) if s['date_format'] in ['%d-%b-%Y', '%Y-%m-%d', '%d/%m/%Y', '%m/%d/%Y'] else 0)
            plc = st.text_area('Planning Categories (comma-separated)', value=s['planning_categories'])
        ok = st.form_submit_button('Save Settings')
    if ok:
        try: ZoneInfo(tz.strip() or SETTINGS_DEFAULTS['timezone'])
        except ZoneInfoNotFoundError:
            st.error('Invalid timezone'); return s
        ns = {
            'user_name': user.strip() or SETTINGS_DEFAULTS['user_name'],
            'partner_name': partner.strip() or SETTINGS_DEFAULTS['partner_name'],
            'currency_symbol': sym.strip() or SETTINGS_DEFAULTS['currency_symbol'],
            'currency_code': (code.strip() or SETTINGS_DEFAULTS['currency_code']).upper(),
            'timezone': tz.strip() or SETTINGS_DEFAULTS['timezone'],
            'date_format': dfmt,
            'expense_categories': ','.join(cats(exp, SETTINGS_DEFAULTS['expense_categories'])),
            'planning_categories': ','.join(cats(plc, SETTINGS_DEFAULTS['planning_categories']))
        }
        save_settings(c, ns); st.success('Saved'); s = settings(c)
    st.caption(f"Current: {s['currency_symbol']} | {s['currency_code']} | {s['timezone']} | {s['date_format']}")
    if st.checkbox('I understand reset deletes budgets, expenses, plans, members') and st.button('Reset All Data', type='secondary'):
        c.execute('DELETE FROM expense_allocations')
        c.execute('DELETE FROM expenses')
        c.execute('DELETE FROM plans')
        c.execute('DELETE FROM budget_items')
        c.execute('DELETE FROM members')
        c.commit()
        seed(c)
        auth_clear()
        st.success('Reset done')
        st.rerun()
    return s


def main():
    st.markdown('<style>.stApp{background:radial-gradient(circle at top left,#fff4e6 0%,#ffe8d6 35%,#ffd7ba 100%);}</style>', unsafe_allow_html=True)
    c = db(); init(c); seed(c); s = settings(c)
    auth = login_sidebar(c)
    header(s)
    if not auth:
        st.info("Login required to view data.")
        c.close()
        return

    tabs = ['Overview']
    if auth.get("is_admin") or auth.get("is_global_admin"):
        tabs.append('Family')
    if can_view(auth, "budget"):
        tabs.append('Budget')
    if can_view(auth, "expenses"):
        tabs.append('Expenses')
    if can_view(auth, "plans"):
        tabs.append('Planning')
    if auth.get("is_admin") or auth.get("is_global_admin"):
        tabs.append('Settings')

    # Allow linking to a specific tab via query param (e.g., ?tab=Expenses)
    # Use whichever query-params API is available for the Streamlit version.
    params = st.experimental_get_query_params() if hasattr(st, "experimental_get_query_params") else getattr(st, "query_params", {})
    sel_tab = params.get('tab', [None])[0] if isinstance(params, dict) else None
    if sel_tab in tabs:
        # move the selected tab to the front so it becomes active
        tabs.insert(0, tabs.pop(tabs.index(sel_tab)))

    t = st.tabs(tabs)
    for i, name in enumerate(tabs):
        with t[i]:
            if name == 'Overview':
                overview(c, s, auth)
            elif name == 'Family':
                grid_family(c, auth)
            elif name == 'Budget':
                grid_budget(c, s, auth)
            elif name == 'Expenses':
                grid_expenses(c, s, auth)
            elif name == 'Planning':
                grid_plans(c, s, auth)
            elif name == 'Settings':
                s = render_settings(c, s, auth)
    c.close()


if __name__ == '__main__':
    main()
