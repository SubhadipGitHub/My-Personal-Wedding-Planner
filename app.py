import sqlite3
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import pandas as pd
import plotly.express as px
import streamlit as st

DB_PATH = Path('wedding_planner.db')
SETTINGS_DEFAULTS = {
    'currency_symbol': 'Rs ', 'currency_code': 'INR', 'timezone': 'Asia/Kolkata',
    'date_format': '%d-%b-%Y', 'user_name': 'Me', 'partner_name': 'Partner',
    'expense_categories': 'Venue,Dress,Cake,Decor,Photography,Travel,Jewelry,Invitation,Catering,Other',
    'planning_categories': 'Dress,Cake,Event,Vendor,Decoration,Guest Task,Other'
}
st.set_page_config(page_title='Wedding Planner', page_icon=':ring:', layout='wide')


def db():
    c = sqlite3.connect(DB_PATH)
    c.execute('PRAGMA foreign_keys = ON')
    return c


def init(c):
    c.executescript('''
    CREATE TABLE IF NOT EXISTS settings(setting_key TEXT PRIMARY KEY, setting_value TEXT NOT NULL);
    CREATE TABLE IF NOT EXISTS members(id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL UNIQUE, role TEXT, created_at TEXT NOT NULL);
    CREATE TABLE IF NOT EXISTS budget_items(id INTEGER PRIMARY KEY AUTOINCREMENT, category TEXT NOT NULL, allocated_amount REAL NOT NULL, notes TEXT, created_at TEXT NOT NULL);
    CREATE TABLE IF NOT EXISTS expenses(id INTEGER PRIMARY KEY AUTOINCREMENT, expense_date TEXT NOT NULL, title TEXT NOT NULL, category TEXT NOT NULL, amount REAL NOT NULL, paid_by INTEGER, for_whom INTEGER, status TEXT NOT NULL, bill_link TEXT, email_ref TEXT, notes TEXT, created_at TEXT NOT NULL, FOREIGN KEY(paid_by) REFERENCES members(id) ON DELETE SET NULL, FOREIGN KEY(for_whom) REFERENCES members(id) ON DELETE SET NULL);
    CREATE TABLE IF NOT EXISTS expense_allocations(id INTEGER PRIMARY KEY AUTOINCREMENT, expense_id INTEGER NOT NULL, member_id INTEGER, allocated_amount REAL NOT NULL, created_at TEXT NOT NULL, FOREIGN KEY(expense_id) REFERENCES expenses(id) ON DELETE CASCADE, FOREIGN KEY(member_id) REFERENCES members(id) ON DELETE SET NULL);
    CREATE TABLE IF NOT EXISTS expense_people(id INTEGER PRIMARY KEY AUTOINCREMENT, expense_id INTEGER NOT NULL, member_id INTEGER NOT NULL, relation_type TEXT NOT NULL, created_at TEXT NOT NULL, FOREIGN KEY(expense_id) REFERENCES expenses(id) ON DELETE CASCADE, FOREIGN KEY(member_id) REFERENCES members(id) ON DELETE CASCADE);
    CREATE TABLE IF NOT EXISTS plans(id INTEGER PRIMARY KEY AUTOINCREMENT, item_type TEXT NOT NULL, title TEXT NOT NULL, due_date TEXT, assigned_to INTEGER, status TEXT NOT NULL, estimated_cost REAL, notes TEXT, created_at TEXT NOT NULL, FOREIGN KEY(assigned_to) REFERENCES members(id) ON DELETE SET NULL);
    ''')
    # lightweight schema migrations
    cols = {r[1] for r in c.execute("PRAGMA table_info(expense_allocations)").fetchall()}
    if "allocation_type" not in cols:
        c.execute("ALTER TABLE expense_allocations ADD COLUMN allocation_type TEXT")
    if "allocation_value" not in cols:
        c.execute("ALTER TABLE expense_allocations ADD COLUMN allocation_value REAL")
    c.execute("UPDATE expense_allocations SET allocation_type='By Amount' WHERE allocation_type IS NULL OR TRIM(allocation_type)=''")
    c.execute("UPDATE expense_allocations SET allocation_value=allocated_amount WHERE allocation_value IS NULL")
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
    c.commit()


def seed(c):
    now = datetime.now().isoformat(timespec='seconds')
    if c.execute('SELECT COUNT(*) FROM members').fetchone()[0] == 0:
        c.executemany('INSERT INTO members(name,role,created_at) VALUES(?,?,?)', [('Me','Self',now),('Partner','Partner',now),('Family','Family',now)])
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


def apply_apex_filters(df, key_prefix, hidden_cols=None):
    hidden_cols = hidden_cols or []
    if df.empty:
        return df, False
    cols = [c for c in df.columns if c not in hidden_cols]
    with st.expander("Filters (APEX Style)", expanded=False):
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
    st.markdown(f"### Wedding Planning Command Center\n{s['user_name']} & {s['partner_name']} | {s['timezone']} | {s['currency_code']}")


def overview(c, s):
    e = q(c, 'SELECT amount,status,category FROM expenses')
    b = q(c, 'SELECT category,allocated_amount FROM budget_items')
    tb = float(b['allocated_amount'].sum()) if not b.empty else 0
    te = float(e['amount'].sum()) if not e.empty else 0
    pdv = float(e[e['status'] == 'Paid']['amount'].sum()) if not e.empty else 0
    pnd = float(e[e['status'] == 'Pending']['amount'].sum()) if not e.empty else 0
    c1, c2, c3, c4 = st.columns(4)
    c1.metric('Total Budget', cur(tb, s)); c2.metric('Total Expenses', cur(te, s)); c3.metric('Paid', cur(pdv, s)); c4.metric('Pending', cur(pnd, s))
    if not e.empty:
        st.plotly_chart(px.bar(e.groupby('category', as_index=False)['amount'].sum(), x='category', y='amount'), use_container_width=True)


def grid_members(c):
    st.subheader('People Grid')
    df = q(c, 'SELECT id,name,role FROM members ORDER BY name')
    if df.empty: df = pd.DataFrame(columns=['id', 'name', 'role'])
    grid_df, filtered_mode = apply_apex_filters(df, "people", hidden_cols=["id"])
    ed = st.data_editor(
        grid_df,
        num_rows='dynamic',
        use_container_width=True,
        key='mgrid',
        column_config={'id': None},
    )
    if st.button('Save People Grid'):
        ok = upsert_grid(
            c,
            'members',
            ed,
            ['name', 'role'],
            [lambda r: 'Name required' if not r.get('name') else None],
            delete_missing=not filtered_mode,
        )
        if ok: st.success('Saved'); st.rerun()


def grid_budget(c, s):
    st.subheader('Budget Grid')
    cts = cats(s.get('expense_categories', ''), SETTINGS_DEFAULTS['expense_categories'])
    df = q(c, 'SELECT id,category,allocated_amount,notes FROM budget_items ORDER BY id DESC')
    if df.empty: df = pd.DataFrame(columns=['id', 'category', 'allocated_amount', 'notes'])
    grid_df, filtered_mode = apply_apex_filters(df, "budget", hidden_cols=["id"])
    ed = st.data_editor(
        grid_df,
        num_rows='dynamic',
        use_container_width=True,
        key='bgrid',
        column_config={'id': None, 'category': st.column_config.SelectboxColumn(options=cts)},
    )
    if st.button('Save Budget Grid'):
        ok = upsert_grid(c, 'budget_items', ed, ['category', 'allocated_amount', 'notes'], [
            lambda r: 'Budget category invalid' if r.get('category') not in cts else None,
            lambda r: 'Allocated amount must be > 0' if float(r.get('allocated_amount') or 0) <= 0 else None,
        ], delete_missing=not filtered_mode)
        if ok: st.success('Saved'); st.rerun()


def grid_expenses(c, s):
    st.subheader('Expenses Grid')
    mm = members_map(c)
    names = sorted(mm.keys())
    id_to_name = {v: k for k, v in mm.items()}
    cts = cats(s.get('expense_categories', ''), SETTINGS_DEFAULTS['expense_categories'])
    df = q(c, '''SELECT id,expense_date,title,category,amount,paid_by,status,for_whom,bill_link,email_ref,notes FROM expenses ORDER BY expense_date DESC,id DESC''')
    if df.empty:
        df = pd.DataFrame(columns=['id', 'expense_date', 'title', 'category', 'amount', 'paid_by', 'status', 'for_whom', 'bill_link', 'email_ref', 'notes'])
    else:
        participants = q(
            c,
            '''
            SELECT ep.expense_id, ep.relation_type, m.name
            FROM expense_people ep
            LEFT JOIN members m ON m.id = ep.member_id
            ORDER BY ep.id
            '''
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
        df["for_whom"] = df.apply(
            lambda r: whom_map.get(int(r["id"]), [id_to_name[r["for_whom"]]] if pd.notna(r["for_whom"]) and r["for_whom"] in id_to_name else []),
            axis=1,
        )
        df['expense_date'] = pd.to_datetime(df['expense_date'], errors='coerce')
    grid_df, filtered_mode = apply_apex_filters(df, "expenses", hidden_cols=["id"])
    ed = st.data_editor(grid_df, num_rows='dynamic', use_container_width=True, key='egrid', column_config={
        'id': None,
        'expense_date': st.column_config.DateColumn(format='YYYY-MM-DD'),
        'category': st.column_config.SelectboxColumn(options=cts),
        'paid_by': st.column_config.MultiselectColumn(options=names),
        'status': st.column_config.SelectboxColumn(options=['Paid', 'Pending']),
        'for_whom': st.column_config.MultiselectColumn(options=names),
    })
    def v1(r): return 'Expense title required' if not r.get('title') else None
    def v2(r): return 'Expense category invalid' if r.get('category') not in cts else None
    def v3(r): return 'Expense amount must be > 0' if float(r.get('amount') or 0) <= 0 else None
    def v4(r):
        vals = r.get('paid_by') if isinstance(r.get('paid_by'), list) else ([r.get('paid_by')] if r.get('paid_by') else [])
        if not vals:
            return 'Paid By requires at least one person'
        for v in vals:
            if v not in mm:
                return 'Paid By invalid'
        return None
    def v5(r): return 'Status invalid' if r.get('status') not in ['Paid', 'Pending'] else None
    def v6(r):
        if blank(r.get('expense_date')): return 'Expense date required'
        if pd.isna(pd.to_datetime(r.get('expense_date'), errors='coerce')): return 'Expense date invalid'
        return None
    def v7(r):
        vals = r.get('for_whom') if isinstance(r.get('for_whom'), list) else ([r.get('for_whom')] if r.get('for_whom') else [])
        for v in vals:
            if v not in mm:
                return 'For Whom invalid'
        return None
    if st.button('Save Expenses Grid'):
        ed2 = ed.copy()
        ed2['expense_date'] = pd.to_datetime(ed2['expense_date'], errors='coerce').dt.date.astype(str)
        ex = {r[0] for r in c.execute('SELECT id FROM expenses').fetchall()}
        keep = set()
        now = datetime.now().isoformat(timespec='seconds')
        for _, r in ed2.iterrows():
            rid = None if blank(r.get('id')) else int(r.get('id'))
            row = {
                'expense_date': r.get('expense_date'),
                'title': '' if blank(r.get('title')) else str(r.get('title')).strip(),
                'category': '' if blank(r.get('category')) else str(r.get('category')).strip(),
                'amount': 0.0 if blank(r.get('amount')) else float(r.get('amount')),
                'paid_by': r.get('paid_by') if isinstance(r.get('paid_by'), list) else ([] if blank(r.get('paid_by')) else [r.get('paid_by')]),
                'for_whom': r.get('for_whom') if isinstance(r.get('for_whom'), list) else ([] if blank(r.get('for_whom')) else [r.get('for_whom')]),
                'status': '' if blank(r.get('status')) else str(r.get('status')).strip(),
                'bill_link': '' if blank(r.get('bill_link')) else str(r.get('bill_link')).strip(),
                'email_ref': '' if blank(r.get('email_ref')) else str(r.get('email_ref')).strip(),
                'notes': '' if blank(r.get('notes')) else str(r.get('notes')).strip(),
            }
            if all([blank(row['expense_date']), not row['title'], not row['category'], row['amount'] == 0.0, not row['paid_by'], not row['for_whom'], not row['status']]):
                if rid is None:
                    continue
            for fn in [v1, v2, v3, v4, v5, v6, v7]:
                err = fn(row)
                if err:
                    st.error(err)
                    return
            paid_ids = [mm[v] for v in row['paid_by'] if v in mm]
            whom_ids = [mm[v] for v in row['for_whom'] if v in mm]
            paid_primary = paid_ids[0] if paid_ids else None
            whom_primary = whom_ids[0] if whom_ids else None
            if rid in ex:
                c.execute(
                    """
                    UPDATE expenses
                    SET expense_date=?, title=?, category=?, amount=?, paid_by=?, for_whom=?, status=?, bill_link=?, email_ref=?, notes=?
                    WHERE id=?
                    """,
                    (row['expense_date'], row['title'], row['category'], row['amount'], paid_primary, whom_primary, row['status'], row['bill_link'], row['email_ref'], row['notes'], rid),
                )
                expense_id = rid
                keep.add(rid)
            else:
                inserted = c.execute(
                    """
                    INSERT INTO expenses(expense_date,title,category,amount,paid_by,for_whom,status,bill_link,email_ref,notes,created_at)
                    VALUES(?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (row['expense_date'], row['title'], row['category'], row['amount'], paid_primary, whom_primary, row['status'], row['bill_link'], row['email_ref'], row['notes'], now),
                )
                expense_id = inserted.lastrowid
                keep.add(expense_id)
            c.execute('DELETE FROM expense_people WHERE expense_id=?', (expense_id,))
            for pid in paid_ids:
                c.execute('INSERT INTO expense_people(expense_id,member_id,relation_type,created_at) VALUES(?,?,?,?)', (expense_id, pid, 'paid_by', now))
            for wid in whom_ids:
                c.execute('INSERT INTO expense_people(expense_id,member_id,relation_type,created_at) VALUES(?,?,?,?)', (expense_id, wid, 'for_whom', now))
        if not filtered_mode:
            for did in ex - keep:
                c.execute('DELETE FROM expenses WHERE id=?', (did,))
        c.commit()
        st.success('Saved')
        st.rerun()

    st.markdown('#### Allocation Grid (By Expense Row)')
    expense_rows = q(c, "SELECT id,title,amount,paid_by FROM expenses ORDER BY expense_date DESC, id DESC")
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


def grid_plans(c, s):
    st.subheader('Planning Grid')
    mm = members_map(c); names = sorted(mm.keys()); cts = cats(s.get('planning_categories', ''), SETTINGS_DEFAULTS['planning_categories'])
    df = q(c, '''SELECT p.id,p.item_type,p.title,p.due_date,m.name AS assigned_to,p.status,p.estimated_cost,p.notes FROM plans p LEFT JOIN members m ON m.id=p.assigned_to ORDER BY p.due_date,p.id DESC''')
    if df.empty: df = pd.DataFrame(columns=['id', 'item_type', 'title', 'due_date', 'assigned_to', 'status', 'estimated_cost', 'notes'])
    else:
        df['due_date'] = pd.to_datetime(df['due_date'], errors='coerce')
    grid_df, filtered_mode = apply_apex_filters(df, "plans", hidden_cols=["id"])
    ed = st.data_editor(grid_df, num_rows='dynamic', use_container_width=True, key='pgrid', column_config={
        'id': None,
        'due_date': st.column_config.DateColumn(format='YYYY-MM-DD'),
        'item_type': st.column_config.SelectboxColumn(options=cts),
        'assigned_to': st.column_config.SelectboxColumn(options=names),
        'status': st.column_config.SelectboxColumn(options=['Not Started', 'In Progress', 'Done'])
    })
    if st.button('Save Planning Grid'):
        ed2 = ed.copy(); ed2['due_date'] = pd.to_datetime(ed2['due_date'], errors='coerce').dt.date.astype(str).replace('NaT', None)
        ok = upsert_grid(c, 'plans', ed2, ['item_type', 'title', 'due_date', 'assigned_to', 'status', 'estimated_cost', 'notes'], [
            lambda r: 'Planning type invalid' if r.get('item_type') not in cts else None,
            lambda r: 'Planning title required' if not r.get('title') else None,
            lambda r: 'Planning status invalid' if r.get('status') not in ['Not Started', 'In Progress', 'Done'] else None,
            lambda r: 'Estimated cost cannot be negative' if float(r.get('estimated_cost') or 0) < 0 else None,
            lambda r: 'Assigned to invalid' if r.get('assigned_to') and r.get('assigned_to') not in mm else None,
        ], map_in={'assigned_to': mm}, delete_missing=not filtered_mode)
        if ok: st.success('Saved'); st.rerun()


def render_settings(c, s):
    st.subheader('Settings')
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
        c.execute('DELETE FROM expense_allocations'); c.execute('DELETE FROM expenses'); c.execute('DELETE FROM plans'); c.execute('DELETE FROM budget_items'); c.execute('DELETE FROM members'); c.commit(); seed(c); st.success('Reset done'); st.rerun()
    return s


def main():
    st.markdown('<style>.stApp{background:radial-gradient(circle at top left,#fff4e6 0%,#ffe8d6 35%,#ffd7ba 100%);}</style>', unsafe_allow_html=True)
    c = db(); init(c); seed(c); s = settings(c)
    header(s)
    t = st.tabs(['Overview', 'People', 'Budget', 'Expenses', 'Planning', 'Settings'])
    with t[0]: overview(c, s)
    with t[1]: grid_members(c)
    with t[2]: grid_budget(c, s)
    with t[3]: grid_expenses(c, s)
    with t[4]: grid_plans(c, s)
    with t[5]: s = render_settings(c, s)
    c.close()


if __name__ == '__main__':
    main()
