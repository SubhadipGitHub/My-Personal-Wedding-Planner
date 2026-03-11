# My Personal Wedding Planner (Streamlit)

An interactive, local-first wedding planning app built with Streamlit + SQLite.

## Key Features

- ✅ User accounts + login (admin + member access)
- ✅ Password reset flow with expiring reset tokens
- ✅ Last login timestamp recorded per user
- 🧑‍🤝‍🧑 Multi-family support (each “family” is a separate data scope)
- 🔐 Role-based access controls (global admin, family admin, member)
- Oracle APEX-style interactive grids (no separate add forms) for:
  - People
  - Budget
  - Expenses
  - Expense Allocations
  - Planning
- APEX-style filtering in every grid:
  - global row search across all columns
  - per-column filters by type (text values, number ranges, date ranges)
  - filters apply directly to the same editable grid (single-grid workflow)
- Unified dashboard for your wedding planning progress
- Overview multi-member filter for grouped expense tracking
- Member-wise budget assignment with share against total budget
- Pending expense projection chart from current month through next 12 months
- Pending expense timeline by due month with month-level filter in Overview
- Overall budget planning by category (venue, dress, cake, decor, etc.)
- Expense tracking by person:
  - self
  - partner
  - family members
- Expense details with:
  - paid/pending status
  - due date for pending planning
  - bill/document links
  - email reference notes
- Custom expense allocation:
  - select an expense row and allocate to members
  - split standards supported: `By amount` or `By percentage`
  - validations ensure:
    - amount split total equals expense amount
    - percentage split total equals 100%
    - required fields and minimum input quality checks
- Expense split analytics:
  - total expenditure
  - paid vs pending split
  - person-wise paid/pending split by payer
  - person-wise paid/pending split by allocation beneficiary
- Record management:
  - add/edit/delete rows directly in each grid
  - save changes per-grid
- Planning tracker for dresses, cakes, events, vendors, and tasks
- App settings tab for:
  - currency symbol/code
  - timezone
  - date format
  - couple names
  - configurable expense categories
  - configurable planning categories
  - one-click reset of all non-settings data
- Local SQLite database storage (`wedding_planner.db`)

## Tech Stack

- Python
- Streamlit
- SQLite (via `sqlite3`)
- Pandas
- Plotly

## Project Files

- `app.py` → main Streamlit app
- `requirements.txt` → Python dependencies
- `wedding_planner.db` → local database (auto-created on first run)

## Run Locally (Windows PowerShell)

1. Go to project directory:

```powershell
cd d:\Github\My-Personal-Wedding-Planner
```

2. Create virtual environment:

```powershell
python -m venv .venv
```

3. Activate virtual environment:

```powershell
.\.venv\Scripts\Activate.ps1
```

4. Install dependencies:

```powershell
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

5. Run the app:

```powershell
python -m streamlit run app.py
```

6. Open the local URL shown in terminal (usually `http://localhost:8501`).

---

## Deployment Notes (Optional)

This is a local-first app; to deploy publicly you can use any Streamlit hosting service or containerized platform.

### Quick deploy options

- **Streamlit Community Cloud**: connect this repo and deploy (requires a GitHub repo).
- **Docker**: build a container that runs `streamlit run app.py` and mounts a persistent volume for `wedding_planner.db`.
- **Other PaaS** (Heroku / Azure / AWS): ensure the app has write access for the SQLite file and use an external persistent storage volume.

---

## How to Use the App

### 1) Login & accounts

- On first launch, the app prompts you to create the **admin account** (`admin` / `Welcome@12345`).
- Admins can create additional family members and grant them access.
- Each user can log in with their username and password.
- The app records **last login time** for every login.
- Password reset is supported via a token that expires after 1 hour.

### 2) Create Families (Members)

- Families are managed in the **People** tab.
- Each “family” is a separate group of budget/expenses/plans.
- Global admins can manage all families; family admins can manage only their own family.

### 3) Budget

- Add categories and amounts in the **Budget** tab.
- Assign budgets to specific family members.
- Budget vs. spent metrics and charts are visible in the **Overview** tab.

### 4) Expenses

- Use the **Expenses** tab to log expenses.
- Track who paid, status (Paid/Pending), due dates, bill links, and notes.
- Expenses can be split across members and tracked in the **Allocations** section.

### 5) Planning

- Use the **Planning** tab for tasks, vendors, deliveries, event bookings, etc.
- Set due dates, assign to members, and track status.

### 6) Overview Dashboard

- Review overall budget vs actuals, expense trends, and pending items.
- Filter by family and see per-member breakdowns.

### 7) Settings

- Customize:
  - Currency symbol/code
  - Timezone
  - Date format
  - Couple names
  - Expense & planning categories
- Use **Reset All Data** to clear budgets, expenses, plans, and members while keeping settings.

---

## Notes

- Your data is stored locally in `wedding_planner.db`.
- Keep this file if you want to preserve your plans and finance records.
- Delete this file only if you want a fresh start.
