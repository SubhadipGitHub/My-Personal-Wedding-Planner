# My Personal Wedding Planner (Streamlit)

An interactive, local-first wedding planning app built with Streamlit + SQLite.

## Features

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

- `app.py` -> main Streamlit app
- `requirements.txt` -> Python dependencies
- `wedding_planner.db` -> local database (auto-created on first run)

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

## How to Use the App

1. Open the app and start in the `People` tab.
   Add yourself, your partner, and family members who will pay or manage tasks.

2. Go to the `Budget` tab.
   Add budget categories with allocated amounts. Categories are strictly taken from `Settings`.

3. Use the `Expenses` tab for every payment.
   Add title, category, amount, payment status, and optional bill/email references.
   Use multi-select `Paid By` in the interactive grid.
   Manage splits in the Allocation section by selecting an expense row and allocation type.
   Review split widgets to track total expenditure and paid vs pending percentages.

4. Use `Planning` to manage work items.
   Add dresses, cake bookings, event tasks, vendor follow-ups, due dates, assignees, and status.

5. Monitor the `Overview` tab regularly.
   Check total budget vs spent, category-wise spend charts, and current paid/pending totals.

6. Configure the `Settings` tab.
   Set currency, timezone, date format, names, and your category lists to personalize the planner.
   Use `Reset All Data` if you want to clear budgets, expenses, plans, and members while keeping settings.

7. Update records continuously.
   Your entries are saved automatically in `wedding_planner.db`, so reopen the app anytime and continue.

## If `streamlit` command is not recognized

Use module mode (works reliably inside venv):

```powershell
python -m streamlit run app.py
```

## If PowerShell blocks venv activation

Run this once in the same terminal session, then activate again:

```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
```

## Notes

- Your data is stored locally in `wedding_planner.db`.
- Keep this file if you want to preserve your plans and finance records.
- Delete this file only if you want a fresh start.
