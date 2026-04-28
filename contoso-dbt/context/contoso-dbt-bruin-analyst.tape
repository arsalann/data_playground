Output contoso-dbt/context/contoso-dbt-bruin-analyst.gif

Set Shell "zsh"
Set FontSize 14
Set Width 1024
Set Height 1024
Set Padding 24
Set Margin 12
Set MarginFill "#FF5569"
Set BorderRadius 0
Set TypingSpeed 30ms
Set PlaybackSpeed 1.33

Hide
Type "rm -rf /tmp/contoso-demo && mkdir -p /tmp/contoso-demo && rsync -a --exclude=context --exclude=.venv --exclude=target --exclude=logs --exclude=__pycache__ --exclude=dbt_packages --exclude='*.pyc' /Users/bear/conductor/workspaces/data_playground/paris/contoso-dbt/ /tmp/contoso-demo/contoso-dbt/" Enter
Sleep 1500ms
Type "cd /tmp/contoso-demo/contoso-dbt" Enter
Type "export PS1='%F{cyan}contoso-dbt%f $ '" Enter
Type "clear" Enter
Show

# 1. Show the dbt project structure.
Type "tree -L 2 -I '__pycache__'" Sleep 250ms Enter
Sleep 3500ms

# 2. Create the context folder and bruin init empty.
Type "mkdir context && cd context" Sleep 250ms Enter
Sleep 700ms

Type "bruin init empty --in-place ." Sleep 250ms Enter
Sleep 3000ms

# 3. Show what bruin import database WOULD look like (typed only, then cleared).
Type "bruin import database --connection contoso_dbt_bq --schemas contoso_dbt_raw --schemas contoso_dbt_staging --schemas contoso_dbt_reports ." Sleep 1500ms
Ctrl+u
Sleep 400ms

# 4. Show ai enhance (typed only, then cleared).
Type "bruin ai enhance --claude assets/" Sleep 1500ms
Ctrl+u
Sleep 400ms

# 5. (Hidden) Drop the real, AI-enhanced context over the empty skeleton —
#    excluding the demo's own .tape/.gif files so it doesn't show its own output.
Hide
Type "rm -rf /tmp/contoso-demo/contoso-dbt/context && rsync -a --exclude='contoso-dbt-bruin-analyst*' --exclude='ai-data-analyst*' /Users/bear/conductor/workspaces/data_playground/paris/contoso-dbt/context/ /tmp/contoso-demo/contoso-dbt/context/ && cd /tmp/contoso-demo/contoso-dbt/context" Enter
Type "clear" Enter
Show

# 6. Show the populated context tree (3 levels deep).
Type "tree -L 3" Sleep 250ms Enter
Sleep 4s

# 7. Hand off to Claude as the data analyst.
Type "cd /tmp/contoso-demo/contoso-dbt" Sleep 200ms Enter
Sleep 500ms

Type "claude --dangerously-skip-permissions" Sleep 250ms Enter
# Wait for the workspace-trust prompt to render, then accept (Yes is default).
Sleep 5s
Enter
# Wait for Claude's main UI to be ready for input.
Sleep 6s

Type "You are a data analyst for Contoso. Follow AGENTS.md, then answer: give me a breakdown of marketing channels and their total conversion rates for the past 2 years."
Sleep 800ms
Enter
Sleep 72s
