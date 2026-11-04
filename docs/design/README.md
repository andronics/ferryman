# Design records

Why the code is the way it is. Copy [0000-template.md](0000-template.md) to `NNNN-short-title.md` before a
new feature, a new dependency, or a public-interface change, and add a line below. Status moves Proposed →
Approved (by a human) → Implemented; supersede an implemented record with a new one, never rewrite it.

**Index:** read this first, then open only the records relevant to your task.

| # | Title | Status | Decision in one line |
|---|---|---|---|
| [0000](0000-template.md) | Template for new records | n/a | Copy it; Context, Decision, Alternatives considered, Consequences, Open questions |
| [0001](0001-migration-to-the-templates.md) | Migration to the templates | Approved | Release `next` as 0.3.0 first, controller beside the agent, then the stacked templates keeping every command, path and metric name |

## Earlier design notes

Before numbered records, the 0.3 design was written as two unnumbered notes on the `next` branch; they land
in this folder with 0.3.0 and stay as they are:

- `0.3-controller.md`: the controller, and agents that run from their slice of its configuration.
- `phase-2-actions.md`: notifications, events and alert rules that act.

They are background, not records: a change to what they describe gets a numbered record.
