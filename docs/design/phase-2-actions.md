# Phase 2: rules that act

Status: implemented on `next` (PRs #24–#28) with the defaults below: 3 actions per hour per target, once per problem, no actions for late events, failover deferred. Tested on the Talos cluster: a unit failing on purpose was restarted twice, then limited. It builds on phase 1 (docs/design/0.3-controller.md), whose rules could only notify.

## Why

Phase 1 moved every event to the controller and made notifications a matter of rules. But the controller still only *tells* someone. Most of what that person then does is mechanical:
- restart the unit that crash-looped;
- re-run the transfer that failed on a network blip;
- rotate a pool by hand;
- re-tune a host whose uplink changed.

Phase 2 lets a rule do that itself, within limits that stop it making things worse, and records everything it does.

## What phase 1 already provides

- **Every event, in order:** each event reaches the controller in order, outages included. Each has an agent, a kind, a subject and the time it happened.
- **Rules:** rules match events, count them within windows, and open and close problems.
- **Commands:** the controller already queues commands to agents (`restart` a unit, `run` a transfer, `tune`), and agents carry them out and report the result. Phase 1's UI buttons use them.
- **Rotation:** rotating a pool happens on the controller itself.

So an action is mostly a rule queuing a command instead of a person.

## Config

A rule gains `do`, alongside or instead of `send`. Every rule must have at least one of them.

```yaml
notify:
  rules:
    - name: crash-loop
      match: {kind: unit.failed}
      when: {count: 3, within: 15m}
      send: [ops]
      do:
        action: restart              # restart the failing unit, where it fails
        delay: 1m                    # give it a minute to come back by itself
        limit: {count: 2, within: 1h}

    - name: retry-transfers
      match: {kind: transfer.failed}
      do: {action: retry, delay: 10m}   # one more try, unless a run succeeds first

    - name: rotate-on-quota
      match: {kind: transfer.failed, subject: "*-uploads"}
      do: {action: rotate, pool: uploads}

    - name: retune-on-drift
      match: {kind: tune.drift}
      do: {action: tune, limit: {count: 1, within: 7d}}
      send: [ops]

    - name: catch-up
      match: {kind: unit.recovered, subject: media-tv}
      do: {action: run, transfer: tv-uploads}   # move what piled up while it was down
```

### Actions

| `action` | Does | Target | Carried out by |
|---|---|---|---|
| `restart` | Restart a unit | The event's unit (`unit.*`, `warm.*`, `mount.*`, `serve.*` events) on the event's agent, or `unit:` / `agent:` | The agent (`restart` command) |
| `retry` | Run a failed transfer again | The event's transfer on the event's agent | The agent (`run` command) |
| `run` | Run a named transfer | `transfer:`, on the event's agent or `agent:` | The agent (`run` command) |
| `tune` | Re-tune a host's uploads | The event's agent, or `agent:` | The agent (`tune` command) |
| `rotate` | Rotate a pool by hand: the current key keeps its quota | `pool:`, else the event's pool | The controller |

`agent:`, `unit:`, `transfer:` and `pool:` take the event's own values as placeholders, `{agent}` and `{subject}`, so a rule can point at something other than what the event is about. For example: `run` the uploads for the unit that just recovered.

### Settings of `do`

| Field | Default | |
|---|---|---|
| `action` | *(required)* | `restart`, `retry`, `run`, `tune` or `rotate` |
| `delay` | `0` | Wait this long first; cancelled if the problem closes meanwhile |
| `limit` | `{count: 3, within: 1h}` | At most this many actions per rule and target in a window |
| `every` | none | While the problem stays open, act again this often (still within `limit`); none: once per problem |
| `dry_run` | `false` | Record what it would do (`action.planned`) and do nothing |

A top-level `actions: {enabled: true}` turns every rule's `do` off at once. The rules keep notifying, and what they would have done is recorded as `action.planned`. It's a kill switch for an incident, and a way to try new rules safely.

### Checked at load

- The target can be worked out. `restart` needs a unit: from a `unit.*`, `warm.*`, `mount.*` or `serve.*` match, or `unit:` given. `retry` needs a `transfer.*` match. `rotate` needs `pool:`, or a match on `pool.*` or `transfer.*` events (the transfer's unit's pool).
- A named `agent:`, `unit:`, `transfer:` or `pool:` exists, and the transfer or unit is bound on that agent.
- `delay`, `limit.within` and `every` are durations.

## When an action happens

1. **On firing.** An action is taken when its rule fires and opens a problem. With `every`, it repeats while the problem stays open. Matches that only keep an open problem open don't act again.
2. **After `delay`.** The action waits. If the problem closes first (the unit recovered by itself, the next scheduled run succeeded), it's cancelled and recorded as `action.cancelled`. Waiting actions are kept in the controller's state (`<state>/actions.json`), so a controller restart doesn't drop or repeat them.
3. **Within `limit`.** A count per rule and target (the agent and unit or transfer). Past it, the action is skipped and `action.limited` is raised: an error, and a problem of its own a rule can send. That's what stops a broken unit from being restarted forever.
4. **Live events only.** An action is never taken for an event that arrived in a backlog (more than 2 minutes late). The situation it describes has probably changed, and acting on it hours later could undo a recovery. The backlog summary says so: "restart skipped: reported late".
5. **Not twice at once.** If the same action on the same target is already waiting or queued, a new firing doesn't add another.
6. **Expiring commands.** A command for an agent that doesn't collect it within 10 minutes (the agent is silent) expires, recorded as `action.expired`. It isn't carried out when the agent comes back, perhaps much later. This is new for every command, the UI's buttons included.

## What gets recorded

Every action is an event of the controller's own, so it shows in the event log, metrics and notifications like anything else:

| Event | Severity | When |
|---|---|---|
| `action.taken` | info | Queued, or done for `rotate`: rule, action, target, command id |
| `action.done` / `action.failed` | info / error | The agent's result for the command |
| `action.cancelled` | info | The problem closed during `delay` |
| `action.limited` | error | Over `limit`: skipped |
| `action.expired` | warning | The agent never collected it |
| `action.planned` | info | `dry_run`, or actions turned off: what would have happened |

A rule with both `send` and `do` mentions its action in its notification ("unit.failed media-tv on pve-01 (3x); restarting in 1m"). Commands record `by: rule:<name>`, beside the UI's `by: <user>`. The state API lists waiting actions and each rule's count within its limit.

## Changes in the code

**`config.py`:**
- `RuleDo` (action, the targets, delay, limit, every, dry_run);
- `Rule.do`, with `send` now optional (one of the two required);
- `Actions` (`enabled`), and the load checks above.

**`alerts.py`:**
- On firing, plan the action alongside the notification.
- Keep waiting actions (persisted) and cancel them when their problem closes.
- Count limits.
- Skip late events.
- Hand due actions to the controller.

**`controller.py`:**
- `act()` turns a due action into a command, or a rotation.
- Commands gain `expires` and `rule`.
- `take()` drops expired ones; `finish()` raises `action.done` / `action.failed` for a rule's commands.
- A loop runs due actions.

**Agent:** nothing new. `restart`, `run` and `tune` are the commands it already carries out.

## Not in this phase: failover

"Run the transfer on another agent" was in phase 1's outline. It needs more than an action, because a transfer reads one host's files:
- it only makes sense for sources on shared storage (NFS, Ceph) that another host also has;
- the same transfer would then be defined on two agents, which the load-time overlap check refuses for a `sync`;
- the controller would need to know the primary is really down, and not merely slow, before the standby runs, or the two would run at once.

That's a feature of its own: standby transfers (`standby_for: <agent>/<transfer>`), exempt from the overlap check against their primary and run only while the primary's agent is silent. I'd design it separately once phase 2 shows how actions behave in practice.

## PRs

1. **Commands:** expiry for every command (`action.expired`); `by: rule:<name>`; events for a rule's command results.
2. **Config:** `RuleDo`, `Rule.do`, `send` optional, `actions.enabled`, load-time checks. Examples and the example-completeness test.
3. **Engine:** planning on firing, `delay` with cancellation on recovery, persisted waiting actions, `limit`, `every`, live events only, de-duplication, `dry_run` and `action.planned`.
4. **Actions:** `restart`, `retry`, `run`, `tune` through the agents' commands; `rotate` on the controller; notifications naming the action.
5. **Docs:** the wiki's Notifications page (to become "Rules"), Controller, Troubleshooting ("why didn't it restart?": limits, late events, cancelled); release notes.

Tests for each:
- a crash loop restarts once and is limited after `count`;
- a recovery cancels a delayed restart;
- a late backlog takes no action;
- a silent agent's command expires;
- `rotate` keeps the current key's quota;
- `dry_run` and `actions.enabled: false` only plan;
- a restart of the controller during a `delay` neither drops nor repeats the action.

Then a run on the Talos cluster with a unit made to fail on purpose.

## Questions

1. **Defaults.** A `limit` of 3 per hour per target, and one action per problem unless `every` is set. Are these right for you, or should actions be more conservative by default (for example 1 per hour)?
2. **Late events.** Never acting on a backlog is the safe choice. Is there a case where you'd want it, for example retrying a transfer that failed during an outage?
3. **Failover.** Deferred as above. Is it something you need soon, which would make it phase 3?
4. **Release.** Phase 2 as 0.4.0 after 0.3.0 is out, rather than holding 0.3 for it?
