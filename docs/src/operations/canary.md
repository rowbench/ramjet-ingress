# Canary auto-promotion

Let a healthy canary promote itself, and pull it back the moment it stops being
healthy. It is off unless asked for, and opting in is one annotation.

Annotate the **canary** Ingress:

```yaml
metadata:
  annotations:
    nginx.ingress.kubernetes.io/canary: "true"
    nginx.ingress.kubernetes.io/canary-weight: "5"
    ramjet.dev/auto-promote: "true"
    # ramjet.dev/auto-promote-interval: 60s
    # ramjet.dev/auto-promote-steps: 5,10,25,50,100
    # ramjet.dev/auto-promote-max-5xx-percent: "1"
    # ramjet.dev/auto-promote-max-latency-factor: "1.5"
    # ramjet.dev/auto-promote-min-requests: "50"
```

Every field has a default that is safe to run with. The rest exist because
"safe" is a property of a particular service's error budget, and nobody else can
know it. The full table with parsing rules is in the
[annotations reference](../configuration/annotations.md#canary-auto-promotion).

## The state machine

Every interval, per opted-in canary: take the **window** — this interval's
deltas only, canary side and stable side separately — and decide.

```text
                    ┌─────────────────────────────────┐
                    │  window: canary and stable       │
                    │  requests, 5xx, mean latency     │
                    └───────────────┬─────────────────┘
                                    │
              either side < min-requests?
                        │ yes                  │ no
                        ▼                      ▼
                   ┌────────┐        canary 5xx% > max-5xx-percent
                   │  HOLD  │           or canary mean latency >
                   └────────┘           stable mean × max-latency-factor
                                            │ yes            │ no
                                            ▼                ▼
                                     ┌────────────┐    next step exists?
                                     │  ROLLBACK  │      │ yes      │ no
                                     │ weight → 0 │      ▼          ▼
                                     └────────────┘   ┌──────┐  ┌──────────┐
                                                      │ STEP │  │ PROMOTED │
                                                      └──────┘  └──────────┘
```

The router counts the two sides apart, which is what makes the comparison
possible at all — see
[`canary_stats`](./observability.md#reading-canary_stats).

## Three things that are easy to get wrong

**Holding is not failing.** A canary receiving nothing at 03:00 is a quiet
service, not a broken one. Gating on **both** sides — not just the canary's —
also matters: a latency comparison against four stable requests is not a
comparison. Rolling back on low traffic would make the feature unusable on
anything but the busiest routes.

**Windows, not lifetimes.** The counters are cumulative and the process may have
been up for a week, so a lifetime error rate cannot move fast enough to catch
anything. Each pass subtracts the previous pass's reading.

> The first pass after a step spans the moment the weight changed and so mixes
> two ratios. That is deliberate, and it errs safe: the older and smaller weight
> is the one over-represented.

**Errors are absolute, latency is relative.** An error budget is a number
somebody actually has, so production being on fire is not a licence to promote a
canary that is also on fire. Latency has no such absolute: a service that
legitimately takes two seconds would be un-promotable against a fixed threshold,
so the canary is compared to what it is replacing.

## Interlocks

- **A rollback pin pauses everything.** An operator holding the
  [emergency brake](./rollback.md) has taken manual control of what this replica
  serves; patching Ingresses underneath them would be changing the cluster they
  are trying to hold still.
- **A rollback is one-way.** It writes `auto-promote: "false"` alongside the
  weight, and the loop refuses any canary whose status says it was rolled back
  even if the annotation is somehow still true. Both, because the guard has to
  survive a restart — the annotation carries it across a rescheduled pod — and
  because the two are written in one patch that could half-fail.

  A canary re-armed automatically after failing once will fail again on the next
  interval, flapping traffic across a broken backend for as long as nobody is
  watching. **Re-arming is a human decision.**
- **Reaching the last step is validated before it is accepted.** Stepping to
  100% and immediately declaring victory would mean full traffic never gets a
  single window of scrutiny, so promotion happens on the *next* healthy window
  at the final weight.

## What a rollback writes

```yaml
nginx.ingress.kubernetes.io/canary-weight: "0"
ramjet.dev/auto-promote: "false"
ramjet.dev/auto-promote-status: "rolled-back: 5xx 4.2% over 1%"
```

To re-arm after fixing the canary, clear `auto-promote-status` and set
`auto-promote` back to `"true"` yourself.

A successful finish writes `ramjet.dev/auto-promote-status: promoted` and stops.

## Where the decisions show up

Logged on the `audit` target **with their numbers**, written as a Kubernetes
Event **on the canary Ingress**, and POSTed to `--audit-webhook`:

| Event reason | When |
|---|---|
| `CanaryStepped` | The weight advanced to the next step |
| `CanaryPromoted` | The last step held for a healthy window |
| `CanaryRolledBack` | A gate was breached. Recorded as a **Warning** |

```sh
kubectl describe ingress web-canary
```

```text
Events:
  Type     Reason            Age   From            Message
  ----     ------            ----  ----            -------
  Normal   CanaryStepped     4m    ramjet-ingress  canary healthy; weight 5 -> 10
  Normal   CanaryStepped     3m    ramjet-ingress  canary healthy; weight 10 -> 25
  Warning  CanaryRolledBack  2m    ramjet-ingress  rolled back from 25%: 5xx 4.10% over the 1% budget
```

On the Ingress, and only there. An Event exists to point at the object to go and
look at, and after an automatic rollback that object is the canary — a second
copy on the IngressClass would make `kubectl get events` show every promotion
twice for a class-level view the `audit` log and the webhook already carry in
full. Configuration-level events (`ConfigApplied`, `ConfigPinned`,
`ConfigResumed`) still go on the IngressClass, because a compiled generation
belongs to no single Ingress.

**Holds are `debug` only.** On a quiet route they are the normal state, and an
Event per interval per canary would bury the three that matter.

## Why the backend swap stays human

Reaching 100% means every request is served by the canary backend while the
production Ingress still names the old one. The obvious next step — rewrite
`spec.rules[].backend` and delete the canary Ingress — is deliberately left to a
person, and it looks like the last mile of the same job, so it is worth saying
why it is not.

Everything this loop does is **reversible by writing one number**. Every state
it can reach is a weight, and every weight has an inverse the loop already knows
how to apply; a rollback is the same mechanism as a step.

Editing the backend is a different kind of change: it is the thing the canary
was a rehearsal *for*, it normally comes with deleting an object, and undoing it
means reconstructing an object rather than setting a field. A controller that
restructures the resources an operator wrote, on a timer, is a controller people
turn off.

So the loop drives the dial to 100, says so in an Event and in the annotation,
and stops.

## RBAC, and the GitOps warning

This is the **only** write this controller makes to an object an operator
authored. It needs:

```yaml
- apiGroups: ["networking.k8s.io"]
  resources: ["ingresses"]
  verbs: ["patch"]
```

Spec-level, because an annotation is metadata and `ingresses/status` cannot
carry it. The chart's ClusterRole has it. **Without the rule, promotion logs a
permission error every interval and changes nothing.**

The patches are JSON merge patches sent under the `ramjet-ingress` field
manager, so `managedFields` still answers "who set this weight" and taking the
field from whoever created the Ingress — a person, a Helm release, a GitOps
reconciler — needs no override, because a merge patch is never refused as a
conflict.

> Deliberately **not** a server-side apply. An apply states everything the field
> manager owns, so the API server deletes whatever that manager's entry claims
> and the body omits — and this controller writes two disjoint annotation sets
> under one manager. As applies, promotion's `canary-weight` write erased
> `ramjet.dev/observed-generation`, and the status writer's next write erased
> `canary-weight`, leaving a canary with no weight seconds after the controller
> announced it had stepped it up. Nothing here ever needs to remove an
> annotation, which is the only thing an apply buys.

> In a GitOps cluster, a reconciler that also claims `canary-weight` will fight
> this loop and win on its own schedule. Either exclude `canary-weight` from its
> managed fields, or do not opt that Ingress in.

## What it costs when nobody uses it

Nothing measurable. The candidates are compiled by the controller and arrive on
the generation channel; the loop issues **no API reads of its own**, because the
controller has already listed every Ingress and parsed every annotation. A loop
doing its own `list` would cost a cluster-wide read every minute, forever, on
every installation, whether or not anybody uses the feature.

With nobody opted in, the list is empty and the loop is a timer that does
nothing.
