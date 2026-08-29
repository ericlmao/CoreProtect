# Folia Support

CoreProtect runs on [Folia](https://papermc.io/software/folia) without any configuration. The plugin
detects a Folia server on start-up and schedules its work accordingly; on any other server nothing
about it changes.

## What Folia changes

An ordinary server runs the whole world on a single thread, so a plugin can touch anything from that
thread. Folia splits the world into regions and gives each one a thread of its own. A task then has
to run on the thread that owns whatever it is about to touch, and there is no single "server thread"
to fall back on. Handing work to the wrong thread is not slow, it is unsafe.

CoreProtect therefore says what each piece of work concerns, and the scheduler decides what that
means on the server it is running on:

| What the work concerns | On Folia | Elsewhere |
| --- | --- | --- |
| A location or a chunk | The thread owning that region | The server thread |
| An entity or a player | The thread the entity is currently on, following it if it moves | The server thread |
| Nothing in particular | The global region | The server thread |
| Storage, and other work that touches no world state | A pooled thread | An asynchronous task |

The regionised schedulers are reached through
[folia-scheduler](https://github.com/moonrise-studios/folia-scheduler), which picks the right one
from what the work is given.

## Rollbacks and restores

A rollback covers whatever area was asked for, which on Folia may span many regions at once. Chunks
are grouped by the region that owns them and each group is rolled back on its own thread, in batches
that yield between chunks so no region is held up. Progress and the final summary are reported as
usual; a rollback spanning regions takes the same arguments and gives the same result as one that
does not.

Entities are handled on the thread they are currently on rather than through the world, since on
Folia an entity may have moved to another region since it was logged.

## Requirements

Folia itself requires Java 21 or newer, which is also what CoreProtect's Folia support is built
against. Nothing about that reaches a server without regionised scheduling: those classes are only
loaded once a Folia server has been detected, so older servers are unaffected and continue to run on
Java 11.

## Limitations

`/co purge` and the automatic purge run against the database rather than the world, so they are
unaffected by regions and behave identically on Folia.

The inspector, lookups, and the API all work as they do elsewhere.
