<div align="right">
  <sub><em>part of <a href="https://github.com/mattlianje/d4"><img src="https://raw.githubusercontent.com/mattlianje/d4/master/pix/cabinet6-mark-adaptive-light.svg" width="32" align="top" hspace="4">c6</a></em></sub>
</div>

<p align="center">
  <img src="https://raw.githubusercontent.com/mattlianje/d4/master/pix/dbos4s.png" width="300">
</p>

# dbos4s
**Simple Scala bindings for [DBOS](https://www.dbos.dev/)**

A thin layer over the official [dev.dbos:transact](https://mvnrepository.com/artifact/dev.dbos/transact) Java library that lets you turn vanilla Scala
functions into durable workflows 🧈✨

## Installation
**dbos4s** is on the MavenCentral repository and cross publish for Scala 2.12, 2.13 and 3. **dbos4s** brings the pinned Java DBOS onto your classpath, and changes nothing about the transactor.

```scala
"xyz.matthieucourt" %% "dbos4s-transact0.9"         % "0.1.0"
"xyz.matthieucourt" %% "dbos4s-etl4s-transact0.9"   % "0.1.0"
"xyz.matthieucourt" %% "dbos4s-testkit-transact0.9" % "0.1.0" % Test
```
All you need:
```scala
import dbos4s._
```

## Of note... 
At its core, it simply is a spread of niceties on top of DBOS Java to make it idiomatic Scala, and remove some of the interop jankiness.

> [!WARNING]
> The underlying [Java library](https://mvnrepository.com/artifact/dev.dbos/transact) isn't 1.0 yet, so its API (and ours) may still shift.

## Quickstart
```scala
import dbos4s._

val dbos = Dbos("store") {
  _.withDatabaseUrl("jdbc:postgresql://localhost:5432/store")
   .withDbUser("postgres").withDbPassword("dbos")
}

val fulfill = dbos.workflow("fulfill") {
  dbos.step("reserve") { println("reserved widget") }
  val txn = dbos.step("charge") { println("charged $42"); "txn_8f2" }
  dbos.step("ship") { println(s"shipped to Alice ($txn)") }
}

dbos.launch()
fulfill.start("order-42").result
```

> [!IMPORTANT]
> **One rule:** Since workflow bodies are replayed on recovery, keep them
> deterministic and put all I/O, randomness, time and side effects inside
> `step { … }`
>
> Completed steps return their recorded output instead of re-running.
>
> The `id` (e.g. "order-42") is the idempotency key: start the same id twice and you
> get the first run's result back, not a second execution

## Getting started

1. **Provision PostgreSQL:** 
   
   Upon which DBOS is built and will persist your workflow state

   ```bash
   docker run -d -p 5432:5432 -e POSTGRES_PASSWORD=dbos postgres
   ```
   (Full runnable examples live in [`examples/`](examples/src))

2. **Configure a `Dbos`:**
   
   The Scala builder syntax mirrors the Java `DBOSConfig`

   ```scala
   val dbos = Dbos("payments") {
     _.withDatabaseUrl("jdbc:postgresql://localhost:5432/payments")
      .withDbUser("postgres")
      .withDbPassword("dbos")
   }
   ```

3. **Register workflows and steps before launch:**
   
   This is where you will drop your vanilla Scala functions

   ```scala
   val checkout = dbos.workflow("checkout") {
     dbos.step("reserve")(reserveInventory())
     dbos.step("charge")(chargeCard())
   }
   ```

4. **Launch, then start:** 
   
   `launch()` recovers interrupted workflows... `start(id)` runs one keyed by `id`

   ```scala
   dbos.launch()

   /* .result blocks for the return val */
   checkout.start("order-42").result
   ```

## Examples

### Exactly-once side effects
A step runs at most once; replays return its recorded output:
```scala
dbos.workflow("checkout") {
  dbos.step("charge") { chargeCard(42.00) }
  dbos.step("email")  { sendReceipt() }
}
```

### Sleep that survives reboots
The deadline is checkpointed, so any process resumes it:
```scala
import scala.concurrent.duration._

dbos.workflow("trial") {
  dbos.step("welcome") { sendWelcome() }
  dbos.sleep(14.days)
  dbos.step("nudge")   { sendUpgradeNudge() }
}
```

### Wait for a human
Pause indefinitely for an approval, then continue exactly once:
```scala
import scala.concurrent.duration._

dbos.workflow("refund") {
  dbos.step("request") { notifyReviewer() }
  val ok = dbos.recv[Boolean]("approval", timeout = 7.days) /* Durable block */
  if (ok.contains(true)) dbos.step("payout") { issueRefund() }
}
/* elsewhere, the reviewer unblocks by id */
dbos.send("refund-99", true, "approval")
```

### Retries with backoff
Transient failures retry automatically
```scala
dbos.step("fetch", maxAttempts = 5) { callFlakyApi() }

// All options:
// dbos.step("fetch",
//    maxAttempts = 5,
//    retryInterval = 1.second,
//    backoff = 2.0
// ) { ... }
```

## Operating workflows

The (Scala) runtime API for driving and inspecting workflows once they're registered.

### Enqueue onto a queue

```scala
dbos.registerQueue(new Queue("emails"))
dbos.launch()

val handle = checkout.enqueue("emails", "job-1") // queued (not run inline)
handle.result
```

### Managing in-flight workflows

```scala
dbos.cancel("order-42")
dbos.resume[Receipt]("order-42").result

dbos.listWorkflows() // List[WorkflowStatus]
dbos.listWorkflows(_.withWorkflowName("checkout"))
dbos.listSteps("order-42") // List[StepInfo]

dbos.retrieve[Receipt]("order-42").result // re-attach by id
dbos.fork[Receipt]("order-42", startStep = 2) // re-run from a step
```

### Queue management

```scala
dbos.getQueue("emails") // Option[Queue]
dbos.findQueue("emails") // Option[Queue]
dbos.listQueues()  // List[Queue]

dbos.updateQueue("emails", QueueOptions.setConcurrency(8))
```

### Streams and bulk messaging

A durable stream is an ordered, keyed log; `readStream` returns an `Iterator`.

```scala
dbos.writeStream("progress", 1)
dbos.writeStream("progress", 2)
dbos.closeStream("progress")

dbos.readStream[Int]("order-42", "progress").toList // List(1, 2)
dbos.sendBulk(Seq(SendMessage("a", "go"), SendMessage("b", "go")))

dbos.events("order-42") // Map[String, Any] ... all events
```

### Scheduled (cron) workflows

Register a cron expression and DBOS owns the firing.

```scala
dbos.createSchedule(WorkflowSchedule("nightly", "checkout", "0 0 * * *"))
dbos.pauseSchedule("nightly")

dbos.triggerSchedule[Receipt]("nightly").result
dbos.backfillSchedule("nightly", from, to) // List[Handle[Any]]
```

## Testing

You'll often want to unit-test the logic of your durable workflows without actually
side effecting and/or provisioning a database. This is what `TestDbos()` is for

```scala
import dbos4s.testing._

val dbos = TestDbos()
  .onStep("charge").returns(Receipt("ok")) /* Stub this side-effecting step */

val checkout = dbos.workflow("checkout") {
  val r = dbos.step("charge")(realCharge()) /* Body skipped, stub returned */
  dbos.setEvent("phase", "charged")
  r
}

assertEquals(checkout.start("order-1").result, Receipt("ok"))
assertEquals(dbos.steps,  List("charge"))
assertEquals(dbos.events, List(("order-1", "phase", "charged")))
```

Stub, seed and feed the doubles, then read back what the body did:

```scala
dbos.onStep("fetch").returns(42)

/* Step that fails a workflow */
dbos.onStep("charge").throws(new RuntimeException("declined"))

/* Pre-seed upstream events reads */
dbos.seedEvent("upstream", "ready", 7)

/* Queue messages for recv() */
dbos.feed("approval", true)
```
And `TestDbos` keeps a log of everything the body did, so you can assert on it:

```scala
dbos.steps        // List[String]                every step name in order
dbos.started      // List[String]                every workflow id that was started
dbos.slept        // List[FiniteDuration]        every sleep duration

dbos.events       // List[(id, key, value)]      every setEvent
dbos.sends        // List[(id, topic, message)]  every send / sendBulk
dbos.streamWrites // List[(id, key, value)]      every writeStream
```

> [!NOTE]
> `TestDbos` covers only logic, not durability ...
> Thus, methods like `cancel`, `resume` have no in-memory model and throw.

## Why dbos4s?

The Java library identifies workflows by class+method reflection, so in raw Scala you're 
forced to:
- Manufacture a `Function0`
- Pass nulls for the unused proxy slots

Without **dbos4s** (raw Java API):
```scala
import dev.dbos.transact.DBOS
import dev.dbos.transact.config.DBOSConfig
import dev.dbos.transact.execution.ThrowingSupplier
import dev.dbos.transact.workflow.StepOptions
import dev.dbos.transact.StartWorkflowOptions

val dbos = new DBOS(
  DBOSConfig.defaults("payments")
    .withDatabaseUrl("jdbc:postgresql://localhost:5432/payments")
    .withDbUser("postgres")
    .withDbPassword("dbos")
)

val body: Function0[AnyRef] = () => {
  for (i <- 1 to 3) {
    val step: ThrowingSupplier[AnyRef, Exception] = () => { work(i); null }
    dbos.runStep(step, new StepOptions(s"step$i"))
    dbos.setEvent("steps_event", Integer.valueOf(i))
  }
  null
}
val method = classOf[Function0[?]].getMethod("apply")
val registered = dbos.integration()
  .registerWorkflow("checkout", body.getClass.getName, null, body, method, null, null)

dbos.launch()

val handle = dbos.integration()
  .startRegisteredWorkflow(registered, Array.empty[AnyRef],
   new StartWorkflowOptions("order-42"))

handle.getResult()

val phase: Int = dbos
  .getEvent[Integer]("order-42", "steps_event", java.time.Duration.ofSeconds(0))
  .orElse(Integer.valueOf(0))
```
With **dbos4s**:
```scala
import dbos4s._

val dbos = Dbos("payments") {
  _.withDatabaseUrl("jdbc:postgresql://localhost:5432/payments")
   .withDbUser("postgres")
   .withDbPassword("dbos")
}

val checkout = dbos.workflow("checkout") {
  for (i <- 1 to 3) {
    dbos.step(s"step$i")(work(i))
    dbos.setEvent("steps_event", i)
  }
}

dbos.launch()

val handle = checkout.start("order-42")
handle.result

val phase: Int = dbos.event[Int]("order-42", "steps_event").getOrElse(0)
```

## etl4s

`dbos4s-etl4s` is a separately published module that lets you make existing [etl4s](https://github.com/mattlianje/etl4s) pipelines
durable just by adding a few annotations.

Just two steps:
1. Bring a `Dbos` into implicit scope
2. Add your `@step(...)` annotations to etl4s nodes

```scala
import etl4s._
import dbos4s._
import dbos4s.etl4s._

case class Order(id: String, cents: Int)
case class Receipt(orderId: String, charged: Int)

given dbos: Dbos = Dbos("checkout-demo") {
  _.withDatabaseUrl("jdbc:postgresql://localhost:5432/checkout")
  .withDbUser("postgres")
  .withDbPassword("dbos")
}

@step("fetch") val fetch =
      Extract { Order(Dbos.workflowId.getOrElse("unknown"), 4200) }

@step("validate") val validate =
      Transform[Order, Order] { o => if o.cents > 0 then o else sys.error(s"bad order ${o.id}") }

@step("charge") val charge =
      Load[Order, Receipt] { o => Receipt(o.id, o.cents) }

val pipeline = fetch ~> validate ~> charge
val checkout: Workflow[Receipt] = pipeline.asWorkflow("checkout")

dbos.launch()
checkout.start("order-42").result
```

> [!NOTE]
> `@step` is a macro annotation, so enable it per Scala version
>
> ```scala
> // Scala 3
> scalacOptions += "-experimental"
> // Scala 2.13
> scalacOptions += "-Ymacro-annotations"
> // Scala 2.12
> addCompilerPlugin("org.scalamacros" % "paradise" % "2.1.1" cross CrossVersion.full)
> ```

## Durable AI agents
Synthetic example using **dbos4s** + **etl4s** to create an invincible agentic workflow.
```scala
import etl4s._
import dbos4s._, dbos4s.etl4s._
import scala.concurrent.duration._

given Dbos = dbos

@step("plan")    val plan       = Extract[String, Plan](agent.decompose)
@step("recall")  val queryDb    = Transform[Plan, List[Doc]](vectorDb.retrieve)
@step("search")  val searchWeb  = Transform[Plan, List[Doc]](web.search)
@step("llmCall") val llmCall    = Transform[List[Doc], Draft](llm.call)

@step("approve")
val waitForApproval = Transform[Answer, Answer] { a =>
  notify.requestReview(a)
  dbos.recv[Verdict]("review", timeout = 1.hour)
}

@step("publishDb")
val publishDb = Load[Answer, Unit](report.save)

@step("publishSlack")
val publishSlack = Load[Answer, Unit](slack.post)

/* Vanilla etl4s pipeline */
val researchBrief =
  plan ~> (queryDb & searchWeb) ~> llmCall ~> waitForApproval ~> (publishDb & publishSlack)

/* Make it a DBOS workflow */
val researchBriefWf = researchBrief.asWorkflow("research-brief")

dbos.launch()
researchBriefWf.start("Why is Gegard Mousasi retired in 2026?").result
```

## Status
"Workflows with arguments" and `Function0` serialization is still quite an open question
for transliteration to Scala...

Feel free to suggest alternative alternative APIs even if a feature is marked as "✅"

| Feature | dbos4s | Notes |
|---|:--:|---|
| Durable workflows (no args) | ✅ | `dbos.workflow("name") { ... }` |
| Durable steps | ✅ | `dbos.step` / `.step(StepOptions)` (retries, backoff) |
| Workflow events | ✅ | `setEvent` / `event` returns `Option` |
| Messaging (send / recv) | ✅ | `send` / `recv` returns `Option` |
| Durable sleep | ✅ | `dbos.sleep(...)` |
| Queues | ✅ | `registerQueue` + `enqueue` + queue management |
| etl4s pipelines as workflows | ✅ | - |
| In-memory test double | ✅ | `dbos4s-testkit` |
| Workflows with arguments | 🟡 | use etl4s or the raw Java proxied @Workflow method calls |
| Enqueue onto a queue | ✅ | `wf.enqueue(queue, id)` |
| Scheduled / cron workflows | 🟡 | Still need `registerProxy` |
| Workflow management | ✅ | - |
| Queue management | ✅ | - |
| Durable streams | ✅ | - |
| Messaging extras | ✅ | - |

