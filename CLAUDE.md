# CLAUDE.md — Nostreets.Extensions.Core

## Self-Maintenance Rule

**After any meaningful change to this codebase, update this file before closing the task.**

---

## Project Overview

The **foundational utility library** every OS service and app depends on. It owns the shared
result envelope (`ServiceResponse<T>`), the data-access contracts (`IDBService<…>`), the entity
base (`DBObject`/`IDBObject`), and a very large set of extension methods + helpers (reflection,
JSON, caching, dynamic-class building, Excel/SQL, etc.). If a generic helper exists anywhere in
the platform, it most likely lives here.

**Framework**: .NET 8.0 (upgraded from net7.0 in Phase 1 of the SDK/NuGet effort). Packaged as
`Nostreets.Extensions.Core` to the local feed. **It is the root dependency** — it has no internal
project references, so (unlike the other libs) it has no ProjectRef/NugetRef switchable refs.

> **Generic-helpers-first rule (cross-project):** Before writing a new generic helper inline
> anywhere in the solution, search **this project** (and `Nostreets.Extensions`) first. Reuse or
> extend what's here rather than re-rolling locally. Every CLAUDE.md in the solution points back
> to this rule.

---

## The types downstream code actually consumes

| Type | Namespace | Why it matters |
|------|-----------|----------------|
| `ServiceResponse` / `ServiceResponse<T>` | `Nostreets.Extensions.Core.Models.Responses` (file `ServiceReponse.cs` — note the typo) | The universal result envelope: `IsSuccessful`, `TransactionId`, `Errors` (`Dictionary<string,string[]>`), and `Data` (generic). Factory methods `Success(...)` / `Error(...)`. **Its ctor is `internal`** — System.Text.Json **cannot deserialize** it; Newtonsoft can. That's why the in-house Refit SDKs use the Newtonsoft content serializer. Do not "fix" the ctor without understanding that contract. |
| `IDBService` / `IDBService<T>` / `IDBService<T, IdType>` / `IDBService<T, IdType, AddType, UpdateType>` | `Nostreets.Extensions.Interfaces` | The data-access contract. Implemented by `EFDBService<T>` in `Nostreets.Orm.EF.Core` (and `InternalService<>` in OS.Base.Services). CRUD surface: `Get`, `GetAll`, `Where`, `FirstOrDefault`, `Count`, `Insert`/`InsertRange`, `Update`/`UpdateRange`, `Delete`/`DeleteRange`, `Build`. **Predicates are `Func<T,bool>` (client-side), not `Expression<…>`** — filtering happens in memory, so it's built for modest result sets. |
| `DBObject` / `DBObject<T>` / `IDBObject` | `Nostreets.Extensions.DataControl.Classes` / `.Interfaces` | Abstract entity base with audit fields: `Id`, `DateCreated`/`CreatedBy(Id)`, `DateModified`/`ModifiedBy(Id)`, `IsArchived` (soft-delete flag). `DBObject` defaults to `string` (GUID) ids. The `BaseService<T>` cascade walks `IDBObject` graphs. |
| `SerializedList<T>` / `SerializedDictionary<TKey,TValue>` | `Nostreets.Extensions.DataControl.Classes` | `IList<T>` / `IDictionary` that persist as a JSON string column while exposing a typed in-memory view (lazy bi-directional sync). Used pervasively for `[NotMapped]`-style JSON columns on entities. |
| `Error` | `Nostreets.Extensions.DataControl.Classes` | Exception-capture entity (`: DBObject`) — `ErrorMessage`, `Source`, `Class`, `Method`, `Line`, traces. Backs `AuditError`. |
| `Basic` (static) | `Nostreets.Extensions.Extend.Basic` | The ~4k-line mega extension class. High-traffic members: `SpaceBetweenUpperCase` ("MyName"→"My Name"), `MapProperties` (object/Expando ↔ T with name remapping), `Clone<T>` (deep copy, `ObjectCreationHandling.Replace` — safe for the SerializedList bridge pattern), `GetMethodNameThruStack` (call-stack method name for audit logging), `JsonSerialize`/`JsonDeserialize`, reflection helpers (`GetPropertyValue`/`SetPropertyValue`/`Instantiate`), attribute scanning, enum helpers. |
| `CacheManager` | `Nostreets.Extensions.Utilities.Managers.Core` | Dual-layer cache: in-process `MemoryCache` + optional Redis (StackExchange.Redis). |
| `ClassBuilder`, `Scanners`, `ExcelService`, `Encryption`, `FileManager` | `Nostreets.Extensions.Utilities` | Runtime type generation; assembly/attribute scanning; OleDB Excel CRUD; AES; file IO. |

---

## Directory / namespace layout

```
Extend/        # extension-method groups: Basic, Data, Config, Web, Google, IOC, Blazor
Helpers/       # Data (SqlService/OleDbService/DataMapper/SqlExecutor + a custom LINQ QueryProvider),
               #   Converter (Date/TimeOnly + JSON converters), Web (assembly/controller resolvers)
Interfaces/    # IDBService variants, IDBObject, IDataReaderExt, IExecutors
Models/        # Responses/ (ServiceResponse, BaseResponse, Item(s)Response, …) + Requests/
DataControl/   # Classes/ (DBObject, SerializedList, SerializedDictionary, PagedList, Error, Token, …) + Enums/
Utilities/     # ClassBuilder, Scanners, ExcelService, Encryption, FileManager, CacheManager, Solution, …
```

**Namespaces are inconsistent by design/history** — some files use `Nostreets.Extensions.*`
(e.g. `Nostreets.Extensions.Interfaces`, `Nostreets.Extensions.Extend.Basic`,
`Nostreets.Extensions.DataControl.Classes`) and others use `Nostreets.Extensions.Core.*`
(e.g. `Nostreets.Extensions.Core.Models.Responses`). **Don't assume namespace == folder path** —
grep for the type. Don't mass-rename namespaces; downstream code imports the existing ones.

---

## Conventions

- **Extension-first.** Reach for `Basic`/`Data`/etc. before writing inline helpers (the
  generic-helpers-first rule). Add genuinely-generic helpers here so the next caller finds them.
- **`ServiceResponse<T>` everywhere** as the service result envelope; never throw across a service
  boundary when a `ServiceResponse.Error(...)` will do.
- **Entities derive from `DBObject`** for audit fields + soft-delete (`IsArchived`).
- **`SerializedList<T>`** for list-shaped JSON columns; deep-copy with `Clone()` before sharing a
  reference into a request DTO (it uses `ObjectCreationHandling.Replace`, which is what makes it
  safe for types exposing both an `IList<T>` and a paired serialized-string property).

## Gotchas / dependencies

- **.NET-Framework-era package baggage.** Although it targets net8.0, this project still references
  legacy packages — **EF6, ASP.NET MVC 5 / WebApi, OWIN, Unity, Castle.Windsor** (plus System.Web
  assemblies, Hangfire, Google.Apis, StackExchange.Redis). This was flagged as a known risk for the
  net7→net8 upgrade and is **not** to be churned without sign-off. EF6 and EF Core coexist in the
  package — namespace `DbContext`/`Database` imports carefully in consumers.
- **Custom LINQ provider** under `Helpers/Data/QueryProvider/` translates expression trees to
  SQL/OleDB — large and lightly documented; treat complex-query edge cases with care.
- **`ConfigurationManager`-based data services** (`SqlService`/`OleDbService`) read connection
  strings from `System.Configuration`, not `IConfiguration` — relevant if you wire them in an
  ASP.NET Core host.

## Packaging

- net8.0; repo-level `Directory.Build.props` carries metadata + `<Version>` (manual SemVer, `1.0.0`);
  `PackageId` defaults to the project name. `GeneratePackageOnBuild=false` — pack explicitly:
  `dotnet pack <csproj> -c Release -o "C:\Users\Nile O\.nuget-local-feed"`.
- Root of the dependency graph: **no internal project references**, hence no ProjectRef/NugetRef
  switch here (the switch lives in the libs/apps that consume it).

## What to Avoid

- Do not re-roll generic helpers inline — search/extend here first.
- Do not change `ServiceResponse`'s ctor/shape without accounting for the STJ-vs-Newtonsoft contract
  (the SDKs depend on Newtonsoft being able to deserialize it).
- Do not mass-rename the (inconsistent) namespaces — consumers import them as-is.
- Do not add/upgrade/remove the legacy .NET-Framework-era package refs without sign-off — it's a
  known, deliberately-deferred migration risk.
