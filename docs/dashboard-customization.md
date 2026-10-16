# Custom Nakama dashboard

## What is customized

This fork adds Cafe Bazaar (store ID 10) and Myket (11) to the existing purchase
views, and adds a **Game Notification** tab inside the existing **Send
Notification** popup on a player's Notifications page. The page's button and
notification list stay intact. The popup opens on **Nakama notification**, with
the original form; its second tab contains the custom form. The new form supports:

- Resource Grant: message plus searchable BankDef currency/value rows. This is a notice
  about resources already granted, despite the UI's requested type label.
- Message: message only.
- Message with Link: message, HTTP(S) URL and link button label.

Sending never grants resources. It does not call a wallet or gameplay API.

## Dashboard structure and source availability

The Go server embeds its dashboard into the binary:

1. `console/ui/dist/` contains the upstream compiled Vue application and assets.
   The frontend source and build project are **not included** in this revision.
   `console/ui/src/` is not an editable application here. Heroic Labs describes
   the current console UI/design system as proprietary in its
   [console announcement](https://heroiclabs.com/blog/announcing-nakama-console-3/index.html).
2. `console/ui.go` embeds `ui/dist/*` and exposes a file server.
3. `server/console.go`, especially `registerDashboardHandlers`, serves the SPA
   index, substitutes console configuration and serves assets. JavaScript/CSS
   under `/static/` use immutable caching and optional gzip variants.
4. `console/console.proto` defines the Console API. Generated `.pb.go`,
   `.pb.gw.go` and OpenAPI files expose it over authenticated gRPC/HTTP.
5. `server/console_notification.go` implements the native notification endpoint.
   `server/console_purchase.go` reads native purchase records.
6. `console/acl/` enforces dashboard roles/permissions. Frontend controls do not
   replace server authorization.

This fork owns a small source extension instead of pretending to have the
upstream Vue source. It has no npm dependencies and does not depend on private
Vue component instances, internal stores or minified JavaScript variable names.
The custom build preserves the stock files in the checkout.

## Files to edit for future requests

| File | Responsibility |
| --- | --- |
| `console/custom/custom-console.mjs` | Player-route adapter, native popup discovery and lifecycle cleanup. |
| `console/custom/popup-tabs.mjs` | Shared accessible popup tabs and interception of only custom submits. |
| `console/custom/notification-popup.mjs` | Notification-only form inside native Send Notification. |
| `console/custom/wallet-popup.mjs` | Wallet grant form and uncertain-request operation ID retention. |
| `console/custom/resource-picker.mjs` | BankDef currency loading, search, explicit list selection and validity. |
| `console/custom/purchase-panel.mjs` | Third Payments subtab and verify/correct flow. |
| `console/custom/api.mjs` | Authenticated Console calls and game response/error unwrapping. |
| `server/console_api_explorer.go` | Trusted admin actor propagation and per-operation native ACL checks. |
| `server/console_audit.go` | Purchase token redaction in Console audit metadata. |
| `console/custom/custom-console.css` | Scoped layout, light/dark colors and mobile form layout. |
| `console/custom/model.mjs` | Store IDs, notification code/types, payload construction/validation, native Console send request. |
| `console/custom/model.test.mjs` | Contract, validation, auth/error and upstream patch tests. |
| `console/custom/build.mjs` | Copies upstream assets into a separate output directory, updates the store enum map, creates hashed/gzipped assets and adds the extension entry point. |
| `build/Dockerfile.cardgame` | Reproducible custom runtime and matching Go pluginbuilder targets. |
| `build/cardgame-go` | Builds game Go plugins against this Common fork using a temporary modfile. |
| `build/cardgame.sh` | Builds and loads both images locally. It never pushes. |
| `../nakama-common/api/api.proto` | Canonical store enum shared by Nakama APIs. |
| `../nakama-common/api/api.pb.go` | Generated protobuf implementation and reflection data. |
| `../nakama-common/index.d.ts` | Runtime TypeScript purchase-store string union. |
| `vendor/github.com/heroiclabs/nakama-common/api/` | Matching Common API copy used by Nakama's vendored build. |

The extension handles `#/players/{UUID}/notifications`, `/wallet` and `/payments`.
Popup extensions mount only while their matching native popup is open; Payments
adds its third subtab inside the native Payments subtab group. It identifies the popup by its
form dialog, heading and native To/Subject/Code controls. A body MutationObserver
handles Vue's asynchronous route changes and teleported popup nodes; it schedules
one reconciliation per animation frame and does not poll. Tabs and the custom
panel are inserted inside the popup's scrolling body. The page's notification
list and Send Notification button are not modified.

Both tabs use the original popup's Send button. The extension captures submit
only while the custom tab is selected, preventing the native handler from also
sending. The native tab's submit path remains untouched. Inactive form controls
are disabled so hidden required fields do not block the selected form. The custom
panel does not introduce a nested HTML form. Native input state survives tab
switching and a custom send. Closing the popup or changing routes disposes the
extension and aborts an outstanding request.
An in-flight send is disabled to prevent double clicks. An error keeps the
form data and displays the failure; there is no automatic resend.

This is a local extension, **not an official Nakama frontend plugin API**. After
upgrading the upstream console, verify the player route, native form dialog, popup heading,
To/Subject/Code control names, scrolling body, submit button, theme and session
storage contract. If those DOM contracts change, update the adapter in
`custom-console.mjs`. Do not silently fall back to a global broadcast form.

## Purchase store names

Store IDs must remain aligned with existing native `purchase.store` values:

| Store | ID | Protobuf enum |
| --- | --- | --- |
| Cafe Bazaar | 10 | `CAFE_BAZAAR` |
| Myket | 11 | `MYKET` |

Existing upstream IDs 0–4 stay unchanged. There is no database migration or
rewrite of purchase records. This adds enum names/display support; validation
and consumption still live in the game backend.

The protobuf enum alone does not update the compiled frontend. Its JavaScript
contains a separate generated enum map. `build.mjs` replaces exactly one known
upstream map with the same map plus these two entries. It fails the build if
that anchor disappears or occurs more than once. It writes a new
content-hashed main JS file, updates the index and regenerates `.gz` versions.
The old immutable asset URL is not reused. The frontend's existing store formatter
then shows the correct names in purchase rows and detail panels.

When changing store enums, regenerate Common's Go file from Common's root:

```sh
PATH="$HOME/go/bin:$PATH" go generate ./api
cp api/api.proto api/api.pb.go ../nakama/vendor/github.com/heroiclabs/nakama-common/api/
go test ./api/...
```

Install `protoc` and `protoc-gen-go` first if needed. Keep the TypeScript union,
Nakama Console/API OpenAPI store definitions and `StoreProviders` in the custom
model aligned. Never renumber an existing provider. For broader protocol
changes, use Nakama's `buf.sh` and the generation templates rather than editing
Go descriptors by hand.

## Notification contract and permissions

The form uses the current dashboard session's `SESSION_TOKENS.accessToken`
(sessionStorage first, then localStorage) and makes a same-origin request to
Nakama's existing `POST /v2/console/notification`. It neither stores credentials
nor sends requests to third-party services. Expired/forbidden sessions show an
error; normal sign-in/refresh remains owned by the stock dashboard.

An example native request:

```json
{
  "user_ids": ["11111111-1111-4111-8111-111111111111"],
  "subject": "Game notification",
  "code": 17,
  "persistent": true,
  "content": {
    "Value": {
      "Type": 1,
      "Message": {"Message": "You received a gift."},
      "Rewards": [{"Id": "PowerCore", "Value": 100}]
    }
  }
}
```

The `Value` wrapper and PascalCase fields match the game client's
`GameNotificationData` and the backend's `GameNotification` payload. Type 2 omits
Rewards/Url/ButtonText. Type 3 supplies `Url` and
`ButtonText:{"Message":"Read more"}`, without Rewards. Text uses the existing
RichString literal format. User content is passed as JSON/text, never HTML.
Resource values must be positive whole numbers within JavaScript's safe integer
range. Message size, resource count and link schemes are validated before send.

The selected player is fixed by the profile route. The form always sends a
one-element `user_ids` array; it cannot broadcast. The native Console endpoint
retains its existing notification-write permission checks. Regular game clients
cannot call it. No new server permission, HTTP key, public RPC or database table
is added. The game's existing `notification/send` RPC remains available for
Dashboard API Explorer scripts and broadcasts, but this form needs only the
native notification endpoint and the code-17 client handler.

## Build locally

Run from the Nakama fork with Nakama Common beside it:

```sh
./build/cardgame.sh ../nakama-common
```

This builds and loads `cardgame/nakama:3.41.0-cardgame.1` and
`cardgame/nakama-pluginbuilder:3.41.0-cardgame.1` for the local Docker architecture.
It never commits or pushes. Set `VERSION` when creating a new image revision.
The Dockerfile verifies Common's generated API matches the vendored copy before
building. The runtime image contains the server and embedded assets, not game
modules, game secrets, or local database contents.

Individual builds:

```sh
docker buildx build --load --target runtime \
  --build-context nakama-common=../nakama-common \
  -f build/Dockerfile.cardgame -t cardgame/nakama:3.41.0-cardgame.1 .
docker buildx build --load --target pluginbuilder \
  --build-context nakama-common=../nakama-common \
  -f build/Dockerfile.cardgame -t cardgame/nakama-pluginbuilder:3.41.0-cardgame.1 .
```

For a different host architecture, build both targets for the same chosen
platform using `--platform`. Do not assume a locally built ARM64 image can run
on an AMD64 production host.

Go plugins share package fingerprints with the host process. The changed Common
protobuf package means existing plugins built against upstream Common must be
rebuilt with the matching custom pluginbuilder before use. Its wrapper creates
a temporary Go modfile that replaces Common with `/opt/nakama-common`; it leaves
the game's go.mod/go.sum unchanged. Use the same Go compiler, dependencies,
architecture and `-trimpath` flags as the server. Always verify actual plugin
loading on the custom runtime, not just compilation.

For a preview without recompiling Go, generate assets into a separate directory:

```sh
node --test console/custom/*.test.mjs
node console/custom/build.mjs /tmp/nakama-console-preview
```

Serve these files with a local development proxy to the Console API. A deployed
Nakama binary does not read edited JS from disk: embedded assets require rebuilding
the binary/image and recreating the container.

## Verification and upgrade checklist

1. Run Common's API tests, the custom Node tests and `go test ./console/...`.
2. Build runtime and matching pluginbuilder; rebuild the game plugin and confirm
   it loads into the custom runtime without package-version errors.
3. In an isolated database, create a disposable player and purchase rows with
   stores 10/11. Check both the purchase table and each detail panel, including
   the global Payments page.
4. In the player's Notifications page, verify there are no additional page tabs.
   Open Send Notification and verify both popup tabs, preserved native drafts,
   both send paths, resource add/remove rows, all three custom sends, validation,
   errors, close/reopen behavior and keyboard tabs.
5. Read native notifications and confirm `code=17` plus exact payloads. Verify
   wallet balances and wallet ledger did not change.
6. Test route changes, another selected player, dark theme and narrow layout.
   Recheck the extension adapter after upstream UI upgrades.
7. Use a fresh browser reload to verify the new main asset hash and gzip variant.
   Confirm Console ACLs still reject unauthorized sends.
8. Keep images local until explicitly approved for pushing. Do not put game
   API keys or production credentials in the runtime/pluginbuilder image.


## Shared currency selector and wallet grant popup

Both Resource Grant notification rows and the Edit Wallet popup's new
**Send Resource** tab use resource-picker.mjs. It fetches
`admin/bank_items` through authenticated Console CallRpcEndpoint. Only the
server BankDef Currencies map is loaded; Items/Bundles never appear. The current
schema has no friendly names, so Name equals each currency key. Each option
shows only this name, without an ID/kind subtitle.

The selector's button is not editable. Opening it displays a search box and
scrollable choices. Typing filters choices only; clicking an option sets the ID.
Submit is disabled until every row has a valid selection and integer amount.
Removed/unknown restored currencies display a red border and block submission.
Wallet grants omit Infinite-prefixed currencies and SetOperator counters because
the operation only adds to native wallet balances. Notification-only notices can
list all currencies already changed separately. Catalog fetch failure is visible
and retryable; News/Message with Link remain usable without a currency catalog.

Native Edit Wallet opens first and keeps its original Monaco JSON editor and
Save behavior. Its second tab calls admin/grant_resources with the fixed player,
message, currency deltas and a new OperationId UUID. The game backend commits
native users.wallet, wallet_ledger and native notification in one serializable
transaction. The ledger records admin, source and reason. A repeated OperationId
returns the existing result and cannot add the same grant twice. Uncertain
requests are retained in sessionStorage and restored on reopening; their contents
must be retried unchanged before starting another grant. Explicit backend
validation rejection permits fixing input; unknown server/network errors retain
the same ID. Refresh the native Wallet view to reload wallet and ledger data.

## Purchase Correction

Payments retains Purchases and Subscriptions and adds a third verification tab.
It submits the selected user's token and store to admin/purchase_verify. The
configurable correction message is sent only by admin/purchase_correct.
Backend responses control whether Correct Purchase appears; correction always
repeats authoritative checks rather than trusting the browser's verification.

The game backend checks native delivery first, validates store records and payload
ownership, and only permits eligible undelivered purchases. Bazaar/Myket tokens
need a SKU, so the backend searches the server shop catalog (bounded to 100
products and a deadline) or uses an existing pending receipt's SKU. Google reads
the token's SKU from productsv2. Removed products with no receipt snapshot cannot
be inferred. Configure Google service-account credentials before using it live.
The correction consumes before granting, shares the normal purchase pipeline,
and commits native purchase/wallet/ledger/Resource Grant notice together. A trusted
pending reservation supports recovery if consumption succeeded but commit failed.
An already-delivered purchase never grants or notifies twice. Notifications only
describe rewards; sending a notification never changes the wallet by itself.

These RPCs use the existing authenticated Console API Explorer transport with
empty user_id (no impersonation). The runtime attaches the signed-in operator
only after checking native permissions. All need API_EXPLORER write, plus:

| RPC | Additional permission |
| --- | --- |
| admin/bank_items | STORAGE_DATA read |
| admin/grant_resources | ACCOUNT_WALLET write + NOTIFICATION write |
| admin/purchase_verify | IN_APP_PURCHASE read |
| admin/purchase_correct | IN_APP_PURCHASE write + ACCOUNT_WALLET write + NOTIFICATION write |

Game handlers reject player-session, impersonated and HTTP-key calls, and require
the trusted operator. An upstream runtime without this bridge cannot invoke them.
Console audit metadata redacts Token (case-insensitively) for verification and
correction; optional game RPC I/O logging also excludes those requests. Native
Console auditing remains active. No Console protobuf/API generation is needed
for these tools; their contracts live in the game backend's adminconsole and
purchase modules. No additional notification storage/jobs/workers were added.

## Regression checks for admin tools

Run node --test console/custom/*.test.mjs. The Docker runtime build also executes
TestGameAdminRPCPermissions against the Linux server build. Game backend tests
cover Console guards, currency validation and native SQL grants/corrections;
opt-in integration tests use a separate database and mocked store HTTP responses.
Browser checks should include switching Payments -> Wallet -> Notifications,
reopening dialogs, preserving native forms, explicit currency search/selection,
disabled invalid submissions, notice-only sends and existing delivered tokens.
Use a disposable player/database for grants; never test correction with a real
purchase token unless that delivery is intended.

## Static currency artwork

`console/custom/currency-art.mjs` maps currency IDs to local PNG files in
`console/custom/currency-art/`. The initial snapshot copies the client
`Assets/SO/Singleton/CurrencyCollection.asset` art references: 49 configured
currency entries using 47 original PNGs. `currency-art/sources.json` records
which client file supplied each entry. Client assets and Unity settings are
not modified. No runtime connection to the client project is required.

Currency dropdown options, selected currencies and correction reward rows use
this mapping beside the name. Images are decorative, contained at 24px, and do
not add a second text line. Unknown IDs use the bundled default.svg; a mapped
image that fails to load also switches to that default. The mapping does not
control which currencies can be selected: BankDef remains authoritative.

To supply art for another currency, copy its PNG into currency-art, add its exact
ID/path to currency-art.mjs and record its client source in sources.json, then
rebuild the console/image. New BankDef currencies without a mapping appear with
the default icon automatically. The build copies the art directory and helper
module alongside the dashboard; store credentials and backend APIs are unchanged.

## Shared notification defaults and global messages

Settings -> Configuration includes a Game Notifications card following the native
card/form styling. It edits game_purchase_correction_message and
game_resource_grant_message through the existing Console setting endpoints.
Values are JSON strings in Nakama's native setting table. Before first save, the
server returns built-in defaults without creating rows or requiring migrations.
Updates upsert only these supported keys, reject empty/non-string/overlong values,
and retain native SETTINGS write authorization and audit logging. UTC behavior is
unchanged. ListSettings returns both configured values and built-in fallbacks.

notification-settings.mjs owns loading/editing these settings and initializes new
Purchase Correction and Send Resource forms. Requests fetch current shared values;
user edits made during loading are preserved. Pending resource-grant retries keep
their original message so their operation fingerprint remains unchanged. Individual
sends can still edit the default text.

The global #/notifications Send Notification popup now uses the same Game
Notification component with allUsers=true. Its fixed recipient is All players;
only Message (type2) and Message with Link (type3) are available. It does not load
BankDef or provide resource controls. The payload builder rejects resource
broadcasts and mixed single-user/broadcast targets. Empty user_ids invokes the
native Console broadcast path; no custom delivery system is introduced. The
original Nakama notification tab remains available. Per-player Game Notification
still offers Resource Grant (type1) plus the two message types. Only labels changed;
the game's numeric payload contracts remain compatible.

The new source files are console/custom/notification-settings.mjs (settings UI and
form defaults) and server/console_setting.go (native setting defaults, validation
and persistence). The shared popup and route adapter handle both notification
entry points. Regression tests cover native JSON setting strings/defaults, invalid
updates and broadcast payload restrictions; browser verification checks saved
settings across Configuration, Purchase Correction and Send Resource.

## Docker Hub release

The deployment pair is sadeghdev/nakama:3.41.0-cardgame.2 and
sadeghdev/nakama-pluginbuilder:3.41.0-cardgame.2 for linux/amd64. Publish with:

```sh
IMAGE_NAMESPACE=sadeghdev VERSION=3.41.0-cardgame.2 PLATFORMS=linux/amd64,linux/arm64 PUSH=true BUILDER=multi-builder ./build/cardgame.sh ../nakama-common
```

PUSH defaults to false. The matching plugin builder applies a temporary Go module
replace to /opt/nakama-common; backend go.mod and vendor files stay unchanged.
The backend Compose file and Dockerfile must use the published pair. Deploy by
pulling both images, compiling the game plugin, then recreating Nakama. No server
checkout updates or deployments are performed by this image-publishing command.

Purchase Correction reuses native tab classes, inner font-medium markup and a
native-style active indicator. All three subtabs keep identical padding/height;
the correction form aligns with native content without an extra outer card.


### Client item artwork

`console/custom/currency-art.mjs` maps item IDs to static client images used by the shared resource picker and purchase rewards. The map covers CharacterCollection (portraits resolved through AssetCollection), CurrencyCollection, GearCollection and AvatarAssetCollection. Character portraits also supply shard icons when CurrencyCollection has no image. PachinkoToken uses its reward prefab sprite; SmokeBomb level four uses its existing client sprite. Progression counters use the client level icon.

To refresh from the client checkout, run `python3 console/custom/export-item-art.py /path/to/cardgame-client /path/to/nakama/console/custom`, then rebuild the console. The exporter reads assets without changing the Unity project. `currency-art/sources.json` records each source, and `coverage.json` lists unresolved current IDs and obsolete entries. Confirmed obsolete IDs are excluded from the picker through `obsolete-items.mjs`; BankDef and existing user data are preserved. New IDs without artwork retain the default icon until the map is refreshed. Dropdown options override the popup button margin and use compact 34px rows.

The shared resource picker shows one Currency/Value header row and a numbered left column. Row positions and accessible labels are renumbered after removal; dropdown IDs remain stable. This layout is shared by resource notifications and wallet grants.

Both player and global Game Notification tabs offer only Message and Message with Link. Resource notifications are sent by Wallet → Send Resource (or purchase correction), together with the associated grant; the notification popup does not load a resource picker. Native Nakama notification forms are unchanged.

Image releases use a shared multi-platform tag for linux/amd64 and linux/arm64. Docker selects the matching host architecture. Both runtime and pluginbuilder must use the same release tag; backend Compose does not force a platform.
