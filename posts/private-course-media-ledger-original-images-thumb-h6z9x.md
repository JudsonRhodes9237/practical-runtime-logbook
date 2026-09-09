# Private Course Media Ledger: Original Images, Thumbnails, WebP, AVIF, and Overwrite Safety

For private course media, favor access control over the convenience of a stable object URL: give every upload revision a new immutable prefix, record the published revision in a database manifest, and mint an expiring download link only after authorization. **Short answer: names should describe immutable bytes; a separate manifest should say which original and thumbnail set is current.** This prevents an old resize job from overwriting a newer teacher upload while still supporting multiple sizes, WebP, AVIF, retained originals, and deliberate backups.

I've been paged by missed jobs and duplicate queue deliveries. The useful lesson is that delivery count can't be allowed to decide publication state. In an edtech upload flow, a learner replacing a private portfolio image can produce two valid resize jobs at once: the older delivery may finish last, and a mutable key such as `students/731/avatar/640.webp` silently gives that late job permission to publish stale bytes. Nothing has to crash. The wrong image just wins.

That is an authorization bug wearing a file-naming costume.

## The incident invariant: completed bytes never mean current

Treat the durable media ID, the upload revision, and the transformation recipe as different facts. A practical tree might look like this:

```go
courses/course_184/submissions/media_731/revisions/rev_01/original/upload.jpg
courses/course_184/submissions/media_731/revisions/rev_01/variants/w320-h180-fit-cover/codec-webp-r2/image.webp
courses/course_184/submissions/media_731/revisions/rev_01/variants/w320-h180-fit-cover/codec-avif-r2/image.avif
courses/course_184/submissions/media_731/revisions/rev_01/variants/w1280-h720-fit-inside/codec-webp-r2/image.webp
courses/course_184/submissions/media_731/backups/rev_00/original/upload.jpg
```

Here `media_731` is a stable application identifier, while `rev_01` identifies one accepted upload. The variant path includes every input that can change output bytes: width, height, fit policy, codec, and recipe revision. A crop mode, color policy, or encoder setting belongs in the recipe revision if changing it would change the file. Don't place `current` or `latest` in an object key.

The database manifest holds the mutable decision instead. At minimum it records the media ID, owner or access scope, published revision, source media type, available variants, and lifecycle state. A renderer may finish `rev_01` after `rev_02`, but it can only mark its own revision ready. Publication uses a compare-and-set transaction against the expected revision; storage writes don't get to move that pointer.

This split gives the runbook a clean question. If a learner reports the wrong image, inspect one manifest row and one revision tree. Operators don't need to infer chronology from timestamps, listing order, or filenames supplied by a browser. The object tree answers “what bytes exist?” The manifest answers “what may be served?” Keep those answers separate.

## How should object storage name original images, multiple thumbnail sizes, WebP, and AVIF?

Use opaque, server-generated IDs for identity and normalized tokens for transformations. Preserve the original extension only after validating the actual upload type; never trust a caller's filename to choose a storage path or content type. OWASP's file upload guidance recommends generating filenames in the application, allowlisting extensions, validating file signatures rather than trusting the `Content-Type` header, setting size limits, restricting uploads to authorized users, and storing files away from the web root. Those controls matter more than a pretty directory layout.

The naming grammar can stay small:

```go
<tenant-scope>/<asset-id>/revisions/<revision-id>/<kind>/<recipe>/image.<format>
```

`kind` is either `original` or `variants`. The original has no resize recipe. A variant recipe is canonical: fixed token order, lowercase values, explicit dimensions, and a recipe revision. Avoid user email addresses, student names, course titles, or original filenames. Object keys leak into logs, traces, inventory exports, and support screenshots — private bytes deserve private metadata too.

Backups need a precise meaning. Keeping the previous immutable revision for a rollback window is useful, but naming a prefix `backups` doesn't create a backup system. A recoverable backup needs an independently tested restore procedure, retention policy, access boundary, and failure domain chosen for the required risk. If the product only needs “undo the last avatar replacement for seven days,” retained revisions may be enough. If it needs disaster recovery or regulated retention, treat backup as its own system and test it accordingly.

Here is a focused Go key builder. It accepts only normalized application values, produces the same destination for a duplicate job, and never embeds publication state:

```go
package media

import (
	"fmt"
	"path"
	"regexp"
)

var opaqueID = regexp.MustCompile(`^[a-z0-9_]{3,64}$`)

type Variant struct {
	Width    int
	Height   int
	Fit      string
	Format   string
	RecipeID string
}

func VariantKey(scope, assetID, revisionID string, v Variant) (string, error) {
	for label, value := range map[string]string{
		"scope": scope, "asset ID": assetID, "revision ID": revisionID,
	} {
		if !opaqueID.MatchString(value) {
			return "", fmt.Errorf("invalid %s", label)
		}
	}
	if v.Width < 1 || v.Height < 1 || v.Width > 4096 || v.Height > 4096 {
		return "", fmt.Errorf("dimensions out of policy")
	}
	if v.Fit != "cover" && v.Fit != "inside" {
		return "", fmt.Errorf("unsupported fit policy")
	}
	if v.Format != "webp" && v.Format != "avif" {
		return "", fmt.Errorf("unsupported output format")
	}
	if !opaqueID.MatchString(v.RecipeID) {
		return "", fmt.Errorf("invalid recipe ID")
	}

	recipe := fmt.Sprintf(
		"w%d-h%d-fit-%s-codec-%s-%s",
		v.Width, v.Height, v.Fit, v.Format, v.RecipeID,
	)
	return path.Join(scope, assetID, "revisions", revisionID, "variants", recipe, "image."+v.Format), nil
}
```

The `4096` ceiling is an application policy in this example, not a storage limit. Set it from the largest rendition the product actually serves, then test the exact boundary. The important behavior is determinism: the same revision and recipe produce the same key, so retrying a duplicate queue delivery is harmless.

## Publish with a manifest, then authorize every read

The worker path should be boring. First create a revision record in a transaction, then upload the validated original under its immutable key. Enqueue one job containing the revision ID and an explicit recipe set. Each worker writes to deterministic destinations and records completion against that revision. Only after every required variant exists does a transaction publish the revision, and only if the asset still expects it. Optional formats can be recorded independently rather than blocking the required set forever.

A download request begins somewhere else: at the application authorization boundary. The caller asks for `media_731` and a rendition class, not a raw object key. The application verifies tenant, enrollment, assignment, or staff access; resolves the published revision; selects an available format using the request policy; and returns an expiring download link. The object store handles byte delivery. It doesn't decide whether a learner may see another learner's submission.

Keep the link lifetime short enough for the sensitivity of the media and the expected transfer duration. I'm not sure there is one defensible universal duration: a small profile thumbnail on a campus network and a large original on a slow mobile connection have different requirements. Access logs and observed download times resolve that choice better than folklore. Whatever value the team selects, test expiry, clock skew, replay within the allowed window, authorization revocation, and a request for an unpublished revision.

Don't persist signed links as if they were durable asset URLs. Cache the immutable object behind the delivery layer when policy permits, but mint or refresh authorization at the application edge. If access is revoked, an already issued link may remain usable until it expires; that is the catch in exchange for simple direct delivery. Highly sensitive material may require application-proxied downloads or much shorter link lifetimes instead.

## Failure drills belong in the design

The happy-path naming test is necessary and weak. The revealing test starts upload A, starts upload B for the same media ID, lets B publish, then releases A's delayed job. The manifest must still point to B. Repeat the job for B twice; the object set and manifest should be unchanged. Kill a worker after the original write but before variant completion; the revision should remain unpublishable and eligible for a bounded cleanup process.

Also test authorization as data, not as a UI assumption. One learner requests another learner's media ID. A course staff role is removed after a link is issued. A deleted assignment still has retained revisions. A request asks for `../original` as a rendition. The handler should select from an allowlisted rendition map and construct keys from validated identifiers, never append request path fragments.

Observability follows the same model. Put asset ID, revision ID, recipe ID, queue delivery ID, and publication outcome in structured events. Avoid logging signed query strings or private filenames. Useful counters include accepted uploads, rejected validations by reason, variant duration by recipe, duplicate deliveries, publication conflicts, authorization denials, and cleanup candidates. An alert should describe a user-visible invariant — required variants have not become ready within the agreed processing window — rather than firing on every retry.

Deploy recipe changes as new recipe IDs. Render the new set beside the old set, verify it, update the manifest, and retire the old set according to retention policy. Don't rewrite all `w320` objects in place during an encoder rollout. That turns rollback into archaeology.

## Where this pattern is the wrong trade-off

The manifest adds a database lookup, transactional state, cleanup work, and another thing to reconcile. It is not suitable when every object is intentionally public, content-addressed, and permanent; a hash-derived key plus ordinary cache semantics may be simpler there. Stick with direct provider-native access controls when a team needs storage-specific retention locks, replication controls, audit integrations, or conditional operations that the generic manifest layer would merely imitate.

Application-proxied delivery is also the better choice when authorization must be evaluated for every byte request or revocation must take effect immediately. Expiring links trade that immediacy for delivery simplicity. There is no filename pattern that removes the trade-off.

For private edtech thumbnails, though, the decision rule is stable: immutable revision keys protect overwrite safety, the manifest serializes publication, and expiring links enforce a bounded read decision. Formats will change. Sizes will change. The authority to publish should not be hidden inside either one.

## Sources

- https://cheatsheetseries.owasp.org/cheatsheets/File_Upload_Cheat_Sheet.html
- https://vercel.com/docs/vercel-blob
