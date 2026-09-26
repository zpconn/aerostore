#!/usr/bin/env python3
"""Refresh document links/hashes against an already passed immutable content audit."""
from datetime import datetime,timezone
import hashlib,json,re,shutil
from pathlib import Path
from urllib.parse import unquote,urlsplit
ROOT=Path(__file__).resolve().parents[2]
BASE=Path(__file__).resolve().parent
DEST=ROOT/'docs/bench_data/retry_2026-09-26'
def sha(path):return hashlib.sha256(path.read_bytes()).hexdigest()
audit=json.loads((DEST/'archive-review.json').read_text()); manifest=json.loads((DEST/'audit-manifest.json').read_text());errors=[]
if not audit['passed']:errors.append('deep content audit did not pass')
if sha(DEST/'artifact-manifest.json')!=audit['manifest_sha256'] or manifest['main_artifact_manifest_sha256']!=audit['manifest_sha256']:errors.append('main content manifest changed after deep audit')
for item in manifest['files']:
 p=DEST/item['path']
 if sha(p)!=item['sha256'] or p.stat().st_size!=item['bytes']:errors.append('separate audit artifact changed: '+item['path'])
docs={};links=[];deferred=[];changes=[]
for name,old in audit['document_sha256'].items():
 p=ROOT/name;docs[name]=sha(p)
 if old!=docs[name]:changes.append({'path':name,'previous_sha256':old,'current_sha256':docs[name]})
 for raw in re.findall(r'\[[^\]]*\]\(([^)]+)\)',p.read_text()):
  target=raw.strip().split(' "',1)[0].strip('<>');parts=urlsplit(target)
  if parts.scheme or parts.netloc or not parts.path:continue
  resolved=(p.parent/unquote(parts.path)).resolve();exists=resolved.exists()
  if not exists and resolved.is_relative_to(DEST/'guardrails'):deferred.append({'document':name,'link':target});continue
  links.append({'document':name,'link':target,'exists':exists})
  if not exists:errors.append('broken relative link '+name+': '+target)
if any(c['path']!='docs/bench_data/retry_2026-09-26/README.md' for c in changes):errors.append('unreviewed document changed outside archive README')
result={'passed':not errors,'errors':errors,'created_at':datetime.now(timezone.utc).isoformat(),'scope':'Lightweight document-only refresh after successful main content audit. Verifies frozen main-manifest identity and separately archived audit hashes; does not repeat decompression or proof checks. Archive README may add audit links and later the completed guardrail result; substantive source/data changes require a fresh content review.','main_artifact_manifest_sha256':audit['manifest_sha256'],'deep_audit_sha256':sha(DEST/'archive-review.json'),'previous_audit_manifest_sha256':sha(DEST/'audit-manifest.json'),'review_script_sha256':sha(Path(__file__)),'document_sha256':docs,'document_only_changes':changes,'relative_links':links,'deferred_guardrail_links':deferred}
output=BASE/'archive-doc-review.json';output.write_text(json.dumps(result,indent=2)+'\n')
if result['passed']:
 for name in ('review_archive_links.py','archive-doc-review.json'):shutil.copyfile(BASE/name,DEST/name)
 names=sorted({e['path'] for e in manifest['files']}|{'review_archive_links.py','archive-doc-review.json'})
 manifest['files']=[{'path':name,'source':str((BASE/name).relative_to(ROOT)),'sha256':sha(DEST/name),'bytes':(DEST/name).stat().st_size} for name in names]
 (DEST/'audit-manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')
print(json.dumps({k:result[k] for k in ('passed','errors','document_only_changes','deferred_guardrail_links')},indent=2))
raise SystemExit(0 if result['passed'] else 1)
