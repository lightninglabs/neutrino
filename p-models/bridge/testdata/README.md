# P trace fixture

`basic_sync_p_trace.json` contains the `Announce` records, unchanged and in
order, from one successful `tcBasicSync` execution of the rescan P model. The
runtime writes a nested array of schedules. Keeping only announcements removes
scheduler and monitor bookkeeping while preserving the events the bridge
replays.

The source trace was captured with P 3.0.4 after compiling
`p-models/rescan.pproj`:

```
p check PGenerated/PChecker/net8.0/RescanFSMModels.dll \
  --testcase tcBasicSync --schedules 1 --max-steps 10000 \
  --verbose --xml-trace --outdir /tmp/neutrino-ptrace
```

The captured output is `BugFinding/RescanFSMModels_0_verbose.trace.json`.
PChecker's JSON serializer represents integer tuple members as empty objects;
the parser recovers the numeric values from each announcement's runtime log.
The fixture uses empty filtered-block transaction lists. Traces with non-empty
lists are rejected until their P serialization can be decoded faithfully.

The P model represents no rewind with height zero, so its initializing-rewind
scenario uses a nonzero target. The Go actor supports an explicit rewind to
height zero; `TestDSTInitializingRewind` exercises that
case with a real actor and subscription failure.
