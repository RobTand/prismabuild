#!/usr/bin/env python3
"""Refuse test and GPU work that does not go through PrismaBuild.

Briefs are discipline, not enforcement, and discipline is what failed: the
same briefs that said "use the pool" produced fifteen agents recomputing one
baseline and a flock jam seven deep.  This hook moves the rule from prose an
agent may skim to a refusal it cannot.

Recognized test runners and GPU containers are also refused. The global agent
policy covers indirect execution this lexical guard cannot prove. Read-only inspection (``nvidia-smi``, ``squeue``,
``sinfo``) is allowed, because refusing it would only teach agents to route
around the hook.  What is refused is the things that actually contend for the
GPU: the CUDA venv interpreter, the box-local flock wrappers the pool replaces,
and -- since the fleet has a scheduler -- a bare ``sbatch``, ``srun`` or
``salloc``.

That last one is not a fourth rule, it is the same rule reaching the shape that
defeats its proxy.  The proxy for GPU work is the CUDA interpreter on the
command line, and a submission names no interpreter at all: the venv is inside
the job script, on a node this box never sees.  So the one shape that most
needs to go through the lane was the one shape the hook let through.
``scancel`` is deliberately left alone: cancelling a job is not starting work,
and a guard that refuses the cleanup strands the person complying with it.

Two carve-outs, and both are the same lesson.  A guard that can refuse its own
repair, or refuse the alternative it names, is worse than no guard: it strands
the person trying to comply.  So commands that never start GPU work are let
through by name, and so is the pool's own machinery -- including starting a
worker, whose ``--python`` argument is the CUDA venv path by design.

The same lesson reaches one shape further in.  A command that hands prose to a
program -- an agent posting "shard 3 died under the runner" to the mailbox --
is a mention of fleet work, not an instance of it, and refusing it makes the
mailbox lossy in the case it is most needed: nobody can warn about a runner
failure without the warning being mistaken for the failure.  So in a segment
that is an interpreter running a SCRIPT FILE, quoted arguments are read as
prose (see ``_prose_start``).  Nothing that executes an argument qualifies --
``python -c``, ``python -m``, a shell, a wrapper, or a launcher forwarding its
trailing argv -- because those are lexically identical to the mailbox and the
difference is what runs the words.

Staged rather than flipped: enforcement requires the flag file to exist, so
turning it on is one ``touch`` and does not edit config under running agents.
"""
from __future__ import annotations

import json
import os
import re
import sys
from pathlib import Path

FLAG = Path(os.environ.get("REQUIRE_POOL_FLAG", "/home/rob/tmp/arb/require_pool.on"))
sys.path.insert(0, str(Path(__file__).resolve(strict=True).parent))
from runtime_paths import fleet_tool, generation_root  # noqa: E402

_RUNTIME_ROOT = generation_root(__file__)
#: The submitter the refusals below tell an operator to run.  Resolved under
#: both layouts, because a hook running from a checkout would otherwise print
#: the published runtime's flat path, which a checkout does not have.
PBRUN = str(
    fleet_tool("pbrun.py", root=_RUNTIME_ROOT)
    or _RUNTIME_ROOT / "tools" / "pbrun.py"
)

# The CUDA interpreter, the flock wrappers, and a bare flock on the GPU lock.
# That last one is not hypothetical: the wrappers were only ever a convenience,
# and the jam this hook exists to prevent re-formed from direct `flock
# .../.gpu.lock ...` invocations that named no wrapper at all -- one pytest
# holding an exclusive GPU lock for 73 minutes with a `docker run --gpus all`
# waiting 53 minutes behind it.  Matching the wrappers alone would have left
# the observed path open.
#
# Anything else -- nvidia-smi, a CPU python, a git command -- is none of this
# hook's business.
CONTENDS = re.compile(
    r"venvs/prismaquant-cu130/bin/python"
    r"|/gpuslot\.sh"
    r"|/gpulock\.sh"
    r"|flock\s[^|;]*\.gpu\.lock"
)


#: Commands that never start GPU work, whatever their text contains.  A commit
#: message quoting the refused pattern is prose ABOUT the rule, not an instance
#: of it -- and this hook refused its own commit, then refused the edit that
#: would have fixed that, before this existed.  A guard that can lock out its
#: own repair is a worse failure than the one it guards against.
NEVER_GPU = (
    "git", "gh", "echo", "cat", "rg", "grep", "sed", "awk", "less", "diff",
    # Asking where a program is, what it is, or what it says about itself.
    # ``which sbatch`` and ``man sbatch`` are the first two commands anyone
    # runs at a scheduler they have never used, and refusing them teaches the
    # reader to route around the hook before they have read the rule.
    "which", "whereis", "type", "man", "ls", "stat", "file",
    "head", "tail", "wc", "dpkg", "apt", "apt-get", "apt-cache",
)


#: ``command`` is NOT in the list above, because it is not a command that
#: never starts work: ``command sbatch job.sh`` submits the job, and the whole
#: segment was exempt for as long as the builtin's name led it.  Its two
#: inspection switches are the part that must stay allowed -- ``command -v
#: sbatch`` prints a path and runs nothing, and it is in the runbook -- so
#: those are exempted by shape rather than by the builtin's name.  ``-p`` is
#: accepted between the two because it only chooses the default PATH.
INSPECTION_SWITCHES = frozenset({"-v", "-V"})


#: A segment that switches the GPU off for its own child cannot be GPU work,
#: whatever interpreter it names.  Two agents in one day were refused for
#: ``kl_tool.py --help`` and ``kl_tool.py compare --help`` -- argparse text
#: that starts nothing -- because the rule's proxy is the interpreter path and
#: nothing asked what the command does.  The interpreter stays the proxy: it
#: is the deliberate one, and asking "does this touch CUDA" of an arbitrary
#: command line is exactly the guess this hook refuses to make.  What this
#: adds is a way to *say so*, and the saying is enforced by the kernel rather
#: than believed: with ``CUDA_VISIBLE_DEVICES`` empty the child sees no
#: device, so this cannot become a way around the pool -- work smuggled
#: through it would simply fail.  ``--help`` under that prefix is the use.
NO_DEVICE = re.compile(r"""(?:^|\s)CUDA_VISIBLE_DEVICES=(?:''|""|)(?=\s)""")


#: The pool's own machinery, which must never be refused by the hook that
#: exists to route work *into* it.  ``pbrun`` submits; ``worker_loop`` and
#: ``worker`` are the things that consume the queue -- and a worker is
#: configured with ``--python <the CUDA venv>``, so it names the refused
#: pattern by construction.  Refusing that means the hook blocks the pool from
#: being started at all, which is the same class of failure as refusing its own
#: repair: the guard removing the alternative it is pointing at.
#:
#: ``slurm_job.py`` joins them for both reasons at once: it is what a SLURM job
#: execs on the node, so it *is* the sanctioned path, and it carries the
#: action's own sealed interpreter -- the CUDA venv -- on its command line.
POOL_ENTRYPOINTS = ("pbrun.py", "pbtest.py", "pbcampaign.py",
                    "worker_loop.py", "worker.py", "slurm_job.py")


#: A submission to the scheduler, by any of its three verbs.  Bounded by
#: non-word characters on both sides so that ``/usr/bin/sbatch`` is one and
#: ``my-sbatch-wrapper`` and ``sbatch.sh`` are not -- and so that ``scancel``,
#: ``squeue`` and ``sinfo`` never match: reading the queue and cancelling a job
#: start no work.
SCHEDULER = re.compile(r"(?<![\w.-])(?:sbatch|srun|salloc)(?![\w.-])")


#: The verbs' own help and version switches.  ``sbatch --help`` submits
#: nothing, and it is how a reader of the runbook finds out whether the
#: scheduler is installed at all.  Only a segment that is the verb and these
#: switches and nothing else is let through: ``sbatch --help job.sh`` also
#: submits nothing in practice, but the hook does not parse the verbs' grammar
#: and does not guess.  A bare ``sbatch`` with no switch reads a script from
#: standard input, which is a submission.
DESCRIBES_ITSELF = frozenset({"-h", "--help", "--usage", "-V", "--version"})


def _describes_itself(segment: str) -> bool:
    """True when this segment only asks a scheduler verb about itself."""

    tokens = _command_tokens(segment)
    if len(tokens) < 2:
        return False
    if tokens[0].rsplit("/", 1)[-1] not in ("sbatch", "srun", "salloc"):
        return False
    return set(tokens[1:]) <= DESCRIBES_ITSELF


#: Shell words that stand in front of a command without being one.  A
#: compound command's boundaries put these at the head of a segment -- ``for f
#: in *.py; do grep sbatch $f; done`` runs ``grep``, and the segment the
#: boundaries hand over begins ``do`` -- so a segment led by one of these was
#: judged as an unknown command and refused.  That is the same failure as
#: judging a compound command by its first token, one level in: the word the
#: exemption has to look at is the command word, not whatever is leftmost.
#: ``for``, ``case`` and ``select`` are deliberately absent: the word after
#: those is a variable name, not a command, so stripping them would judge a
#: loop by its loop variable.
RESERVED_WORDS = frozenset({
    "!", "{", "if", "elif", "then", "else", "while", "until", "do", "time",
})


def _command_tokens(segment: str) -> list[str]:
    """The segment's words, past the prefixes that are not the command."""

    tokens = segment.split()
    while tokens:
        head = tokens[0]
        if head in RESERVED_WORDS:
            tokens.pop(0)
            continue
        if "=" in head and not head.startswith("/"):
            tokens.pop(0)          # a variable assignment prefixing a command
            continue
        break
    return tokens


def _first_token(command: str) -> str:
    """The command actually being run, past the words that are not it."""

    tokens = _command_tokens(command)
    # ``command X ...`` runs X, so X is the command being judged.  ``-p`` is
    # accepted between them because it only chooses the default PATH.
    while tokens and (tokens[0].rsplit("/", 1)[-1] == "command"
                      or tokens[0] == "-p"):
        tokens.pop(0)
    return tokens[0].rsplit("/", 1)[-1] if tokens else ""


def _inspects_only(segment: str) -> bool:
    """True when this segment only asks ``command`` where a program is."""

    tokens = _command_tokens(segment)
    if not tokens or tokens[0].rsplit("/", 1)[-1] != "command":
        return False
    rest = [token for token in tokens[1:] if token != "-p"]
    return bool(rest) and rest[0] in INSPECTION_SWITCHES


def _drop_pool_payload(command: str) -> str:
    """Cut a pool submission's payload off before the command is segmented.

    ``pbrun.py --gpu -- <cmd>`` hands everything past ``--`` to the pool, which
    execs it on a worker that already holds the reservation.  Segmenting that
    payload tears the interpreter away from the entrypoint that vouches for it,
    so a submission whose payload was ``bash -lc 'cd x && <cuda venv> -m
    pytest'`` was refused *as off-pool GPU work* -- the hook locking out the
    very submission it exists to require.

    The cut is deliberately narrow: it needs a pool entrypoint AND a ``--``
    after it, because that pair is what makes the rest argv for another
    process.  A chain like ``pbrun.py --help && <cuda venv> train.py`` has no
    ``--``, so its second segment is still scanned and still refused.

    It is applied to ONE segment, after the boundaries are known, and that is
    the correction issue #89 names.  Cutting the raw command line at the first
    entrypoint's ``--`` threw away everything after it, including commands
    that belong to the outer shell and never reach pbrun: ``pbrun.py --gpu --
    true && <cuda python> train.py`` runs the second command locally, and
    ``pbrun.py -- true && sbatch job.sh`` submits locally, and neither was
    scanned.  A payload that really is argv keeps its ``&&`` inside quotes,
    which the segmenter does not split on, so the payload the cut exists for
    is still one segment.
    """

    best = None
    for entry in POOL_ENTRYPOINTS:
        at = command.find(entry)
        if at < 0:
            continue
        sep = re.search(r"\s--\s", command[at:])
        if sep is None:
            continue
        cut = at + sep.start()
        if best is None or cut < best:
            best = cut
    return command if best is None else command[:best]


#: A here-document opener, as the shell would read one.  The delimiter may be
#: quoted, which is what decides whether the body is expanded -- and POSIX
#: gives three spellings of "quoted", not one: ``<<'EOF'``, ``<<"EOF"`` and
#: ``<<\\EOF`` all stop expansion.  Matching only the first two read a
#: backslash-quoted delimiter as an unquoted one and scanned a body the shell
#: copies verbatim.
HEREDOC = re.compile(
    r"""<<-?\s*(?:"""
    r"""(?P<mark>['"])(?P<quoted>[A-Za-z_][A-Za-z0-9_]*)(?P=mark)"""
    r"""|\\(?P<escaped>[A-Za-z_][A-Za-z0-9_]*)"""
    r"""|(?P<bare>[A-Za-z_][A-Za-z0-9_]*)"""
    r""")"""
)


def _heredoc_tag(match: re.Match[str]) -> tuple[str, bool]:
    """The delimiter this opener names, and whether it stops expansion."""

    for group in ("quoted", "escaped"):
        if match.group(group) is not None:
            return match.group(group), True
    return match.group("bare"), False

#: Programs that execute their standard input.  A here-document fed to one of
#: these is a script, not data on its way to a file, and scanning it is the
#: whole point rather than the mistake: ``bash <<'EOF'`` with the CUDA
#: interpreter inside it starts exactly the work this hook refuses.
INTERPRETERS = ("bash", "sh", "dash", "zsh", "ksh")


def _is_interpreter(name: str) -> bool:
    return name in INTERPRETERS or re.fullmatch(r"python[0-9.]*", name) is not None


#: Commands that run another command given to them as an argument.  The
#: interpreter reading a here-document is not always the command word of the
#: segment that owns it: ``ssh box bash``, ``sudo bash`` and ``docker exec -i
#: c bash`` all end in a shell that reads the body, and the first of those is
#: how routine work reaches the other boxes here.  Looking past the command
#: word only for these keeps ``grep -c python`` and ``cat bash_notes`` what
#: they are, which reading every word of every segment did not.
WRAPPERS = frozenset({
    "ssh", "sudo", "doas", "env", "nice", "ionice", "timeout", "nohup",
    "setsid", "stdbuf", "xargs", "docker", "podman", "kubectl", "chroot",
    "unshare", "taskset", "flock", "command", "time",
})


def _name_of(token: str) -> str:
    return token.strip("'\"").rsplit("/", 1)[-1]


#: Switches that hand an interpreter something to RUN rather than something to
#: read: python's ``-c`` and ``-m``, a shell's ``-c`` in its clustered
#: spellings, and ``sbatch --wrap``, whose value is a command line.  A segment
#: carrying one of these executes an argument, so none of its arguments can be
#: called prose.
CODE_SWITCHES = frozenset({
    "-c", "-m", "-e", "-lc", "-ic", "-cl", "-mc",
    "--command", "--eval", "--exec", "--wrap",
})


#: One quoted argument, in the two spellings an option's value takes.
QUOTED = re.compile(r"""'[^']*'|"[^"]*\"""")


def _words(segment: str) -> list[str]:
    """The segment's words, with their quotes kept.

    ``shlex`` would strip them, and the quotes are the signal: they are what
    separates a word that carries prose from a word that carries a path.
    """

    words: list[str] = []
    current: list[str] = []
    quote = ""
    for char in segment:
        if quote:
            current.append(char)
            if char == quote:
                quote = ""
            continue
        if char in "'\"":
            quote = char
            current.append(char)
            continue
        if char.isspace():
            if current:
                words.append("".join(current))
                current = []
            continue
        current.append(char)
    if current:
        words.append("".join(current))
    return words


def _prose_start(segment: str) -> int | None:
    """Where this segment's arguments stop being commands, if they ever do.

    Issue #209: a mailbox send was refused because the message body described
    a shard that had died under a test runner.  Nothing ran and no GPU was
    touched -- the command posts a JSON file to a directory -- but the runner's
    name in the ``--body`` prose matched the scan.  That is the failure this
    module's own docstring already names for a commit message, one caller
    later, and it makes the mailbox lossy in exactly the case it is most
    needed: one agent cannot warn another about a runner failure without the
    warning being mistaken for the failure.

    The exemption is keyed on a shape rather than on that caller's path,
    because the path is a caller outside this repo and the shape is a fact
    about execution: ``python x.py ...`` is the shell exec'ing an interpreter
    and the interpreter exec'ing a FILE.  Neither of them execs an argument,
    so a quoted argument of such a segment is prose ABOUT work, not work.

    Every way of handing an argument to something that runs it fails this
    test, and that is the load-bearing half:

    * ``python -c`` and ``python -m`` run the argument, so they return None.
    * A shell -- ``bash -lc "pytest"`` -- is not a script run at all.
    * A wrapper -- ``ssh box "pytest"``, ``docker run --entrypoint "pytest"``
      -- does not lead with a python interpreter, so it never gets here.
    * Nor does a launcher that forwards its trailing argv: ``uv run "pytest"``,
      ``systemd-run --pty "pytest"``, ``watch "pytest tests"``.  This is why
      the exemption is not "a quoted argument is prose": that reading is
      lexically identical to those, and would have opened a hole wider than
      the bug.

    The index returned is that of the first word past the script path.  The
    interpreter and the script path themselves are never elided, so a quoted
    command word -- ``'/…/prismaquant-cu130/bin/python' train.py`` -- is still
    read as the command it is.
    """

    words = _words(segment)
    index = 0
    while index < len(words):
        head = words[index]
        if head in RESERVED_WORDS or ("=" in head and not head.startswith("/")):
            index += 1
            continue
        break
    if index >= len(words):
        return None
    if re.fullmatch(r"python[0-9.]*", _name_of(words[index])) is None:
        return None
    index += 1
    while index < len(words):
        word = words[index]
        if word in CODE_SWITCHES:
            return None
        if word.startswith("-"):
            index += 1
            continue
        # The first thing that is not a switch is what python will run.  Only
        # a file makes this a script run; anything else is not read here.
        return index + 1 if _name_of(word).endswith(".py") else None
    return None


def _mentions_elided(segment: str) -> str:
    """The segment as the guard should read it, with its prose blanked out.

    Quotes are read lexically and escapes inside them are not tracked, so a
    body carrying an escaped quote ends its span early and the rest of the
    prose is scanned as command text.  That falls to the refusing side on
    purpose: the way to send prose this guard would misread is to hand it to
    the tool in a file, where it is never on a command line at all.
    """

    start = _prose_start(segment)
    if start is None:
        return segment
    words = _words(segment)
    return " ".join(words[:start]
                    + [QUOTED.sub(" ", word) for word in words[start:]])


def _feeds_an_interpreter(segment: str) -> bool:
    """True when this segment hands its standard input to an interpreter."""

    tokens = _command_tokens(segment)
    if not tokens:
        return False
    if _is_interpreter(_name_of(tokens[0])):
        return True
    if _name_of(tokens[0]) not in WRAPPERS:
        return False
    return any(_is_interpreter(_name_of(token)) for token in tokens[1:])


def _substitution(text: str, start: int) -> tuple[str, int]:
    """The body of the ``$( ... )`` at ``start``, and the index past its ``)``.

    Quotes and nested parentheses are tracked, so ``$(echo "a)b")`` ends at
    the right place.  An unterminated substitution yields the rest of the
    text, which is the conservative reading: what the shell would run if the
    caller finished typing.

    A here-document body inside the substitution is skipped rather than read
    as command text, because the body is prose as often as not and an
    apostrophe in it would otherwise open a quote that swallows the closing
    ``)`` and everything after it.  ``git commit -m "$(cat <<'EOF'`` with a
    body saying "doesn't" is the case that matters, and it is the shape this
    repo writes commit messages with.
    """

    begin = start + 2
    index = begin
    depth = 0
    quote = ""
    pending: list[str] = []
    while index < len(text):
        char = text[index]
        if quote:
            if char == "\\" and quote == '"' and index + 1 < len(text):
                index += 2
                continue
            if char == quote:
                quote = ""
            index += 1
            continue
        if char == "\\" and index + 1 < len(text):
            index += 2
            continue
        if char in "'\"":
            quote = char
            index += 1
            continue
        if text.startswith("<<", index):
            match = HEREDOC.match(text, index)
            if match is None:
                index += 2
                continue
            pending.append(_heredoc_tag(match)[0])
            index = match.end()
            continue
        if char == "\n" and pending:
            index = _past_bodies(text, index, pending)
            pending = []
            continue
        if char == "(":
            depth += 1
        elif char == ")":
            if depth == 0:
                return text[begin:index], index + 1
            depth -= 1
        index += 1
    return text[begin:], len(text)


def _pipeline_after(line: str, at: int) -> str:
    """The rest of the pipeline the command at ``at`` sits in.

    Everything from ``at`` up to the first boundary that ends a pipeline: a
    ``;``, a ``&&``, a ``||``, a ``&`` or a newline.  A ``|`` deliberately
    does not end it, because a pipeline is the one boundary that hands a
    command's output to the next command as input.

    Only a lone ``&`` ends it, on the same reading ``_commands_in`` uses: the
    redirection spellings that merely contain the character belong to the
    command they sit in, and ``cat <<'EOF' 2>&1 | bash`` still pipes its body
    into an interpreter.
    """

    quote = ""
    index = at
    while index < len(line):
        char = line[index]
        if quote:
            if char == quote:
                quote = ""
            index += 1
            continue
        if char == "\\" and index + 1 < len(line):
            index += 2
            continue
        if char in "'\"":
            quote = char
            index += 1
            continue
        if line.startswith("&&", index) or line.startswith("||", index):
            return line[at:index]
        if char == "&" and (line[index + 1:index + 2] == ">"
                            or (index and line[index - 1] in "><&")):
            index += 1
            continue
        if char in ";&\n()":
            return line[at:index]
        index += 1
    return line[at:]


def _past_bodies(text: str, newline: int, tags: list[str]) -> int:
    """Where the here-document bodies opened on one line end.

    ``newline`` indexes the newline that ends the opener's line, so the first
    body starts right after it and each body ends at its own terminator line.
    The index returned is the newline that closes the last of them, which is
    where the shell resumes reading the command line.
    """

    at = newline
    for tag in tags:
        while at < len(text):
            end = text.find("\n", at + 1)
            line = text[at + 1:len(text) if end < 0 else end]
            if line.strip() == tag:
                at = len(text) if end < 0 else end
                break
            if end < 0:
                return len(text)
            at = end
    return at


def _backquoted(text: str, start: int) -> tuple[str, int]:
    """The body of the backquoted command at ``start``, and the index past it."""

    index = start + 1
    while index < len(text):
        if text[index] == "\\" and index + 1 < len(text):
            index += 2
            continue
        if text[index] == "`":
            return text[start + 1:index], index + 1
        index += 1
    return text[start + 1:], len(text)


def _commands_in(text: str) -> list[str]:
    """The commands one piece of shell text runs, each as its own segment.

    This is the correction the whole of issue #89 turns on: the boundaries are
    identified BEFORE any exemption is applied, and they are identified with
    the quoting rules the shell uses rather than by splitting on the operator
    characters wherever they appear.  Splitting raw text got both directions
    wrong at once.  A quoted ``&&`` inside a pbrun payload is not a boundary
    and used to be one, which is why the payload had to be cut off the front
    of the command line; an unquoted ``&&`` after a pbrun payload IS a
    boundary and used to be swallowed by that cut, so the command the outer
    shell ran next was never judged.

    A ``$( ... )`` or backquoted substitution is a command the shell runs, so
    its body becomes its own segment.  The text around it keeps accumulating
    with the substitution elided, so the enclosing command still leads its own
    segment and keeps whatever exemption it had: ``git commit -m "$(cat f)"``
    is a ``git`` segment and a ``cat`` segment, and neither is refused.  The
    body is scanned by ``_scan`` rather than by this function alone, because a
    substitution is a whole shell context and may open a here-document of its
    own -- ``git commit -m "$(cat <<'EOF'`` is how a long message is written
    here, and reading its body as commands refused the commit.

    A ``#`` at the start of a word begins a comment, which runs to the end of
    the line and is not a command.  It has to be recognized for the same
    reason quotes do: an apostrophe in a comment otherwise opens a quote that
    runs on and glues the next line's real command into the comment's segment,
    where it inherits the comment's verdict in whichever direction is wrong.
    """

    segments: list[str] = []
    buffer: list[str] = []
    quote = ""
    index = 0

    def flush() -> None:
        piece = "".join(buffer).strip()
        if piece:
            segments.append(piece)
        buffer.clear()

    while index < len(text):
        char = text[index]
        if quote == "'":
            buffer.append(char)
            if char == "'":
                quote = ""
            index += 1
            continue
        if char == "\\" and index + 1 < len(text):
            # A backslash-newline is a line continuation, not a boundary and
            # not two literal characters.  Handling it here rather than by
            # rewriting the whole command first keeps it out of here-document
            # bodies and quoted strings, where the shell does not apply it.
            buffer.append(" " if text[index + 1] == "\n"
                          else text[index:index + 2])
            index += 2
            continue
        if text.startswith("$(", index):
            inner, index = _substitution(text, index)
            segments.extend(_scan(inner))
            buffer.append(" ")
            continue
        if char == "`":
            inner, index = _backquoted(text, index)
            segments.extend(_scan(inner))
            buffer.append(" ")
            continue
        if quote == '"':
            buffer.append(char)
            if char == '"':
                quote = ""
            index += 1
            continue
        if char in "'\"":
            quote = char
            buffer.append(char)
            index += 1
            continue
        if char == "#" and (not buffer or buffer[-1][-1:].isspace()):
            flush()
            end = text.find("\n", index)
            index = len(text) if end < 0 else end
            continue
        if text.startswith("&&", index) or text.startswith("||", index):
            flush()
            index += 2
            continue
        if char == "&":
            # A lone ``&`` backgrounds the command before it and starts
            # another, so it is a boundary.  The redirection spellings that
            # merely contain the character are not: ``2>&1``, ``>&2``, ``&>``
            # and ``<&3`` all belong to the command they sit in.
            if text[index + 1:index + 2] == ">" or buffer and \
                    buffer[-1][-1:] in "><&":
                buffer.append(char)
                index += 1
                continue
            flush()
            index += 1
            continue
        if char in ";|\n()":
            flush()
            index += 1
            continue
        buffer.append(char)
        index += 1
    flush()
    return segments


def _expansions_in(text: str) -> list[str]:
    """The commands an UNQUOTED here-document body runs when it is expanded.

    An unquoted delimiter makes the body behave like a double-quoted string:
    quotes inside it are literal, and ``$( )`` and backquotes still run.  So
    ``cat >/dev/null <<EOF`` carrying ``$(<cuda python> train.py)`` starts GPU
    work even though the command writing the file is ``cat``.
    """

    segments: list[str] = []
    index = 0
    while index < len(text):
        if text[index] == "\\" and index + 1 < len(text):
            index += 2
            continue
        if text.startswith("$(", index):
            inner, index = _substitution(text, index)
            segments.extend(_scan(inner))
            continue
        if text[index] == "`":
            inner, index = _backquoted(text, index)
            segments.extend(_scan(inner))
            continue
        index += 1
    return segments


def _find_heredoc(text: str) -> tuple[int, int, str, bool] | None:
    """The first here-document opener the shell would act on.

    Quote-aware, so a ``<<EOF`` inside a quoted argument is text rather than
    an opener.
    """

    quote = ""
    index = 0
    while index < len(text):
        char = text[index]
        if quote:
            if char == "\\" and quote == '"' and index + 1 < len(text):
                index += 2
                continue
            if char == quote:
                quote = ""
            index += 1
            continue
        if char == "\\" and index + 1 < len(text):
            index += 2
            continue
        if char in "'\"":
            quote = char
            index += 1
            continue
        if text.startswith("<<", index):
            match = HEREDOC.match(text, index)
            if match is not None:
                tag, quoted = _heredoc_tag(match)
                return match.start(), match.end(), tag, quoted
            index += 2
            continue
        index += 1
    return None


def _heredocs(command: str) -> tuple[str, list[str]]:
    """Split the here-document bodies out, and say what each one is.

    Returns the command text with every body removed, and the segments those
    bodies contribute.  Three cases, and the difference between them is what
    the shell does with the body, not what the body looks like:

    * Fed to an interpreter (``bash <<EOF``): the body is a script, so all of
      it is scanned.
    * An unquoted delimiter (``cat >f <<EOF``): the body is data, but the
      shell expands it first, so its substitutions are scanned.
    * A quoted delimiter to anything else (``cat >f <<'EOF'``): the shell
      copies bytes to a file and nothing in the body runs, so it is dropped.
      This is the case the exemption was written for, and it stays: the file
      that starts the fleet's workers names the CUDA interpreter as a
      ``--python`` argument by construction, and the hook refused that write
      four times.

    Only the opener TOKENS leave the command line, not the rest of the line
    they sit on.  Dropping everything from the opener to the newline is the
    same "raw text is data" mistake one token over: ``cat > f <<'EOF' &&
    <cuda python> train.py`` runs that second command after the write, and it
    was never judged.  A line may also carry more than one opener, and the
    shell reads their bodies in order, so they are consumed in order rather
    than leaving the second body standing where a command should be.
    """

    kept: list[str] = []
    extra: list[str] = []
    rest = command
    while True:
        opener = _find_heredoc(rest)
        if opener is None:
            kept.append(rest)
            break
        start, end, tag, quoted = opener
        line_end = rest.find("\n", end)
        if line_end < 0:
            # An opener with no body yet: nothing has been fed in, so there is
            # nothing to strip and the text before it still stands as command.
            kept.append(rest)
            break
        # Every opener on this command line, in the order the shell reads
        # their bodies, and the spans to excise from the line itself.
        openers = [(tag, quoted)]
        spans = [(start, end)]
        cursor = end
        while cursor < line_end:
            more = _find_heredoc(rest[cursor:line_end])
            if more is None:
                break
            openers.append((more[2], more[3]))
            spans.append((cursor + more[0], cursor + more[1]))
            cursor += more[1]
        line = ""
        at = 0
        for span_start, span_end in spans:
            line += rest[at:span_start]
            at = span_end
        after_openers = len(line)
        line += rest[at:line_end]
        kept.append(line)
        # The command the here-document is attached to is the last one before
        # it, not the first one on the line: ``cd x && bash <<EOF`` feeds bash.
        owner_segments = _commands_in(rest[:start])
        owner = owner_segments[-1] if owner_segments else ""
        # A pipeline hands that command's output to the next one as input, so
        # ``cat <<'EOF' | bash`` executes the body just as ``bash <<'EOF'``
        # does, one command further along.  Only the pipeline counts: after a
        # ``&&`` the next command reads its own standard input, and treating
        # ``cat > f <<'EOF' && bash other.sh`` as executable would refuse the
        # file write this exemption exists for.
        downstream = _pipeline_after(line, after_openers)
        executed = _feeds_an_interpreter(owner) or (
            "|" in downstream
            and any(_feeds_an_interpreter(part)
                    for part in _commands_in(downstream)))
        body_text = rest[line_end + 1:]
        for tag, quoted in openers:
            lines = body_text.split("\n")
            body: list[str] = []
            remainder = ""
            for position, entry in enumerate(lines):
                if entry.strip() == tag:
                    remainder = "\n".join(lines[position + 1:])
                    break
                body.append(entry)
            text = "\n".join(body)
            if executed:
                extra.extend(_commands_in(text))
            elif not quoted:
                extra.extend(_expansions_in(text))
            body_text = remainder
        rest = body_text
    # Rejoin with a boundary, not a space.  Gluing the command before a
    # heredoc to the command after it makes one segment whose first token is
    # the writer -- ``cat``, which is exempt -- and the work behind the
    # heredoc inherits that exemption.  Same hole as judging a compound
    # command by its first token, reached from a different direction.
    return "\n".join(part for part in kept if part), extra


def _scan(text: str) -> list[str]:
    """Every command one piece of shell text runs, here-documents included.

    One function rather than two calls at the top level, because a command
    substitution is a shell context in its own right: its body can open a
    here-document, and reading that body without this pass turned a commit
    message into commands.
    """

    kept, extra = _heredocs(text)
    return [*_commands_in(kept), *extra]


def _segments(command: str) -> list[str]:
    """The commands inside one Bash invocation, each judged on its own.

    A leading ``cat`` does not vouch for what follows ``&&``, and a refused
    segment is not excused by a permitted neighbour.
    """

    # The payload cut is per segment, so it can no longer discard the outer
    # shell's own commands.  See ``_drop_pool_payload``.
    return [_drop_pool_payload(segment) for segment in _scan(command)]


def contends(command: str) -> bool:
    """True when any segment of this command starts GPU work off-pool."""

    for segment in _segments(command):
        if not CONTENDS.search(_mentions_elided(segment)):
            continue
        if _inspects_only(segment):
            continue
        if _first_token(segment) in NEVER_GPU:
            continue
        if NO_DEVICE.search(segment):
            continue
        if any(entry in segment for entry in POOL_ENTRYPOINTS):
            continue
        return True
    return False


def submits(command: str) -> bool:
    """True when any segment submits to SLURM outside the fleet's lane.

    The exemptions are the three that cannot be anything else: a segment led
    by a command that never starts work (a commit message, a grep for the
    word, ``which sbatch``), a verb asked only about itself (``sbatch
    --help``), and a segment naming the lane's own entrypoints, which are
    what run ``sbatch`` on this fleet's behalf.  ``command -v sbatch`` joins
    the first of those by shape rather than by the builtin's name, because
    ``command sbatch job.sh`` submits.

    ``CUDA_VISIBLE_DEVICES=`` is NOT an exemption here, deliberately.  It works
    for a local command because the kernel then denies the child a device; it
    says nothing about a job the scheduler will start on another node with a
    GRES allocation of its own.
    """

    for segment in _segments(command):
        if not SCHEDULER.search(_mentions_elided(segment)):
            continue
        if _inspects_only(segment):
            continue
        if _first_token(segment) in NEVER_GPU:
            continue
        if _describes_itself(segment):
            continue
        if any(entry in segment for entry in POOL_ENTRYPOINTS):
            continue
        return True
    return False


# This is a command-line guard, not a proof of arbitrary program behavior.
# The standing agent policy covers indirect test/GPU execution too.
TEST_WORK = re.compile(
    r"(?<![\w.-])(?:pytest|py.test|ctest|tox|nox)(?![\w.-])"
    r"|(?:^|\s)-m\s+unittest\b"
    r"|\b(?:cargo|go)\s+test\b"
    r"|\b(?:npm|pnpm|yarn)\s+(?:run\s+)?test(?:[ :\s]|$)"
)
GPU_CONTAINER = re.compile(r"\b(?:docker|podman)\s+[^;]*--gpus(?:[ =]|$)")


def unpooled_work(command: str) -> bool:
    for segment in _segments(command):
        if _inspects_only(segment) or _first_token(segment) in NEVER_GPU:
            continue
        if any(entry in segment for entry in POOL_ENTRYPOINTS):
            continue
        scanned = _mentions_elided(segment)
        if TEST_WORK.search(scanned):
            # Asking the test runner for help/version does not run tests.
            if re.search(r"(?:^|\s)--(?:help|version)(?:\s|$)", segment):
                continue
            return True
        if GPU_CONTAINER.search(scanned):
            return True
    return False


def main() -> int:
    if not FLAG.exists():
        return 0
    try:
        event = json.load(sys.stdin)
    except Exception:                                        # noqa: BLE001
        return 0
    command = str((event.get("tool_input") or {}).get("command") or "")
    if unpooled_work(command):
        sys.stderr.write(
            "Refused: all tests and GPU work must run through PrismaBuild.\n"
            f"Use /usr/bin/python3 {PBRUN} --cpus N --demand mem_gb=M "
            "[--gpu] -- <command>, or pbtest.py for test shards.\n"
            "Reserve combined child-process resources and read the CAS receipt.\n"
        )
        return 2
    if contends(command):
        sys.stderr.write(
            "Refused: GPU work goes through the PrismaBuild pool, not a local "
            "lock.\n\n"
            f"Run it as:\n  /usr/bin/python3 {PBRUN} --gpu -- <your command>\n\n"
            "Flags: --exclusive for a timing run that needs the whole box "
            "(expressed as a demand for its full GPU capacity, which the ledger "
            "turns into exclusion); Git checkouts are sealed through the CAS and "
            "may be materialized on any matching box; --here pins one on purpose.\n\n"
            "Why: a box-local flock cannot balance across sparky and sparklina, "
            "and it reproduced hold-while-gated, starvation and partial-hold "
            "waste that the pool's ledger solves structurally.\n"
        )
        return 2                      # exit 2 blocks and shows this to the agent
    if submits(command):
        # The same shape, because it is the same rule: an agent who has read
        # one refusal should be able to read this one without stopping.
        sys.stderr.write(
            "Refused: SLURM work goes through the PrismaBuild lane, not a bare "
            "submission.\n\n"
            f"Run it as:\n  /usr/bin/python3 {PBRUN} --transport slurm "
            "--gpu -- <your command>\n\n"
            "Flags: the lane turns --demand and --tag into --gres and "
            "--constraint, --timeout-s into --time, and seals the checkout "
            "through the CAS so the job materializes it on whichever node the "
            "scheduler picks; scancel and squeue are not refused.\n\n"
            "Why: a bare sbatch names no interpreter, so nothing about it can "
            "be priced, placed against the fleet's own accounting, or looked "
            "up afterwards -- the job's CAS receipt is what makes an action "
            "mean the same thing under either transport.\n"
        )
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
