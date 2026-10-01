import re


class LogClassifier:
    """
    signature(out_log, key) reduces a task's log to a string, tasks with equal signatures have the same kind of error.
    Customize by editing the lists (they are regexes), or by overriding methods in a subclass.
    """

    def __init__(self, error_words=None, stack_frames=None, noise_lines=None, masks=None, max_length=160):

        self.error_words = [
            r"error", r"exception", r"fatal", r"panic", r"traceback",
            r"segmentation fault", r"core dumped", r"abort", r"killed", r"\boom\b", r"out of memory",
            r"not enough", r"insufficient", r"denied", r"not found", r"no such",
            r"cannot", r"can't", r"could not", r"couldn't", r"unable to",
            r"fail", r"cancel", r"exceeded", r"timed? ?out", r"terminate called", r"assert",
            r"no space left", r"non-zero exit", r"exit (status|code) [1-9]", r"execution halted",
            r"invalid", r"illegal", r"unexpected", r"missing", r"corrupt",
        ] if error_words is None else list(error_words)

        self.stack_frames = [
            r"^\s*at\s",                                        # Java, JavaScript
            r"^\s*\.\.\. \d+ more",                             # Java
            r"^\s*File \"",                                     # Python
            r"^\s*[\^~]+\s*$",                                  # Python error markers
            r"^\s*Caused by:\s*$",                              # Java
            r"^\s*\d+:\s+\S",                                   # Rust backtrace, R traceback
            r"^goroutine \d+",                                  # Go
            r"^\s+\S+\.go:\d+",                                 # Go
            r"^#\d+\s+0x",                                      # gdb
        ] if stack_frames is None else list(stack_frames)

        # lines never taken as the error line, besides stack frames
        self.noise_lines = [
            r"^\++ ",                                           # bash xtrace
            r"\b0 (errors?|failures?|failed)\b",
            r"\b(errors?|failures?|failed)\s*[:=]\s*0\b",
            r"\bno errors?\b",
        ] if noise_lines is None else list(noise_lines)

        day = r"(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun)[a-z]*"
        month = r"(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*"

        # applied in order, case sensitive
        self.masks = [
            (r"\x1b\[[0-9;?]*[A-Za-z]", ""),                    # ANSI colors and cursor moves
            (r"[\x00-\x08\x0b-\x1f\x7f]", " "),                 # backspaces and other control chars of progress bars
            (rf"\b(?:{day},? )?{month} +\d+,?(?: \d{{4}})? [\d:.]+(?: ?[AP]M)?(?: [A-Z]{{3,4}}\b)?(?: \d{{4}})?", "<date>"),
            (r"\b\d{4}-\d{2}-\d{2}[T ][\d:.,]+(?:Z|[+-]\d{2}:?\d{2})?", "<date>"),
            (r"https?://\S+", "<url>"),
            (r"(?:[A-Za-z]:)?(?:[\w.+\-=@]*[/\\])+[\w.+\-=@]*", "<path>"),
            (r"\b[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\b", "<uuid>"),
            (r"\b0x[0-9a-fA-F]+\b|\b(?=[0-9a-f]*\d)(?=[0-9a-f]*[a-f])[0-9a-f]{6,}\b", "<hex>"),
            (r"'[^']*'|\"[^\"]*\"|`[^`]*`|‘[^’]*’", "<str>"),
            (r"\d+(\.\d+)?", "<n>"),
            (r"(<\w+>[^\s<]{0,3})(\s*\1)+", r"\1…"),            # collapse repeats: "<n>% <n>% <n>%" -> "<n>%…"
            (r"^\s*<n>%…?\s*", ""),                             # progress output glued at the start of a line
            (r"\s+", " "),
        ] if masks is None else list(masks)

        self.max_length = max_length

    def compile(self):
        self._error_regex = re.compile("|".join(self.error_words), re.IGNORECASE)
        self._stack_frame_regex = re.compile("|".join(self.stack_frames))
        self._noise_regex = re.compile("|".join(self.noise_lines), re.IGNORECASE)
        self._mask_regexes = [(re.compile(regex), replacement) for regex, replacement in self.masks]
        return self

    def is_stack_frame(self, line):
        return self._stack_frame_regex.search(line) is not None

    def is_error_line(self, line):
        return (
            self._error_regex.search(line) is not None
            and not self.is_stack_frame(line)
            and self._noise_regex.search(line) is None
        )

    def mask(self, line, key):
        # a placeholder of word chars, so that a key inside a path gets masked along with the path
        line = line.replace(key, "__task_key__")
        for regex, replacement in self._mask_regexes:
            line = regex.sub(replacement, line)
        return line.replace("__task_key__", "<key>").strip()[:self.max_length]

    def is_meaningful(self, masked_line):
        """has at least one word outside of masks, a bare progress line like '<n>%…' is not meaningful"""
        return re.search(r"(?<!<)\b[A-Za-z]{3,}", masked_line) is not None

    def signature(self, out_log, key):

        lines = [l for l in (out_log or "").splitlines() if l.strip()]

        error_line = next((l for l in reversed(lines) if self.is_error_line(l)), None)
        if error_line is not None:
            return self.mask(error_line, key)

        return self.no_error_line_signature(lines, key)

    def no_error_line_signature(self, lines, key):

        def top_stack_frame():
            return next((l for l in lines if self.is_stack_frame(l)), None)

        def last_meaningful_line():
            masked_lines = (self.mask(l, key) for l in reversed(lines))
            return next((m for m in masked_lines if self.is_meaningful(m)), None)

        frame = top_stack_frame()
        if frame is not None:
            return "<no error line> frame: " + self.mask(frame, key)

        last = last_meaningful_line()
        if last is not None:
            return "<no error line> last: " + last

        return "<no error line>"
